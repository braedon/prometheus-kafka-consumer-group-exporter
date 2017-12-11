import argparse
import javaproperties
import logging
import signal
import sys
import time

import kafka.errors as Errors

from functools import partial
from jog import JogFormatter
from kafka import KafkaConsumer
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, REGISTRY
from struct import unpack_from

METRIC_PREFIX = 'kafka_consumer_group_'

topics = {}  # topic->partition->leader
node_highwaters = {}  # node->topic->partition->highwater
node_lowwaters = {}  # node->topic->partition->lowwater
offsets = {}  # group->topic->partition->offset
commits = {}  # group->topic->partition->commits
exporter_offsets = {}  # partition->offset


def build_highwaters():
    # Copy node_highwaters before iterating over it
    # as it may be updated by other threads.
    # (only first level - lower levels are replaced
    # wholesale, so don't worry about them)
    local_node_highwaters = node_highwaters.copy()

    highwaters = {}
    for node, topics in local_node_highwaters.items():
        for topic, partitions in topics.items():
            highwaters[topic] = {**highwaters.get(topic, {}), **partitions}

    return highwaters


def build_lowwaters():
    # Copy node_lowwaters before iterating over it
    # as it may be updated by other threads.
    # (only first level - lower levels are replaced
    # wholesale, so don't worry about them)
    local_node_lowwaters = node_lowwaters.copy()

    lowwaters = {}
    for node, topics in local_node_lowwaters.items():
        for topic, partitions in topics.items():
            lowwaters[topic] = {**lowwaters.get(topic, {}), **partitions}

    return lowwaters


# Check if a dict contains a key, returning
# a copy with the key if not.
# Effectively a way to immutably add a key
# to a dictionary, allowing other threads
# to safely iterate over it.
def ensure_dict_key(curr_dict, key, new_value):
    if key in curr_dict:
        return curr_dict

    new_dict = curr_dict.copy()
    new_dict[key] = new_value
    return new_dict


def group_metrics(metrics):
    metric_dict = {}
    for (metric_name, metric_doc, label_keys, label_values, value) in metrics:
        if metric_name not in metric_dict:
            metric_dict[metric_name] = (metric_doc, label_keys, {})

        metric_dict[metric_name][2][label_values] = value

    return metric_dict


def gauge_generator(metrics):
    metric_dict = group_metrics(metrics)

    for metric_name, (metric_doc, label_keys, value_dict) in metric_dict.items():
        # If we have label keys we may have multiple different values,
        # each with their own label values.
        if label_keys:
            gauge = GaugeMetricFamily(metric_name, metric_doc, labels=label_keys)

            for label_values in sorted(value_dict.keys()):
                value = value_dict[label_values]
                gauge.add_metric(tuple(str(v) for v in label_values), value)

        # No label keys, so we must have only a single value.
        else:
            gauge = GaugeMetricFamily(metric_name, metric_doc, value=list(value_dict.values())[0])

        yield gauge


def counter_generator(metrics):
    metric_dict = group_metrics(metrics)

    for metric_name, (metric_doc, label_keys, value_dict) in metric_dict.items():
        # If we have label keys we may have multiple different values,
        # each with their own label values.
        if label_keys:
            counter = CounterMetricFamily(metric_name, metric_doc, labels=label_keys)

            for label_values in sorted(value_dict.keys()):
                value = value_dict[label_values]
                counter.add_metric(tuple(str(v) for v in label_values), value)

        # No label keys, so we must have only a single value.
        else:
            counter = CounterMetricFamily(metric_name, metric_doc, value=list(value_dict.values())[0])

        yield counter


class HighwaterCollector(object):

    def collect(self):
        highwaters = build_highwaters()
        metrics = [
            ('kafka_topic_highwater', 'The offset of the head of a partition in a topic.',
             ('topic', 'partition'), (topic, partition),
             highwater)
            for topic, partitions in highwaters.items()
            for partition, highwater in partitions.items()
        ]
        yield from gauge_generator(metrics)


class LowwaterCollector(object):

    def collect(self):
        lowwaters = build_lowwaters()
        metrics = [
            ('kafka_topic_lowwater', 'The offset of the tail of a partition in a topic.',
             ('topic', 'partition'), (topic, partition),
             lowwater)
            for topic, partitions in lowwaters.items()
            for partition, lowwater in partitions.items()
        ]
        yield from gauge_generator(metrics)


class ConsumerOffsetCollector(object):

    def collect(self):
        metrics = [
            (METRIC_PREFIX + 'offset', 'The current offset of a consumer group in a partition of a topic.',
             ('group', 'topic', 'partition'), (group, topic, partition),
             offset)
            for group, topics in offsets.items()
            for topic, partitions in topics.items()
            for partition, offset in partitions.items()
        ]
        yield from gauge_generator(metrics)


class ConsumerLagCollector(object):

    def collect(self):
        highwaters = build_highwaters()
        metrics = [
            (METRIC_PREFIX + 'lag', 'How far a consumer group\'s current offset is behind the head of a partition of a topic.',
             ('group', 'topic', 'partition'), (group, topic, partition),
             highwaters[topic][partition] - offset)
            for group, topics in offsets.items()
            for topic, partitions in topics.items()
            for partition, offset in partitions.items()
            if topic in highwaters and partition in highwaters[topic]
        ]
        yield from gauge_generator(metrics)


class ConsumerLeadCollector(object):

    def collect(self):
        lowwaters = build_lowwaters()
        metrics = [
            (METRIC_PREFIX + 'lead', 'How far a consumer group\'s current offset is ahead of the tail of a partition of a topic.',
             ('group', 'topic', 'partition'), (group, topic, partition),
             offset - lowwaters[topic][partition])
            for group, topics in offsets.items()
            for topic, partitions in topics.items()
            for partition, offset in partitions.items()
            if topic in lowwaters and partition in lowwaters[topic]
        ]
        yield from gauge_generator(metrics)


class ConsumerCommitsCollector(object):

    def collect(self):
        metrics = [
            (METRIC_PREFIX + 'commits', 'The number of commit messages read by the exporter consumer from a consumer group for a partition of a topic.',
             ('group', 'topic', 'partition'), (group, topic, partition),
             commit_count)
            for group, topics in commits.items()
            for topic, partitions in topics.items()
            for partition, commit_count in partitions.items()
        ]
        yield from counter_generator(metrics)


class ExporterOffsetCollector(object):

    def collect(self):
        metrics = [
            (METRIC_PREFIX + 'exporter_offset', 'The current offset of the exporter consumer in a partition of the __consumer_offsets topic.',
             ('partition',), (partition,),
             offset)
            for partition, offset in exporter_offsets.items()
        ]
        yield from gauge_generator(metrics)


class ExporterLagCollector(object):

    def collect(self):
        topic = '__consumer_offsets'
        highwaters = build_highwaters()
        metrics = [
            (METRIC_PREFIX + 'exporter_lag', 'How far the exporter consumer is behind the head of a partition of the __consumer_offsets topic.',
             ('partition',), (partition,),
             highwaters[topic][partition] - offset)
            for partition, offset in exporter_offsets.items()
            if topic in highwaters and partition in highwaters[topic]
        ]
        yield from gauge_generator(metrics)


class ExporterLeadCollector(object):

    def collect(self):
        topic = '__consumer_offsets'
        lowwaters = build_lowwaters()
        metrics = [
            (METRIC_PREFIX + 'exporter_lead', 'How far the exporter consumer is ahead of the tail of a partition of the __consumer_offsets topic.',
             ('partition',), (partition,),
             offset - lowwaters[topic][partition])
            for partition, offset in exporter_offsets.items()
            if topic in lowwaters and partition in lowwaters[topic]
        ]
        yield from gauge_generator(metrics)


def shutdown():
    logging.info('Shutting down')
    sys.exit(1)


def signal_handler(signum, frame):
    shutdown()


def main():
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(
        description='Export Kafka consumer offsets to Prometheus.')
    parser.add_argument(
        '-b', '--bootstrap-brokers', default='localhost',
        help='Addresses of brokers in a Kafka cluster to talk to.' +
        ' Brokers should be separated by commas e.g. broker1,broker2.' +
        ' Ports can be provided if non-standard (9092) e.g. brokers1:9999.' +
        ' (default: localhost)')
    parser.add_argument(
        '-p', '--port', type=int, default=9208,
        help='Port to serve the metrics endpoint on. (default: 9208)')
    parser.add_argument(
        '-s', '--from-start', action='store_true',
        help='Start from the beginning of the `__consumer_offsets` topic.')
    parser.add_argument(
        '--topic-interval', type=float, default=30.0,
        help='How often to refresh topic information, in seconds. (default: 30)')
    parser.add_argument(
        '--high-water-interval', type=float, default=10.0,
        help='How often to refresh high-water information, in seconds. (default: 10)')
    parser.add_argument(
        '--low-water-interval', type=float, default=10.0,
        help='How often to refresh low-water information, in seconds. (default: 10)')
    parser.add_argument(
        '--consumer-config', action='append', default=[],
        help='Provide additional Kafka consumer config as a consumer.properties file. Multiple files will be merged, later files having precedence.')
    parser.add_argument(
        '-j', '--json-logging', action='store_true',
        help='Turn on json logging.')
    parser.add_argument(
        '--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='detail level to log. (default: INFO)')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='turn on verbose (DEBUG) logging. Overrides --log-level.')
    args = parser.parse_args()

    log_handler = logging.StreamHandler()
    log_format = '[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s'
    formatter = JogFormatter(log_format) \
        if args.json_logging \
        else logging.Formatter(log_format)
    log_handler.setFormatter(formatter)

    log_level = getattr(logging, args.log_level)
    logging.basicConfig(
        handlers=[log_handler],
        level=logging.DEBUG if args.verbose else log_level
    )
    logging.captureWarnings(True)

    port = args.port

    consumer_config = {
        'bootstrap_servers': 'localhost',
        'auto_offset_reset': 'latest',
        'group_id': None,
        'consumer_timeout_ms': 500
    }

    for filename in args.consumer_config:
        with open(filename) as f:
            raw_config = javaproperties.load(f)
            converted_config = {k.replace('.', '_'): v for k, v in raw_config.items()}
            consumer_config.update(converted_config)

    if args.bootstrap_brokers:
        consumer_config['bootstrap_servers'] = args.bootstrap_brokers.split(',')

    if args.from_start:
        consumer_config['auto_offset_reset'] = 'earliest'

    consumer = KafkaConsumer(
        '__consumer_offsets',
        **consumer_config
    )
    client = consumer._client

    topic_interval = args.topic_interval
    high_water_interval = args.high_water_interval
    low_water_interval = args.low_water_interval

    logging.info('Starting server...')
    start_http_server(port)
    logging.info('Server started on port %s', port)

    def read_short(bytes):
        num = unpack_from('>h', bytes)[0]
        remaining = bytes[2:]
        return (num, remaining)

    def read_int(bytes):
        num = unpack_from('>i', bytes)[0]
        remaining = bytes[4:]
        return (num, remaining)

    def read_long_long(bytes):
        num = unpack_from('>q', bytes)[0]
        remaining = bytes[8:]
        return (num, remaining)

    def read_string(bytes):
        length, remaining = read_short(bytes)
        string = remaining[:length].decode('utf-8')
        remaining = remaining[length:]
        return (string, remaining)

    def parse_key(bytes):
        (version, remaining_key) = read_short(bytes)
        if version == 1 or version == 0:
            (group, remaining_key) = read_string(remaining_key)
            (topic, remaining_key) = read_string(remaining_key)
            (partition, remaining_key) = read_int(remaining_key)
            return (version, group, topic, partition)

    def parse_value(bytes):
        (version, remaining_key) = read_short(bytes)
        if version == 0:
            (offset, remaining_key) = read_long_long(remaining_key)
            (metadata, remaining_key) = read_string(remaining_key)
            (timestamp, remaining_key) = read_long_long(remaining_key)
            return (version, offset, metadata, timestamp)
        elif version == 1:
            (offset, remaining_key) = read_long_long(remaining_key)
            (metadata, remaining_key) = read_string(remaining_key)
            (commit_timestamp, remaining_key) = read_long_long(remaining_key)
            (expire_timestamp, remaining_key) = read_long_long(remaining_key)
            return (version, offset, metadata, commit_timestamp, expire_timestamp)

    def update_topics(api_version, metadata):
        logging.info('Received topics and partition assignments')

        if api_version == 0:
            TOPIC_ERROR = 0
            TOPIC_NAME = 1
            TOPIC_PARTITIONS = 2
            PARTITION_ERROR = 0
            PARTITION_NUMBER = 1
            PARTITION_LEADER = 2
        else:
            TOPIC_ERROR = 0
            TOPIC_NAME = 1
            TOPIC_PARTITIONS = 3
            PARTITION_ERROR = 0
            PARTITION_NUMBER = 1
            PARTITION_LEADER = 2

        new_topics = {}
        for t in metadata.topics:
            error_code = t[TOPIC_ERROR]
            if error_code:
                error = Errors.for_code(error_code)(t)
                logging.warning('Received error in metadata response at topic level: %s', error)
            else:
                topic = t[TOPIC_NAME]
                partitions = t[TOPIC_PARTITIONS]

                new_partitions = {}
                for p in partitions:
                    error_code = p[PARTITION_ERROR]
                    if error_code:
                        error = Errors.for_code(error_code)(p)
                        logging.warning('Received error in metadata response at partition level for topic %(topic)s: %(error)s',
                                        {'topic': topic, 'error': error})
                    else:
                        partition = p[PARTITION_NUMBER]
                        leader = p[PARTITION_LEADER]
                        logging.debug('Received partition assignment for partition %(partition)s of topic %(topic)s',
                                      {'partition': partition, 'topic': topic})

                        new_partitions[partition] = leader

                new_topics[topic] = new_partitions

        global topics
        topics = new_topics

    def update_highwater(node, offsets):
        logging.info('Received high-water marks')

        highwaters = {}
        for topic, partitions in offsets.topics:
            for partition, error_code, offsets in partitions:
                if error_code:
                    error = Errors.for_code(error_code)((partition, error_code, offsets))
                    logging.warning('Received error in offset response for topic %(topic)s: %(error)s',
                                    {'topic': topic, 'error': error})
                else:
                    logging.debug('Received high-water marks for partition %(partition)s of topic %(topic)s',
                                  {'partition': partition, 'topic': topic})

                    highwater = offsets[0]

                    if topic not in highwaters:
                        highwaters[topic] = {}
                    highwaters[topic][partition] = highwater

        global node_highwaters
        node_highwaters[node] = highwaters

    def update_lowwater(node, offsets):
        logging.info('Received low-water marks')

        lowwaters = {}
        for topic, partitions in offsets.topics:
            for partition, error_code, offsets in partitions:
                if error_code:
                    error = Errors.for_code(error_code)((partition, error_code, offsets))
                    logging.warning('Received error in offset response for topic %(topic)s: %(error)s',
                                    {'topic': topic, 'error': error})
                else:
                    logging.debug('Received low-water marks for partition %(partition)s of topic %(topic)s',
                                  {'partition': partition, 'topic': topic})

                    lowwater = offsets[0]

                    if topic not in lowwaters:
                        lowwaters[topic] = {}
                    lowwaters[topic][partition] = lowwater

        global node_lowwaters
        node_lowwaters[node] = lowwaters

    def fetch_topics(this_time):
        logging.info('Requesting topics and partition assignments')

        next_time = this_time + topic_interval
        try:
            node = client.least_loaded_node()

            logging.debug('Requesting topics and partition assignments from %(node)s',
                          {'node': node})

            api_version = 0 if client.config['api_version'] < (0, 10) else 1
            request = MetadataRequest[api_version](None)
            f = client.send(node, request)
            f.add_callback(update_topics, api_version)
        except Exception:
            logging.exception('Error requesting topics and partition assignments')
        finally:
            client.schedule(partial(fetch_topics, next_time), next_time)

    def fetch_highwater(this_time):
        logging.info('Requesting high-water marks')
        next_time = this_time + high_water_interval
        try:
            if topics:
                nodes = {}
                for topic, partition_map in topics.items():
                    for partition, leader in partition_map.items():
                        if leader not in nodes:
                            nodes[leader] = {}
                        if topic not in nodes[leader]:
                            nodes[leader][topic] = []
                        nodes[leader][topic].append(partition)

                global node_highwaters
                # Build a new highwaters dict with only the nodes that
                # are leaders of at least one topic - i.e. the ones
                # we will be sending requests to.
                # Removes old nodes, and adds empty dicts for new nodes.
                # Values will be populated/updated with values we get
                # in the response from each node.
                # Topics/Partitions on old nodes may disappear briefly
                # before they reappear on their new nodes.
                new_node_highwaters = {}
                for node in nodes.keys():
                    new_node_highwaters[node] = node_highwaters.get(node, {})

                node_highwaters = new_node_highwaters

                for node, topic_map in nodes.items():
                    logging.debug('Requesting high-water marks from %(node)s',
                                  {'topic': topic, 'node': node})

                    request = OffsetRequest[0](
                        -1,
                        [(topic,
                          [(partition, OffsetResetStrategy.LATEST, 1)
                           for partition in partitions])
                         for topic, partitions in topic_map.items()]
                    )
                    f = client.send(node, request)
                    f.add_callback(update_highwater, node)
        except Exception:
            logging.exception('Error requesting high-water marks')
        finally:
            client.schedule(partial(fetch_highwater, next_time), next_time)

    def fetch_lowwater(this_time):
        logging.info('Requesting low-water marks')
        next_time = this_time + low_water_interval
        try:
            if topics:
                nodes = {}
                for topic, partition_map in topics.items():
                    for partition, leader in partition_map.items():
                        if leader not in nodes:
                            nodes[leader] = {}
                        if topic not in nodes[leader]:
                            nodes[leader][topic] = []
                        nodes[leader][topic].append(partition)

                global node_lowwaters
                # Build a new node_lowwaters dict with only the nodes that
                # are leaders of at least one topic - i.e. the ones
                # we will be sending requests to.
                # Removes old nodes, and adds empty dicts for new nodes.
                # Values will be populated/updated with values we get
                # in the response from each node.
                # Topics/Partitions on old nodes may disappear briefly
                # before they reappear on their new nodes.
                new_node_lowwaters = {}
                for node in nodes.keys():
                    new_node_lowwaters[node] = node_lowwaters.get(node, {})

                node_lowwaters = new_node_lowwaters

                for node, topic_map in nodes.items():
                    logging.debug('Requesting low-water marks from %(node)s',
                                  {'topic': topic, 'node': node})

                    request = OffsetRequest[0](
                        -1,
                        [(topic,
                          [(partition, OffsetResetStrategy.EARLIEST, 1)
                           for partition in partitions])
                         for topic, partitions in topic_map.items()]
                    )
                    f = client.send(node, request)
                    f.add_callback(update_lowwater, node)
        except Exception:
            logging.exception('Error requesting low-water marks')
        finally:
            client.schedule(partial(fetch_lowwater, next_time), next_time)

    REGISTRY.register(HighwaterCollector())
    REGISTRY.register(LowwaterCollector())
    REGISTRY.register(ConsumerOffsetCollector())
    REGISTRY.register(ConsumerLagCollector())
    REGISTRY.register(ConsumerLeadCollector())
    REGISTRY.register(ConsumerCommitsCollector())
    REGISTRY.register(ExporterOffsetCollector())
    REGISTRY.register(ExporterLagCollector())
    REGISTRY.register(ExporterLeadCollector())

    now_time = time.time()

    fetch_topics(now_time)
    fetch_highwater(now_time)
    fetch_lowwater(now_time)

    global offsets
    global commits
    global exporter_offsets

    try:
        while True:
            for message in consumer:
                exporter_partition = message.partition
                exporter_offset = message.offset
                exporter_offsets = ensure_dict_key(exporter_offsets, exporter_partition, exporter_offset)
                exporter_offsets[exporter_partition] = exporter_offset

                if message.key and message.value:
                    key = parse_key(message.key)
                    if key:
                        value = parse_value(message.value)

                        group = key[1]
                        topic = key[2]
                        partition = key[3]
                        offset = value[1]

                        offsets = ensure_dict_key(offsets, group, {})
                        offsets[group] = ensure_dict_key(offsets[group], topic, {})
                        offsets[group][topic] = ensure_dict_key(offsets[group][topic], partition, offset)
                        offsets[group][topic][partition] = offset

                        commits = ensure_dict_key(commits, group, {})
                        commits[group] = ensure_dict_key(commits[group], topic, {})
                        commits[group][topic] = ensure_dict_key(commits[group][topic], partition, 0)
                        commits[group][topic][partition] += 1

    except KeyboardInterrupt:
        pass

    shutdown()
