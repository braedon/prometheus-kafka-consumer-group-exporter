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
from prometheus_client import start_http_server, Gauge, Counter
from struct import unpack_from

METRIC_PREFIX = 'kafka_consumer_group_'

gauges = {}
counters = {}
topics = {}


def update_gauge(metric_name, label_dict, value, doc=''):
    label_keys = tuple(label_dict.keys())
    label_values = tuple(label_dict.values())

    if metric_name not in gauges:
        gauges[metric_name] = Gauge(metric_name, doc, label_keys)

    gauge = gauges[metric_name]

    if label_values:
        gauge.labels(*label_values).set(value)
    else:
        gauge.set(value)


def increment_counter(metric_name, label_dict, doc=''):
    label_keys = tuple(label_dict.keys())
    label_values = tuple(label_dict.values())

    if metric_name not in counters:
        counters[metric_name] = Counter(metric_name, doc, label_keys)

    counter = counters[metric_name]

    if label_values:
        counter.labels(*label_values).inc()
    else:
        counter.inc()


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

        global topics

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

        topics = new_topics

    def update_highwater(offsets):
        logging.info('Received high-water marks')

        for topic, partitions in offsets.topics:
            for partition, error_code, offsets in partitions:
                if error_code:
                    error = Errors.for_code(error_code)((partition, error_code, offsets))
                    logging.warning('Received error in offset response for topic %(topic)s: %(error)s',
                                    {'topic': topic, 'error': error})
                else:
                    logging.debug('Received high-water marks for partition %(partition)s of topic %(topic)s',
                                  {'partition': partition, 'topic': topic})

                    update_gauge(
                        metric_name='kafka_topic_highwater',
                        label_dict={
                            'topic': topic,
                            'partition': partition
                        },
                        value=offsets[0],
                        doc='The offset of the head of a partition in a topic.'
                    )

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
            global topics
            if topics:
                nodes = {}
                for topic, partition_map in topics.items():
                    for partition, leader in partition_map.items():
                        if leader not in nodes:
                            nodes[leader] = {}
                        if topic not in nodes[leader]:
                            nodes[leader][topic] = []
                        nodes[leader][topic].append(partition)

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
                    f.add_callback(update_highwater)
        except Exception:
            logging.exception('Error requesting high-water marks')
        finally:
            client.schedule(partial(fetch_highwater, next_time), next_time)

    now_time = time.time()

    fetch_topics(now_time)
    fetch_highwater(now_time)

    try:
        while True:
            for message in consumer:
                update_gauge(
                    metric_name=METRIC_PREFIX + 'exporter_offset',
                    label_dict={
                        'partition': message.partition
                    },
                    value=message.offset,
                    doc='The current offset of the exporter consumer in a partition of the __consumer_offsets topic.'
                )

                if message.key and message.value:
                    key = parse_key(message.key)
                    if key:
                        value = parse_value(message.value)

                        update_gauge(
                            metric_name=METRIC_PREFIX + 'offset',
                            label_dict={
                                'group': key[1],
                                'topic': key[2],
                                'partition': key[3]
                            },
                            value=value[1],
                            doc='The current offset of a consumer group in a partition of a topic.'
                        )

                        increment_counter(
                            metric_name=METRIC_PREFIX + 'commits',
                            label_dict={
                                'group': key[1],
                                'topic': key[2],
                                'partition': key[3]
                            },
                            doc='The number of commit messages read by the exporter consumer from a consumer group for a partition of a topic.'
                        )

    except KeyboardInterrupt:
        pass

    shutdown()
