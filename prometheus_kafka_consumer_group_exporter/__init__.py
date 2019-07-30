import argparse
import javaproperties
import logging
import signal
import sys

from jog import JogFormatter
from kafka import KafkaConsumer
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY

from . import scheduler, collectors
from .fetch_jobs import setup_fetch_jobs
from .parsing import parse_key, parse_value


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
        '-b', '--bootstrap-brokers',
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
        consumer_config['bootstrap_servers'] = args.bootstrap_brokers

    consumer_config['bootstrap_servers'] = consumer_config['bootstrap_servers'].split(',')

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

    REGISTRY.register(collectors.HighwaterCollector())
    REGISTRY.register(collectors.LowwaterCollector())
    REGISTRY.register(collectors.ConsumerOffsetCollector())
    REGISTRY.register(collectors.ConsumerLagCollector())
    REGISTRY.register(collectors.ConsumerLeadCollector())
    REGISTRY.register(collectors.ConsumerCommitsCollector())
    REGISTRY.register(collectors.ConsumerCommitTimestampCollector())
    REGISTRY.register(collectors.ExporterOffsetCollector())
    REGISTRY.register(collectors.ExporterLagCollector())
    REGISTRY.register(collectors.ExporterLeadCollector())

    scheduled_jobs = setup_fetch_jobs(topic_interval, high_water_interval, low_water_interval, client)
    scheduler.run_scheduled_jobs(scheduled_jobs)

    try:
        while True:
            for message in consumer:
                offsets = collectors.get_offsets()
                commits = collectors.get_commits()
                commit_timestamps = collectors.get_commit_timestamps()
                exporter_offsets = collectors.get_exporter_offsets()

                exporter_partition = message.partition
                exporter_offset = message.offset
                exporter_offsets = ensure_dict_key(exporter_offsets, exporter_partition, exporter_offset)
                exporter_offsets[exporter_partition] = exporter_offset
                collectors.set_exporter_offsets(exporter_offsets)

                if message.key:
                    key_dict = parse_key(message.key)
                    # Only key versions 0 and 1 are offset commit messages.
                    # Ignore other versions.
                    if key_dict is not None and key_dict['version'] in (0, 1):

                        if message.value:
                            value_dict = parse_value(message.value)
                            if value_dict is not None:
                                group = key_dict['group']
                                topic = key_dict['topic']
                                partition = key_dict['partition']
                                offset = value_dict['offset']
                                commit_timestamp = value_dict['commit_timestamp'] / 1000

                                offsets = ensure_dict_key(offsets, group, {})
                                offsets[group] = ensure_dict_key(offsets[group], topic, {})
                                offsets[group][topic] = ensure_dict_key(offsets[group][topic], partition, offset)
                                offsets[group][topic][partition] = offset
                                collectors.set_offsets(offsets)

                                commits = ensure_dict_key(commits, group, {})
                                commits[group] = ensure_dict_key(commits[group], topic, {})
                                commits[group][topic] = ensure_dict_key(commits[group][topic], partition, 0)
                                commits[group][topic][partition] += 1
                                collectors.set_commits(commits)

                                commit_timestamps = ensure_dict_key(commit_timestamps, group, {})
                                commit_timestamps[group] = ensure_dict_key(commit_timestamps[group], topic, {})
                                commit_timestamps[group][topic] = ensure_dict_key(commit_timestamps[group][topic], partition, 0)
                                commit_timestamps[group][topic][partition] = commit_timestamp
                                collectors.set_commit_timestamps(commit_timestamps)

                        else:
                            # The group has been removed, so we should not report metrics
                            group = key_dict['group']
                            topic = key_dict['topic']
                            partition = key_dict['partition']

                            if group in offsets:
                                if topic in offsets[group]:
                                    if partition in offsets[group][topic]:
                                        del offsets[group][topic][partition]

                            if group in commits:
                                if topic in commits[group]:
                                    if partition in commits[group][topic]:
                                        del commits[group][topic][partition]

                            if group in commit_timestamps:
                                if topic in commit_timestamps[group]:
                                    if partition in commit_timestamps[group][topic]:
                                        del commit_timestamps[group][topic][partition]

                elif message.key and not message.value:
                    # The group has been removed, so we should not report metrics
                    key_dict = parse_key(message.key)
                    if key_dict is not None and key_dict['version'] in (0, 1):
                        group = key_dict['group']
                        topic = key_dict['topic']
                        partition = key_dict['partition']

                        if group in offsets:
                            if topic in offsets[group]:
                                if partition in offsets[group][topic]:
                                    del offsets[group][topic][partition]

                        if group in commits:
                            if topic in commits[group]:
                                if partition in commits[group][topic]:
                                    del commits[group][topic][partition]
                        
                        if group in commit_timestamps:
                            if topic in commit_timestamps[group]:
                                if partition in commit_timestamps[group][topic]:
                                    del commit_timestamps[group][topic][partition]

                # Check if we need to run any scheduled jobs
                # each message.
                scheduled_jobs = scheduler.run_scheduled_jobs(scheduled_jobs)

            # Also check if we need to run any scheduled jobs
            # each time the consumer times out, in case there
            # aren't any messages to consume.
            scheduled_jobs = scheduler.run_scheduled_jobs(scheduled_jobs)

    except KeyboardInterrupt:
        pass

    shutdown()
