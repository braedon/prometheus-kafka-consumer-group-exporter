import argparse
import logging
import signal
import sys

from kafka import KafkaConsumer
from logstash_formatter import LogstashFormatterV1
from prometheus_client import start_http_server, Gauge, Counter
from struct import unpack_from

METRIC_PREFIX = 'kafka_consumer_group_'

gauges = {}
counters = {}


def update_gauge(metric_name, label_dict, value):
    label_keys = tuple(label_dict.keys())
    label_values = tuple(label_dict.values())

    if metric_name not in gauges:
        gauges[metric_name] = Gauge(metric_name, '', label_keys)

    gauge = gauges[metric_name]

    if label_values:
        gauge.labels(*label_values).set(value)
    else:
        gauge.set(value)


def increment_counter(metric_name, label_dict):
    label_keys = tuple(label_dict.keys())
    label_values = tuple(label_dict.values())

    if metric_name not in counters:
        counters[metric_name] = Counter(metric_name, '', label_keys)

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
        '-p', '--port', type=int, default=8080,
        help='Port to serve the metrics endpoint on. (default: 8080)')
    parser.add_argument(
        '-s', '--from-start', action='store_true',
        help='Start from the beginning of the `__consumer_offsets` topic.')
    parser.add_argument(
        '-j', '--json-logging', action='store_true',
        help='Turn on json logging.')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='Turn on verbose logging.')
    args = parser.parse_args()

    log_handler = logging.StreamHandler()
    log_format = '[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s'
    formatter = LogstashFormatterV1() \
        if args.json_logging \
        else logging.Formatter(log_format)
    log_handler.setFormatter(formatter)

    logging.basicConfig(
        handlers=[log_handler],
        level=logging.DEBUG if args.verbose else logging.INFO
    )
    logging.captureWarnings(True)

    port = args.port
    bootstrap_brokers = args.bootstrap_brokers.split(',')

    consumer = KafkaConsumer(
        '__consumer_offsets',
        bootstrap_servers=bootstrap_brokers,
        auto_offset_reset='earliest' if args.from_start else 'latest',
        group_id=None
    )

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

    try:
        for message in consumer:
            update_gauge(
                metric_name=METRIC_PREFIX + 'exporter_offset',
                label_dict={
                    'partition': message.partition
                },
                value=message.offset
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
                        value=value[1]
                    )

                    increment_counter(
                        metric_name=METRIC_PREFIX + 'commits',
                        label_dict={
                            'group': key[1],
                            'topic': key[2],
                            'partition': key[3]
                        }
                    )

    except KeyboardInterrupt:
        pass

    shutdown()
