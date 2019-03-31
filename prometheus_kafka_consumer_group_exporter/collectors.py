from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily

from .fetch_jobs import build_highwaters, build_lowwaters

METRIC_PREFIX = 'kafka_consumer_group_'

# Globals
offsets = {}  # group->topic->partition->offset
commits = {}  # group->topic->partition->commits
commit_timestamps = {}  # group->topic->partition->commit_timestamp
exporter_offsets = {}  # partition->offset


def get_offsets():
    return offsets


def set_offsets(new_offsets):
    global offsets
    offsets = new_offsets


def get_commits():
    return commits


def set_commits(new_commits):
    global commits
    commits = new_commits


def get_commit_timestamps():
    return commit_timestamps


def set_commit_timestamps(new_commit_timestamps):
    global commit_timestamps
    commit_timestamps = new_commit_timestamps


def get_exporter_offsets():
    return exporter_offsets


def set_exporter_offsets(new_exporter_offsets):
    global exporter_offsets
    exporter_offsets = new_exporter_offsets


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
             max(highwaters[topic][partition] - offset, 0))
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


class ConsumerCommitTimestampCollector(object):

    def collect(self):
        metrics = [
            (METRIC_PREFIX + 'commit_timestamp', 'The timestamp of the latest commit from a consumer group for a partition of a topic.',
             ('group', 'topic', 'partition'), (group, topic, partition),
             commit_timestamp)
            for group, topics in commit_timestamps.items()
            for topic, partitions in topics.items()
            for partition, commit_timestamp in partitions.items()
        ]
        yield from gauge_generator(metrics)


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
             max(highwaters[topic][partition] - offset, 0))
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
