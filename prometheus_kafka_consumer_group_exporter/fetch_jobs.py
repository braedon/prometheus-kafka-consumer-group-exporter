import logging

import kafka.errors as Errors

from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy

from . import scheduler

# Globals
topics = {}  # topic->partition->leader
node_highwaters = {}  # node->topic->partition->highwater
node_lowwaters = {}  # node->topic->partition->lowwater


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


def fetch_topics(client, callback):
    logging.info('Requesting topics and partition assignments')

    try:
        node = client.least_loaded_node()

        logging.debug('Requesting topics and partition assignments from %(node)s',
                      {'node': node})

        api_version = 0 if client.config['api_version'] < (0, 10) else 1
        request = MetadataRequest[api_version](None)
        f = client.send(node, request)
        f.add_callback(callback, api_version)

    except Exception:
        logging.exception('Error requesting topics and partition assignments')


def fetch_highwater(client, callback):
    try:
        if topics:
            logging.info('Requesting high-water marks')

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
                f.add_callback(callback, node)

    except Exception:
        logging.exception('Error requesting high-water marks')


def fetch_lowwater(client, callback):
    try:
        if topics:
            logging.info('Requesting low-water marks')

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
                f.add_callback(callback, node)

    except Exception:
        logging.exception('Error requesting low-water marks')


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
    logging.info('Received high-water marks from node {}'.format(node))

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
    logging.info('Received low-water marks from node {}'.format(node))

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


def setup_fetch_jobs(topic_interval, high_water_interval, low_water_interval,
                     client, jobs=None):

    if jobs is None:
        jobs = []

    jobs = scheduler.add_scheduled_job(jobs, topic_interval,
                                       fetch_topics, client, update_topics)
    jobs = scheduler.add_scheduled_job(jobs, high_water_interval,
                                       fetch_highwater, client, update_highwater)
    jobs = scheduler.add_scheduled_job(jobs, low_water_interval,
                                       fetch_lowwater, client, update_lowwater)
    return jobs
