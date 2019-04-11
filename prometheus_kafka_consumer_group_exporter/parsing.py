import logging
from struct import unpack_from, error as struct_error


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
    try:
        (version, remaining_key) = read_short(bytes)
        key_dict = {'version': version}

        # These two versions are for offset commit messages.
        if version == 1 or version == 0:
            key_dict['group'], remaining_key = read_string(remaining_key)
            key_dict['topic'], remaining_key = read_string(remaining_key)
            key_dict['partition'], remaining_key = read_int(remaining_key)

        # This version is for group metadata messages.
        # (we don't support parsing their values currently)
        elif version == 2:
            key_dict['group'], remaining_key = read_string(remaining_key)

        else:
            logging.error('Can\'t parse __consumer_offsets topic message key with'
                          ' unsupported version %(version)s.',
                          {'version': version})
            return None

        return key_dict

    except struct_error:
        logging.exception('Failed to parse key from __consumer_offsets topic message.'
                          ' Key: %(key_bytes)s',
                          {'key_bytes': bytes})


def parse_value(bytes):
    try:
        (version, remaining_key) = read_short(bytes)
        value_dict = {'version': version}

        if version == 0:
            value_dict['offset'], remaining_key = read_long_long(remaining_key)
            value_dict['metadata'], remaining_key = read_string(remaining_key)
            value_dict['timestamp'], remaining_key = read_long_long(remaining_key)

        elif version == 1:
            value_dict['offset'], remaining_key = read_long_long(remaining_key)
            value_dict['metadata'], remaining_key = read_string(remaining_key)
            value_dict['commit_timestamp'], remaining_key = read_long_long(remaining_key)
            value_dict['expire_timestamp'], remaining_key = read_long_long(remaining_key)

        elif version == 2:
            value_dict['offset'], remaining_key = read_long_long(remaining_key)
            value_dict['metadata'], remaining_key = read_string(remaining_key)
            value_dict['commit_timestamp'], remaining_key = read_long_long(remaining_key)

        elif version == 3:
            value_dict['offset'], remaining_key = read_long_long(remaining_key)
            value_dict['leader_epoch'], remaining_key = read_int(remaining_key)
            value_dict['metadata'], remaining_key = read_string(remaining_key)
            value_dict['commit_timestamp'], remaining_key = read_long_long(remaining_key)

        else:
            logging.error('Can\'t parse __consumer_offsets topic message value with'
                          ' unsupported version %(version)s.',
                          {'version': version})
            return None

        return value_dict

    except struct_error as e:
        logging.exception('Failed to parse value from __consumer_offsets topic message.'
                          ' Value: %(value_bytes)s',
                          {'value_bytes': bytes})
