import collections
import datetime
import functools
import inspect
import itertools
import json
import math
from operator import itemgetter
import random
import re
import sys
from typing import List, Optional, Union

import pytz
from pytz import UnknownTimeZoneError

from ._config import config
from .sql.casts import get_time_formatter
from .sql.internal_utils.joins import (
    CROSS_JOIN, FULL_JOIN, INNER_JOIN, LEFT_ANTI_JOIN, LEFT_JOIN, LEFT_SEMI_JOIN, RIGHT_JOIN
)
from .sql.schema_utils import get_on_fields
from .sql.types import create_row, Row, row_from_keyed_values
from .sql.utils import IllegalArgumentException


class Tokenizer:
    def __init__(self, expression: str):
        self.expression = expression

    def get_next(self, separator: Optional[Union[List[str], str]] = None) -> str:
        if isinstance(separator, list):
            separator_positions_and_lengths = [
                (self.expression.find(s), s)
                for s in separator if s in self.expression
            ]
            if separator_positions_and_lengths:
                sep_pos, separator = min(separator_positions_and_lengths, key=itemgetter(0))
            else:
                sep_pos = -1
        elif separator:
            sep_pos = self.expression.find(separator)
        else:
            sep_pos = -1

        if sep_pos < 0:
            value = self.expression
            self.expression = ''
            return value

        value = self.expression[:sep_pos]
        self.expression = self.expression[sep_pos + len(separator):]
        return value


def parse_file_uri(expr):
    t = Tokenizer(expr)
    scheme = t.get_next('://')
    domain = t.get_next('/')
    last_slash_position = t.expression.rfind('/')
    folder_path = '/' + t.expression[:last_slash_position + 1]
    file_pattern = t.expression[last_slash_position + 1:]

    return scheme, domain, folder_path, file_pattern


def format_file_uri(scheme, domain, *local_path_components):
    return f'{scheme}://{domain}{"/".join(local_path_components)}'


def reservoir_sample_and_size(iterable, k, seed):
    """
    Returns a sample of k items of iterable and its original size
    If the iterable contains less than k items the sample is a list of those items
    Algorithm used is reservoir sampling.

    :rtype list
    """
    random.seed(seed)

    # Put the first k elements in the reservoir.
    reservoir = list(itertools.islice(iterable, k))

    # If we have consumed all the elements, return them. Otherwise do the replacement.
    reservoir_size = len(reservoir)
    if reservoir_size < k:
        return reservoir, reservoir_size

    # If input size > k, continue the sampling process.
    for reservoir_size, item in enumerate(iterable, start=k + 1):
        # There are k elements in the reservoir, and the l-th element has been
        # consumed. It should be chosen with probability k/l. The expression
        # below is a random int chosen uniformly from [0, l)
        replacementIndex = random.randint(0, reservoir_size)
        if replacementIndex < k:
            reservoir[replacementIndex.toInt] = item

    return reservoir, reservoir_size


def compute_weighted_percentiles(weighted_values, number_of_percentiles, key=lambda x: x):
    """
    Compute weighted percentiles from a list of values and weights.

    number_of_percentiles evenly distributed percentiles values will be returned,
    including the 0th (minimal value) and the 100th (maximal value).

    A custom key function can be supplied to customize the sort order

    :type weighted_values: list of tuple
    :type number_of_percentiles: int
    :type key: function

    Examples with 0th, 50th and 100th percentiles:
    >>> compute_weighted_percentiles([(2, 0.2), (1, 0.1), (3, 0.7)], 3)
    [1, 3, 3]
    >>> compute_weighted_percentiles([(1, 10), (2, 20), (3, 20)], 3)
    [1, 2, 3]
    >>> compute_weighted_percentiles([(i, 1) for i in range(1, 101)], 1)
    Traceback (most recent call last):
     ...
    ValueError: number_of_percentiles must be at least 2
    >>> compute_weighted_percentiles([(i, 1) for i in range(1, 101)], 2)
    [1, 100]
    >>> compute_weighted_percentiles([(i, 1) for i in range(1, 101)], 3)
    [1, 50, 100]
    >>> compute_weighted_percentiles([(i, 1) for i in range(1, 101)], 4)
    [1, 34, 67, 100]
    >>> compute_weighted_percentiles([(i, 1) for i in range(1, 101)], 5)
    [1, 25, 50, 75, 100]
    >>> compute_weighted_percentiles([
    ...   ((1, "b"), 10),
    ...   ((2, "c"), 20),
    ...   ((3, "a"), 20)
    ... ], 3, key=lambda row: row[1])
    [(3, 'a'), (1, 'b'), (2, 'c')]
    """
    if number_of_percentiles == 1:
        raise ValueError("number_of_percentiles must be at least 2")

    ordered_values = sorted(weighted_values, key=lambda weighted_value: key(weighted_value[0]))
    total_weight = sum(weight for value, weight in ordered_values)

    bounds = []
    cumulative_weight = 0
    for value, weight in ordered_values:
        cumulative_weight += weight
        while len(bounds) / (number_of_percentiles - 1) <= cumulative_weight / total_weight:
            bounds.append(value)

    return bounds


def get_keyfunc(cols, schema, nulls_are_smaller=False):
    """
    Return a function that maps a row to a tuple of some of its columns values
    """

    def key(row):
        """
        Returns a tuple designed for comparisons based on a row

        Each requested columns is mapped to two columns:
        - The first indicate whether the column value is None or not
        - The second is the column value

        This prevent comparison between None and non-None values
        It also allows to defined how None values should be considered
        """
        values = []
        for col in cols:
            value = col.eval(row, schema)
            values += (
                (value is None) != nulls_are_smaller,
                value
            )
        return tuple(values)

    return key


FULL_WIDTH_REGEX = re.compile(
    "["
    + r"\u1100-\u115F"
    + r"\u2E80-\uA4CF"
    + r"\uAC00-\uD7A3"
    + r"\uF900-\uFAFF"
    + r"\uFE10-\uFE19"
    + r"\uFE30-\uFE6F"
    + r"\uFF00-\uFF60"
    + r"\uFFE0-\uFFE6"
    + "]"
)


def str_half_width(value):
    """
    Compute string length with full width characters counting for 2 normal ones
    """
    string = format_cell(value)
    if string is None:
        return 0
    if not isinstance(string, str):
        string = str(string)
    return len(string) + len(FULL_WIDTH_REGEX.findall(string))


def pad_cell(cell, truncate, col_width):
    """
    Compute how to pad the value "cell" truncated to truncate so that it fits col width
    :param cell: Any
    :param truncate: int
    :param col_width: int
    :return:
    """
    cell = format_cell(cell)
    cell_width = col_width - str_half_width(cell) + len(cell)
    if truncate > 0:
        return cell.rjust(cell_width)
    return cell.ljust(cell_width)


def format_cell(value):
    """
    Convert a cell value to a string using the logic needed in DataFrame.show()
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, Row):
        return f"[{', '.join(format_cell(sub_value) for sub_value in value)}]"
    if isinstance(value, dict):
        return "[{0}]".format(
            ", ".join(
                f"{format_cell(key)} -> {format_cell(sub_value)}"
                for key, sub_value in value.items()
            )
        )
    return str(value)


class MonotonicallyIncreasingIDGenerator:
    def __init__(self, partition_index):
        self.value = partition_index * 8589934592 - 1

    def __iter__(self):
        return self

    def __next__(self):
        self.value += 1
        return self.value


# pylint: disable=W0511
# todo: store random-related utils in a separated module
class XORShiftRandom:
    # pylint: disable=W0511
    # todo: align generated values with the ones in Spark
    def __init__(self, init):
        self.seed = XORShiftRandom.hashSeed(init)
        self.haveNextNextGaussian = False
        self.nextNextGaussian = 0

    def nextValue(self, bits):
        seed = self.seed
        nextSeed = seed ^ (seed << 21)
        nextSeed ^= (nextSeed >> 35)
        nextSeed ^= (nextSeed << 4)
        self.seed = nextSeed
        return int(nextSeed & ((1 << bits) - 1))

    def nextDouble(self):
        a = self.nextValue(26)
        b = self.nextValue(27)
        return ((a << 27) + b) * 1.1102230246251565E-16

    def nextGaussian(self):
        if self.haveNextNextGaussian:
            self.haveNextNextGaussian = False
            return self.nextNextGaussian

        v1 = 0
        v2 = 0
        s = 0
        while not 0 < s < 1:
            v1 = 2.0 * self.nextDouble() - 1
            v2 = 2.0 * self.nextDouble() - 1
            s = v1 * v1 + v2 * v2

        multiplier = math.sqrt(-2 * math.log(s) / s)
        self.nextNextGaussian = v2 * multiplier
        self.haveNextNextGaussian = True
        return v1 * multiplier

    @staticmethod
    def hashSeed(seed):
        as_bytes = seed.to_bytes(8, "big")
        lowBits = MurmurHash3.bytesHash(as_bytes)
        highBits = MurmurHash3.bytesHash(as_bytes, lowBits)
        return (highBits << 32) | (lowBits & 0xFFFFFFFF)


class MurmurHash3:
    @staticmethod
    def bytesHash(data, seed=0x3c074a61):
        length = len(data)
        h = seed

        # Body
        i = 0
        while length >= 4:
            k = data[i + 0] & 0xFF
            k |= (data[i + 1] & 0xFF) << 8
            k |= (data[i + 2] & 0xFF) << 16
            k |= (data[i + 3] & 0xFF) << 24

            h = MurmurHash3.mix(h, k)

            i += 4
            length -= 4

        # Tail
        k = 0
        if length == 3:
            k ^= (data[i + 2] & 0xFF) << 16
        if length >= 2:
            k ^= (data[i + 1] & 0xFF) << 8
        if length >= 1:
            k ^= (data[i + 0] & 0xFF)
            h = MurmurHash3.mixLast(h, k)

        # Finalization
        return MurmurHash3.finalizeHash(h, len(data))

    @staticmethod
    def mix(h, data):
        h = MurmurHash3.mixLast(h, data)
        h = MurmurHash3.rotl(h, 13)
        return h * 5 + 0xe6546b64

    @staticmethod
    def finalizeHash(h, length):
        return MurmurHash3.avalanche(h ^ length)

    @staticmethod
    def avalanche(h):
        h ^= h >> 16
        h *= 0x85ebca6b
        h ^= h >> 13
        h *= 0xc2b2ae35
        h ^= h >> 16
        return h

    @staticmethod
    def mixLast(h, k):
        k *= 0xcc9e2d51
        k = MurmurHash3.rotl(k, 15)
        k *= 0x1b873593

        return h ^ k

    @staticmethod
    def rotl(i, distance):
        return i << distance


def merge_rows(left, right):
    return create_row(
        itertools.chain(left.__fields__, right.__fields__),
        left + right
    )


def merge_rows_joined_on_values(left, right, left_schema, right_schema, how, on):
    left_names = left_schema.names
    right_names = right_schema.names

    left_on_fields, right_on_fields = get_on_fields(left_schema, right_schema, on)

    on_parts = [
        (on_field, left[on_field] if left is not None else right[on_field])
        for on_field in on
    ]

    if left is None and how in (FULL_JOIN, RIGHT_JOIN):
        left = create_row(left_names, [None for _ in left_names])
    if right is None and how in (LEFT_JOIN, FULL_JOIN):
        right = create_row(right_names, [None for _ in right_names])

    left_parts = (
        (field.name, value)
        for field, value in zip(left_schema.fields, left)
        if field not in left_on_fields
    )

    if how in (INNER_JOIN, CROSS_JOIN, LEFT_JOIN, FULL_JOIN, RIGHT_JOIN):
        right_parts = (
            (field.name, value)
            for field, value in zip(right_schema.fields, right)
            if field not in right_on_fields
        )
    elif how in (LEFT_SEMI_JOIN, LEFT_ANTI_JOIN):
        right_parts = ()
    else:
        raise IllegalArgumentException(f"Argument 'how' cannot be '{how}'")

    return row_from_keyed_values(itertools.chain(on_parts, left_parts, right_parts))


def strhash(string):
    """
    Old python hash function as described in PEP 456, excluding prefix, suffix and mask.

    :param string: string to hash
    :return: hash
    """
    if string == "":
        return 0

    x = ord(string[0]) << 7
    for c in string[1:]:
        x = ((1000003 * x) ^ ord(c)) & (1 << 32)
    x = (x ^ len(string))
    return x


def portable_hash(x):
    """
    This function returns consistent hash code for builtin types, especially
    for None and tuple with None.
    The algorithm is similar to that one used by CPython 2.7
    >>> portable_hash(None)
    0
    >>> portable_hash((None, 1)) & 0xffffffff
    219750521
    """

    if x is None:
        return 0
    if isinstance(x, list):
        return portable_hash(tuple(x))
    if isinstance(x, tuple):
        h = 0x345678
        for i in x:
            h ^= portable_hash(i)
            h *= 1000003
            h &= sys.maxsize
        h ^= len(x)
        if h == -1:
            h = -2
        return int(h)
    if isinstance(x, str):
        return strhash(x)
    if isinstance(x, datetime.datetime):
        return portable_hash(x.timetuple())
    return hash(x)


def parse_tz(tz):
    """
    Parse a string referencing a timezone which is either supported by pytz or
    in a GMT+1 or GMT+1:30 format.

    Returns a datetime.tzinfo if it was able to parse the string, None otherwise

    >>> parse_tz("GMT")
    <StaticTzInfo 'GMT'>
    >>> parse_tz("Europe/Paris")
    <DstTzInfo 'Europe/Paris' LMT+0:09:00 STD>
    >>> parse_tz("GMT+1")
    pytz.FixedOffset(60)
    >>> parse_tz("GMT+1:30")
    pytz.FixedOffset(90)
    >>> parse_tz("MalformedString")  # returns None
    """
    try:
        return pytz.timezone(tz)
    except UnknownTimeZoneError:
        GMT_PATTERN = r'GMT(?P<sign>[+-])(?P<hours>[0-9]{1,2})(?::(?P<minutes>[0-9]{2}))?'
        match = re.match(GMT_PATTERN, tz)
        if match:
            return parse_gmt_based_offset(match)
        return None


def parse_gmt_based_offset(match):
    # GMT+2 or GMT+2:30 case
    sign, hours, minutes = match.groups()
    sign = -1 if sign == "-" else 1
    try:
        hours = int(hours)
        minutes = int(minutes) if minutes else 0
    except ValueError:
        return None

    if 0 <= hours < 24 and 0 <= minutes < 60:
        offset = sign * (hours * 60 + minutes)
        return pytz.FixedOffset(offset)
    return None


def half_up_round(value, scale):
    """
    Round values using the "half up" logic

    See more: https://docs.python.org/3/library/decimal.html#rounding-modes

    >>> half_up_round(7.5, 0)
    8.0
    >>> half_up_round(6.5, 0)
    7.0
    >>> half_up_round(-7.5, 0)
    -8.0
    >>> half_up_round(-6.5, 0)
    -7.0
    """
    # Python2 and Python3's round behavior differs for rounding e.g. 0.5
    # hence we handle the "half" case so that it is rounded up
    scaled_value = (value * (10 ** scale))
    removed_part = scaled_value % 1
    if removed_part == 0.5:
        sign = -1 if value < 0 else 1
        value += 10 ** -(scale + 1) * sign
    return round(value, scale)


def half_even_round(value, scale):
    """
    Round values using the "half even" logic

    See more: https://docs.python.org/3/library/decimal.html#rounding-modes

    >>> half_even_round(7.5, 0)
    8.0
    >>> half_even_round(6.5, 0)
    6.0
    >>> half_even_round(-7.5, 0)
    -8.0
    >>> half_even_round(-6.5, 0)
    -6.0
    """
    # Python2 and Python3's round behavior differs for rounding e.g. 0.5
    # hence we handle the "half" case so that it round even up and odd down
    if scale > 0:
        return round(value, scale)
    scaled_value = (value * (10 ** scale))
    removed_part = scaled_value % 1
    if removed_part == 0.5:
        rounded_part = int(scaled_value)
        is_even = (rounded_part + max(0, scale)) % 2 == 0
        sign = -1 if value < 0 else 1
        if is_even:
            value -= 10 ** -(scale + 1) * sign
        else:
            value += 10 ** -(scale + 1) * sign
    return round(value, scale)


def levenshtein_distance(str1, str2):
    if str1 == "":
        return len(str2)
    if str2 == "":
        return len(str1)
    return min(
        levenshtein_distance(str1[1:], str2[1:]) + (str1[0] != str2[0]),
        levenshtein_distance(str1[1:], str2) + 1,
        levenshtein_distance(str1, str2[1:]) + 1
    )


def get_json_encoder(options):
    """
    Returns a JsonEncoder which convert Rows to json with the same behavior
    and conversion logic as PySpark.

    :param date_formatter: a function that convert a date into a string
    :param timestamp_formatter: a function that convert a timestamp into a string
    :return: type
    """
    date_format = options.get("dateformat", "yyyy-MM-dd")
    timestamp_format = options.get("timestampformat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    date_formatter = get_time_formatter(date_format)
    timestamp_formatter = get_time_formatter(timestamp_format)

    class CustomJSONEncoder(json.JSONEncoder):
        def encode(self, o):
            def encode_rows(item):
                if isinstance(item, Row):
                    return collections.OrderedDict(
                        (key, encode_rows(value))
                        for key, value in zip(item.__fields__, item)
                    )
                if isinstance(item, (list, tuple)):
                    return [encode_rows(e) for e in item]
                if isinstance(item, dict):
                    return collections.OrderedDict(
                        (key, encode_rows(value))
                        for key, value in item.items()
                    )
                return item

            return super().encode(encode_rows(o))

        # default can be overridden if passed a parameter during init
        # pylint doesn't like the behavior but it is the expected one
        # pylint: disable=E0202
        def default(self, o):
            if isinstance(o, datetime.datetime):
                return timestamp_formatter(o)
            if isinstance(o, datetime.date):
                return date_formatter(o)
            return super().default(o)

    return CustomJSONEncoder


class NotSupportedByThisSparkVersion(Exception):
    pass


def _decorate_with(decorator):
    def inner(obj):
        if inspect.isclass(obj):
            # Process it as a class, decorating all methods of the class
            for name, method in inspect.getmembers(obj, inspect.isroutine):
                setattr(obj, name, decorator(method))
            return obj

        # Otherwise, just assume it's a method here
        return decorator(obj)

    return inner


def since(need_spark_version: str):
    """
    Decorator that tells pysparkling to only allow access to this method when the spark version is higher or equal to
      the need_spark_version.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if config.spark_version is not None and config.spark_version < need_spark_version:
                raise NotSupportedByThisSparkVersion

            return func(*args, **kwargs)
        return wrapper

    return _decorate_with(decorator)


def until(gone_at_spark_version: str):
    """
    Decorator that tells pysparkling to only allow access to this method when the spark version is lower than the
      gone_at_spark_version.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if config.spark_version is not None and config.spark_version >= gone_at_spark_version:
                raise NotSupportedByThisSparkVersion

            return func(*args, **kwargs)

        return wrapper

    return _decorate_with(decorator)
