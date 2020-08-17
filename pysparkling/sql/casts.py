import datetime
import re
from functools import lru_cache

import pytz
from pysparkling.sql.utils import AnalysisException

NO_TIMESTAMP_CONVERSION = object()

JAVA_TIME_FORMAT_TOKENS = re.compile("(([a-zA-Z])\\2*|[^a-zA-Z]+)")

TIME_REGEX = re.compile(
    "^([0-9]+):([0-9]+)?(?::([0-9]+))?(?:\\.([0-9]+))?(Z|[+-][0-9]+(?::(?:[0-9]+)?)?)?$"
)

GMT = pytz.timezone("GMT")


def identity(value, options):
    return value


def cast_from_none(value, from_type, options):
    if value is None:
        return None
    raise AnalysisException(
        "Expected a null value from a field with type {0}, got {1}".format(
            from_type,
            value
        )
    )


def split_datetime_as_string(value):
    first_space_position = (value.find(' ') + 1) or len(value)
    first_t_position = (value.find('T') + 1) or len(value)
    if first_space_position == len(value) and first_t_position == len(value):
        if ":" in value:
            # Value is only a time
            return datetime.date.today().strftime("%Y-%m-%d"), value
        # Value is only a date
        return value, "00:00:00"
    # Value is a datetime
    separation = min(first_space_position, first_t_position)
    date_as_string = value[:separation]
    time_as_string = value[separation:]
    return date_as_string, time_as_string


FORMAT_MAPPING = {
    "EEEE": "%A",
    "EEE": "%a",
    "EE": "%a",
    "E": "%a",
    "e": "%w",
    "dd": "%d",
    "d": "%-d",
    "MMMM": "%B",
    "MMM": "%b",
    "MM": "%m",
    "M": "%-m",
    "yyyy": "%Y",
    "yyy": "%Y",
    "yy": "%y",
    "y": "%Y",
    "HH": "%H",
    "H": "%-H",
    "hh": "%I",
    "h": "%-I",
    "a": "%p",
    "mm": "%M",
    "m": "%-M",
    "ss": "%S",
    "s": "%-S",
    "S": "%f",
    "xxxx": "%z",
    "xx": "%z",
    "ZZZ": "%z",
    "ZZ": "%z",
    "Z": "%z",
    "DDD": "%j",
    "D": "%-j",
}


def get_sub_formatter(group):
    token, _ = group

    if token in FORMAT_MAPPING:
        return lambda value: value.strftime(FORMAT_MAPPING[token])

    if token in ("'", "[", "]"):
        return lambda value: ""

    if "S" in token:
        number_of_digits = len(token)
        return lambda value: value.strftime("%f")[:number_of_digits]

    if token == "XXX":
        def timezone_formatter(value):
            tz = value.strftime("%z")
            return "{0}{1}{2}:{3}{4}".format(*tz) if tz else ""

        return timezone_formatter

    return lambda value: token


@lru_cache
def get_time_formatter(java_time_format):
    """
    Convert a Java time format to a Python time format.

    This function currently only support a small subset of Java time formats.
    """
    sub_formatters = [
        get_sub_formatter(token)
        for token in JAVA_TIME_FORMAT_TOKENS.findall(java_time_format)
    ]

    def time_formatter(value):
        return "".join(sub_formatter(value) for sub_formatter in sub_formatters)

    return time_formatter
