import datetime
import re
from functools import lru_cache

import pytz

from pysparkling.sql.types import TimestampType, DateType, StringType
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


def cast_to_date(value, from_type, options):
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        # Spark cast only considers the first non empty part before a ' ' or a 'T'
        if ' ' in value:
            value = value.strip().split(" ")[0]
        if 'T' in value:
            value = value.split("T")[0]
        date_components = value.split("-")
        if len(date_components) > 3 or len(date_components[0]) != 4:
            return None
        # default month and day to 1
        date_components += ([1] * (3 - len(date_components)))
        try:
            return datetime.date(*map(int, date_components))
        except ValueError:
            return None
    if isinstance(from_type, (TimestampType, DateType, StringType)):
        return None  # other values would have been handle in the lines above

    raise AnalysisException("Cannot cast type {0} to date".format(from_type))


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
