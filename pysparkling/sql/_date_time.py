import datetime
import re
import time
from functools import lru_cache


NO_TIMESTAMP_CONVERSION = object()

JAVA_TIME_FORMAT_TOKENS = re.compile("(([a-zA-Z])\\2*|[^a-zA-Z]+)")

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
            return f"{tz[:3]}:{tz[3:]}" if tz else ""

        return timezone_formatter

    return lambda value: token


@lru_cache(64)
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


def get_unix_timestamp_parser(java_time_format):
    datetime_parser = get_datetime_parser(java_time_format)

    def time_parser(value):
        dt = datetime_parser(value)
        return int(time.mktime(dt.timetuple()))

    return time_parser


@lru_cache(64)
def get_datetime_parser(java_time_format):
    if java_time_format is None:
        return lambda value: cast_to_timestamp(value, StringType(), {})

    if java_time_format is NO_TIMESTAMP_CONVERSION:
        return lambda value: None

    python_pattern = ""
    for token, _ in JAVA_TIME_FORMAT_TOKENS.findall(java_time_format):
        python_pattern += FORMAT_MAPPING.get(token, token)
    return lambda value: datetime.datetime.strptime(value, python_pattern)
