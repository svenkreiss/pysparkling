import datetime
import re
import time
from functools import lru_cache

import pytz
from dateutil.tz import tzlocal

from pysparkling.sql.types import TimestampType, DateType, StringType, NumericType, BooleanType, BinaryType, StructType
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


def default_timestamp_formatter(timestamp):
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def cast_to_string(value, from_type, options):
    date_format = get_time_formatter(options.get("dateformat", "yyyy-MM-dd"))
    timestamp_format = (get_time_formatter(options["timestampformat"])
                        if "timestampformat" in options
                        else default_timestamp_formatter)
    if value is None:
        return "null"
    if isinstance(from_type, DateType):
        return date_format(value)
    if isinstance(from_type, TimestampType):
        return timestamp_format(value)
    if isinstance(from_type, BooleanType):
        return str(value).lower()
    return str(value)


def cast_sequence(value, from_type, options):
    if isinstance(from_type, StructType):
        types = [field.dataType for field in from_type.fields]
    else:
        types = [from_type.elementType] * len(value)
    casted_values = [
        cast_to_string(
            sub_value, sub_value_type, options
        ) if sub_value is not None else None
        for sub_value, sub_value_type in zip(value, types)
    ]
    casted_value = "[{0}]".format(",".join(
        ("" if casted_value is None else "{0}" if i == 0 else " {0}").format(casted_value)
        for i, casted_value in enumerate(casted_values)
    ))
    return casted_value


def cast_to_binary(value, from_type, options):
    if isinstance(from_type, StringType):
        # noinspection PyTypeChecker
        return bytearray(value, 'utf-8')
    if isinstance(from_type, BinaryType):
        return value
    raise AnalysisException("Cannot cast type {0} to binary".format(from_type))


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


def parse_time_as_string(time_as_string):
    time_as_string = time_as_string.strip()
    match = TIME_REGEX.match(time_as_string)
    if not match:
        return None

    hour, minute, second, microsecond, tz_as_string = match.groups()
    if microsecond:
        microsecond = microsecond[:6]  # mimic Spark behaviour

    tzinfo = parse_timezone(tz_as_string)
    if tzinfo is None:  # Unable to parse
        return None

    return dict(
        hour=int(hour),
        minute=int(minute) if minute else 0,
        second=int(second) if second else 0,
        microsecond=int(microsecond) if microsecond else 0,
        tzinfo=tzinfo
    )


def parse_timezone(tz_as_string):
    if tz_as_string:
        if tz_as_string == "Z":
            tzinfo = GMT
        else:
            sign = tz_as_string[0]
            coeff = 1 if sign == "+" else -1
            if tz_as_string.count(":") == 1:
                hours, minutes = tz_as_string[1:].split(":")
                hours, minutes = int(hours), (int(minutes) if minutes else 0)
            elif tz_as_string.count(":") == 0:
                hours, minutes = int(tz_as_string[1:]), 0
            else:
                return None
            offset = coeff * datetime.timedelta(
                hours=hours,
                minutes=minutes
            )
            tzinfo = datetime.timezone(offset)
    else:
        tzinfo = tzlocal()
    return tzinfo


def cast_to_timestamp(value, from_type, options):
    if value == "" or value is None:
        return None
    if isinstance(value, str):
        date_as_string, time_as_string = split_datetime_as_string(value)
        date = cast_to_date(date_as_string, from_type, options=options)
        time_of_day = parse_time_as_string(time_as_string)

        return None if date is None or time_of_day is None else datetime.datetime(
            year=date.year,
            month=date.month,
            day=date.day,
            **time_of_day
        ).astimezone(tzlocal()).replace(tzinfo=None)
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime(
            year=value.year,
            month=value.month,
            day=value.day
        )
    if isinstance(value, (int, float)):
        return datetime.datetime.fromtimestamp(value)
    if isinstance(from_type, (StringType, TimestampType, NumericType, BooleanType)):
        return None
    raise AnalysisException("Cannot cast type {0} to timestamp".format(from_type))


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


def cast_value(value, options):
    if value == "":
        return None
    if isinstance(value, datetime.datetime):
        return value.timestamp()
    if isinstance(value, datetime.date):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    raise ValueError("Unable to cast from value")


def cast_to_user_defined_type(value, from_type, options):
    raise NotImplementedError("Pysparkling does not support yet cast to UDF")


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


def get_unix_timestamp_parser(java_time_format):
    datetime_parser = get_datetime_parser(java_time_format)

    def time_parser(value):
        dt = datetime_parser(value)
        return int(time.mktime(dt.timetuple()))

    return time_parser


@lru_cache
def get_datetime_parser(java_time_format):
    if java_time_format is None:
        return lambda value: cast_to_timestamp(value, StringType(), {})

    if java_time_format is NO_TIMESTAMP_CONVERSION:
        return lambda value: None

    python_pattern = ""
    for token, _ in JAVA_TIME_FORMAT_TOKENS.findall(java_time_format):
        python_pattern += FORMAT_MAPPING.get(token, token)
    return lambda value: datetime.datetime.strptime(value, python_pattern)
