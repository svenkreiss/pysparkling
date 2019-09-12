import datetime
import re
import time as _time
from functools import partial

from pysparkling.sql.types import UserDefinedType, NumericType
from pysparkling.sql.utils import AnalysisException
from pysparkling.sql.types import *
from pysparkling.utils import row_from_keyed_values

TIME_REGEX = re.compile("^([0-9]+):([0-9]+)?(?::([0-9]+))?(?:\\.([0-9]+))?(Z|[+-][0-9]+(?::(?:[0-9]+)?)?)?$")

tz_utc = datetime.timezone(datetime.timedelta(seconds=0))
tz_local = datetime.timezone(datetime.timedelta(seconds=-(_time.altzone if _time.daylight else _time.timezone)))


def identity(value):
    return value


def cast_from_none(value, from_type):
    if value is not None:
        raise AnalysisException(
            "Expected a null value from a field with type {0}, got {1}".format(
                from_type,
                value
            )
        )
    return None


def cast_to_string(value, from_type, date_format="%Y-%m-%d", timestamp_format="%Y-%m-%d %H:%M:%S"):
    if value is None:
        return "null"
    if isinstance(from_type, DateType):
        return value.strftime(date_format)
    if isinstance(from_type, TimestampType):
        return value.strftime(timestamp_format)
    if isinstance(from_type, ArrayType) or isinstance(from_type, StructType):
        if isinstance(from_type, StructType):
            types = [field.dataType for field in from_type.fields]
        else:
            types = [from_type.elementType] * len(value)
        casted_values = [
            cast_to_string(sub_value, sub_value_type) if sub_value is not None else None
            for sub_value, sub_value_type in zip(value, types)
        ]
        return "[{0}]".format(",".join(
            ("" if casted_value is None else "{0}" if i == 0 else " {0}").format(casted_value)
            for i, casted_value in enumerate(casted_values)
        ))
    if isinstance(from_type, MapType):
        casted_values = [
            (cast_to_string(key, from_type.keyType),
             cast_to_string(sub_value, from_type.valueType) if sub_value is not None else None)
            for key, sub_value in value.items()
        ]
        return "[{0}]".format(
            ", ".join("{0} ->{1}".format(
                casted_key,
                " {0}".format(casted_value) if casted_value is not None else ""
            ) for casted_key, casted_value in casted_values)
        )
    if isinstance(from_type, BooleanType):
        return str(value).lower()
    return str(value)


def cast_to_binary(value, from_type):
    if isinstance(from_type, StringType):
        # noinspection PyTypeChecker
        return bytearray(value, 'utf-8')
    if isinstance(from_type, BinaryType):
        return value
    raise AnalysisException("Cannot cast type {0} to binary".format(from_type))


def cast_to_date(value, from_type):
    if isinstance(from_type, StringType):
        # Spark cast only considers the first non empty part before a ' ' or a 'T'
        cleaned_value = value.strip().split(" ")[0].split("T")[0]
        date_components = cleaned_value.split("-")
        if len(date_components) > 3 or len(date_components[0]) != 4:
            return None
        # default month and day to 1
        date_components += ([1] * (3 - len(date_components)))
        try:
            return datetime.date(*map(int, date_components))
        except ValueError:
            return None
    if isinstance(from_type, TimestampType):
        return value.date()
    if isinstance(from_type, DateType):
        return value
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
            tzinfo = tz_utc
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
        tzinfo = tz_local
    return tzinfo


def cast_to_timestamp(value, from_type):
    if isinstance(from_type, StringType):
        date_as_string, time_as_string = split_datetime_as_string(value)
        date = cast_to_date(date_as_string, from_type)
        time = parse_time_as_string(time_as_string)

        if date is None:
            return None
        if time is None:
            return None

        return datetime.datetime(
            year=date.year,
            month=date.month,
            day=date.day,
            **time
        ).astimezone(tz_local).replace(tzinfo=None)
    if isinstance(from_type, DateType):
        return datetime.datetime(
            year=value.year,
            month=value.month,
            day=value.day
        )
    if isinstance(from_type, TimestampType):
        return value
    if isinstance(from_type, (NumericType, BooleanType)):
        return datetime.datetime.fromtimestamp(value)
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


def cast_to_boolean(value, from_type):
    if isinstance(from_type, StringType):
        return True if value.lower() == "true" else False if value.lower() == "false" else None
    if isinstance(from_type, (NumericType, BooleanType)):
        return bool(value)
    raise AnalysisException("Cannot cast type {0} to boolean".format(from_type))


def _cast_to_bounded_type(name, min_value, max_value, value, from_type):
    size = max_value - min_value + 1
    if isinstance(from_type, DateType):
        return None
    if isinstance(from_type, TimestampType):
        return cast_to_byte(cast_to_float(value, from_type), FloatType())
    if isinstance(from_type, StringType):
        casted_value = int(value)
        return casted_value if min_value <= casted_value <= max_value else None
    if isinstance(from_type, (NumericType, BooleanType)):
        value = int(value)
        return value % size if value % size <= max_value else value % -size
    raise AnalysisException("Cannot cast type {0} to {1}".format(from_type, name))


def cast_to_byte(value, from_type):
    return _cast_to_bounded_type("byte", -128, 127, value, from_type)


def cast_to_short(value, from_type):
    return _cast_to_bounded_type("short", -32768, 32767, value, from_type)


def cast_to_int(value, from_type):
    return _cast_to_bounded_type("int", -2147483648, 2147483647, value, from_type)


def cast_to_long(value, from_type):
    return _cast_to_bounded_type("long", -9223372036854775808, 9223372036854775807, value, from_type)


def cast_to_decimal(value, from_type, to_type):
    value_as_float = cast_to_float(value, from_type)
    if value_as_float is None:
        return None
    if value_as_float >= 10 ** (to_type.precision - to_type.scale):
        return None
    if to_type.scale == 0:
        return int(value_as_float)
    return round(value_as_float, ndigits=to_type.scale)


def cast_to_float(value, from_type):
    # pysparkling does not mimic the loss of accuracy of Spark nor value
    # bounding between float min&max values
    if isinstance(from_type, DateType):
        return None
    if isinstance(from_type, TimestampType):
        return float(value.timestamp())
    if isinstance(from_type, (StringType, NumericType)):
        return float(value)
    raise AnalysisException("Cannot cast type {0} to float".format(from_type))


def cast_to_double(value, from_type):
    return cast_to_float(value, from_type)


def cast_to_array(value, from_type, to_type):
    if isinstance(from_type, ArrayType):
        caster = get_caster(from_type=from_type.elementType, to_type=to_type.elementType)
        return [
            caster(sub_value) if sub_value is not None else None
            for sub_value in value
        ]
    raise AnalysisException("Cannot cast type {0} to array".format(from_type))


def cast_to_map(value, from_type, to_type):
    if isinstance(from_type, MapType):
        key_caster = get_caster(from_type=from_type.keyType, to_type=to_type.keyType)
        value_caster = get_caster(from_type=from_type.valueType, to_type=to_type.valueType)
        return {
            key_caster(key): (value_caster(sub_value) if sub_value is not None else None)
            for key, sub_value in value.items()
        }
    raise AnalysisException("Cannot cast type {0} to map".format(from_type))


def cast_to_struct(value, from_type, to_type):
    if isinstance(from_type, StructType):
        return row_from_keyed_values([
            (to_field.name, get_caster(from_field.dataType, to_field.dataType)(sub_value))
            for from_field, to_field, sub_value in zip(from_type.fields, to_type.fields, value)
        ])
    raise NotImplementedError("Pysparkling does not support yet cast to struct")


def cast_to_user_defined_type(value, from_type):
    raise NotImplementedError("Pysparkling does not support yet cast to UDF")


DESTINATION_DEPENDENT_CASTERS = {
    DecimalType: cast_to_decimal,
    ArrayType: cast_to_array,
    MapType: cast_to_map,
    StructType: cast_to_struct,
}

CASTERS = {
    StringType: cast_to_string,
    BinaryType: cast_to_binary,
    DateType: cast_to_date,
    TimestampType: cast_to_timestamp,
    # The ticket to expose CalendarIntervalType, in pyspark is SPARK-28492
    # It is open as this function is written, so we do not support it at the moment.
    # CalendarIntervalType: cast_to_interval,
    BooleanType: cast_to_boolean,
    ByteType: cast_to_byte,
    ShortType: cast_to_short,
    IntegerType: cast_to_int,
    FloatType: cast_to_float,
    LongType: cast_to_long,
    DoubleType: cast_to_double,
    UserDefinedType: cast_to_user_defined_type,
}


def get_caster(from_type, to_type):
    to_type_class = to_type.__class__
    if from_type == to_type:
        return identity
    if to_type_class == NullType:
        return partial(cast_from_none, from_type=from_type)
    if to_type_class in DESTINATION_DEPENDENT_CASTERS:
        return partial(DESTINATION_DEPENDENT_CASTERS[to_type_class], from_type=from_type, to_type=to_type)
    if to_type_class in CASTERS:
        return partial(CASTERS[to_type_class], from_type=from_type)
    raise AnalysisException("Cannot cast from {0} to {1}".format(from_type, to_type))


FORMAT_MAPPING = {
    "EEEE": "%A",
    "EEE": "%a",
    "EE": "%a",
    "E": "%a",
    "e": "%w",
    "dd": "%-d",
    "d": "%d",
    "MMMM": "%B",
    "MMM": "%b",
    "MM": "%m",
    "M": "%-m",
    "yyyy": "%Y",
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


def convert_token_to_python(group):
    token, letter = group

    if token in FORMAT_MAPPING:
        return FORMAT_MAPPING[token]

    if token in ("'", "[", "]"):
        return ""

    return token


def convert_time_format_to_python(java_time_format):
    """
    Convert a Java time format to a Python time format.

    This function currently only support a small subset of Java time formats.

    Example:
    >>> convert_time_format_to_python("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    %Y-%m-%-dT%H:%M:%S.oops
    """
    r = re.compile("(([a-zA-Z])\\2*|[^a-zA-Z]+)")
    return "".join(
        convert_token_to_python(token)
        for token in r.findall(java_time_format)
    )
