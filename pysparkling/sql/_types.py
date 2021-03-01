import datetime
from typing import Dict

from .types import (
    BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, LongType, ShortType,
    StringType, TimestampType
)
from .utils import ParseException


def _create_row_inbound_converter(dataType):
    return lambda *a: dataType.fromInternal(a)


STRING_TO_TYPE = dict(
    boolean=BooleanType(),
    tinyint=ByteType(),
    byte=ByteType(),
    smallint=ShortType(),
    short=ShortType(),
    int=LongType(),
    integer=LongType(),
    bigint=LongType(),
    long=LongType(),
    float=FloatType(),
    double=DoubleType(),
    date=DateType(),
    timestamp=TimestampType(),
    string=StringType(),
    binary=BinaryType(),
    decimal=DecimalType()
)


def string_to_type(string):
    if string in STRING_TO_TYPE:
        return STRING_TO_TYPE[string]
    if string.startswith("decimal("):
        arguments = string[8:-1]
        if arguments.count(",") == 1:
            precision, scale = arguments.split(",")
        else:
            precision, scale = arguments, 0
        return DecimalType(precision=int(precision), scale=int(scale))
    raise ParseException(f"Unable to parse data type {string}")


INTERNAL_TYPE_ORDER = [
    float,
    int,
    str,
    bool,
    datetime.datetime,
    datetime.date,
]
PYTHON_TO_SPARK_TYPE: Dict[type, DataType] = {
    float: FloatType(),
    int: LongType(),
    str: StringType(),
    bool: BooleanType(),
    datetime.datetime: TimestampType(),
    datetime.date: DateType(),
}


def python_to_spark_type(python_type):
    """
    :type python_type: type
    :rtype DataType
    """
    # pylint: disable=W0511
    # todo: support Decimal
    if python_type in PYTHON_TO_SPARK_TYPE:
        return PYTHON_TO_SPARK_TYPE[python_type]
    raise NotImplementedError(
        f"Pysparkling does not currently support type {python_type} for the requested operation"
    )
