import datetime
from functools import lru_cache
import os
import re
import time

from .pandas.utils import require_minimum_pandas_version
from .types import StringType

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
        # pylint: disable=import-outside-toplevel, cyclic-import
        from ._casts import cast_to_timestamp

        return lambda value: cast_to_timestamp(value, StringType(), {})

    if java_time_format is NO_TIMESTAMP_CONVERSION:
        return lambda value: None

    python_pattern = ""
    for token, _ in JAVA_TIME_FORMAT_TOKENS.findall(java_time_format):
        python_pattern += FORMAT_MAPPING.get(token, token)
    return lambda value: datetime.datetime.strptime(value, python_pattern)


def _get_local_timezone():
    """ Get local timezone using pytz with environment variable, or dateutil.

    If there is a 'TZ' environment variable, pass it to pandas to use pytz and use it as timezone
    string, otherwise use the special word 'dateutil/:' which means that pandas uses dateutil and
    it reads system configuration to know the system local timezone.

    See also:
    - https://github.com/pandas-dev/pandas/blob/0.19.x/pandas/tslib.pyx#L1753
    - https://github.com/dateutil/dateutil/blob/2.6.1/dateutil/tz/tz.py#L1338
    """
    return os.environ.get('TZ', 'dateutil/:')


def _check_series_localize_timestamps(s, timezone):
    """
    Convert timezone aware timestamps to timezone-naive in the specified timezone or local timezone.

    If the input series is not a timestamp series, then the same series is returned. If the input
    series is a timestamp series, then a converted series is returned.

    :param s: pandas.Series
    :param timezone: the timezone to convert. if None then use local timezone
    :return pandas.Series that have been converted to tz-naive
    """
    require_minimum_pandas_version()

    try:
        # pandas is an optional dependency
        # pylint: disable=import-outside-toplevel
        from pandas.api.types import is_datetime64tz_dtype
    except ImportError as e:
        raise Exception("require_minimum_pandas_version() was not called") from e
    tz = timezone or _get_local_timezone()
    # pylint: disable=W0511
    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
    if is_datetime64tz_dtype(s.dtype):
        return s.dt.tz_convert(tz).dt.tz_localize(None)
    return s


def _check_dataframe_localize_timestamps(pdf, timezone):
    """
    Convert timezone aware timestamps to timezone-naive in the specified timezone or local timezone

    :param pdf: pandas.DataFrame
    :param timezone: the timezone to convert. if None then use local timezone
    :return pandas.DataFrame where any timezone aware columns have been converted to tz-naive
    """
    require_minimum_pandas_version()

    for column, series in pdf.iteritems():
        pdf[column] = _check_series_localize_timestamps(series, timezone)
    return pdf


def _check_series_convert_timestamps_internal(s, timezone):
    """
    Convert a tz-naive timestamp in the specified timezone or local timezone to UTC normalized for
    Spark internal storage

    :param s: a pandas.Series
    :param timezone: the timezone to convert. if None then use local timezone
    :return pandas.Series where if it is a timestamp, has been UTC normalized without a time zone
    """
    require_minimum_pandas_version()

    try:
        # pandas is an optional dependency
        # pylint: disable=import-outside-toplevel
        from pandas.api.types import is_datetime64_dtype, is_datetime64tz_dtype
    except ImportError as e:
        raise Exception("require_minimum_pandas_version() was not called") from e

    # pylint: disable=W0511
    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
    if is_datetime64_dtype(s.dtype):
        # When tz_localize a tz-naive timestamp, the result is ambiguous if the tz-naive
        # timestamp is during the hour when the clock is adjusted backward during due to
        # daylight saving time (dst).
        # E.g., for America/New_York, the clock is adjusted backward on 2015-11-01 2:00 to
        # 2015-11-01 1:00 from dst-time to standard time, and therefore, when tz_localize
        # a tz-naive timestamp 2015-11-01 1:30 with America/New_York timezone, it can be either
        # dst time (2015-01-01 1:30-0400) or standard time (2015-11-01 1:30-0500).
        #
        # Here we explicit choose to use standard time. This matches the default behavior of
        # pytz.
        #
        # Here are some code to help understand this behavior:
        # >>> import datetime
        # >>> import pandas as pd
        # >>> import pytz
        # >>>
        # >>> t = datetime.datetime(2015, 11, 1, 1, 30)
        # >>> ts = pd.Series([t])
        # >>> tz = pytz.timezone('America/New_York')
        # >>>
        # >>> ts.dt.tz_localize(tz, ambiguous=True)
        # 0   2015-11-01 01:30:00-04:00
        # dtype: datetime64[ns, America/New_York]
        # >>>
        # >>> ts.dt.tz_localize(tz, ambiguous=False)
        # 0   2015-11-01 01:30:00-05:00
        # dtype: datetime64[ns, America/New_York]
        # >>>
        # >>> str(tz.localize(t))
        # '2015-11-01 01:30:00-05:00'
        tz = timezone or _get_local_timezone()
        return s.dt.tz_localize(tz, ambiguous=False).dt.tz_convert('UTC')
    if is_datetime64tz_dtype(s.dtype):
        return s.dt.tz_convert('UTC')
    return s


def _check_series_convert_timestamps_localize(s, from_timezone, to_timezone):
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    :param s: a pandas.Series
    :param from_timezone: the timezone to convert from. if None then use local timezone
    :param to_timezone: the timezone to convert to. if None then use local timezone
    :return pandas.Series where if it is a timestamp, has been converted to tz-naive
    """
    require_minimum_pandas_version()

    try:
        # pandas is an optional dependency
        # pylint: disable=import-outside-toplevel
        import pandas as pd
        from pandas.api.types import is_datetime64_dtype, is_datetime64tz_dtype
    except ImportError as e:
        raise Exception("require_minimum_pandas_version() was not called") from e

    from_tz = from_timezone or _get_local_timezone()
    to_tz = to_timezone or _get_local_timezone()
    # pylint: disable=W0511
    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
    if is_datetime64tz_dtype(s.dtype):
        return s.dt.tz_convert(to_tz).dt.tz_localize(None)
    if is_datetime64_dtype(s.dtype) and from_tz != to_tz:
        # `s.dt.tz_localize('tzlocal()')` doesn't work properly when including NaT.
        return s.apply(
            lambda ts: ts.tz_localize(from_tz, ambiguous=False).tz_convert(to_tz).tz_localize(None)
            if ts is not pd.NaT else pd.NaT)
    return s


def _check_series_convert_timestamps_local_tz(s, timezone):
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    :param s: a pandas.Series
    :param timezone: the timezone to convert to. if None then use local timezone
    :return pandas.Series where if it is a timestamp, has been converted to tz-naive
    """
    return _check_series_convert_timestamps_localize(s, None, timezone)


def _check_series_convert_timestamps_tz_local(s, timezone):
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    :param s: a pandas.Series
    :param timezone: the timezone to convert from. if None then use local timezone
    :return pandas.Series where if it is a timestamp, has been converted to tz-naive
    """
    return _check_series_convert_timestamps_localize(s, timezone, None)
