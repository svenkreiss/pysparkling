from pyspark.sql.types import StringType

from pysparkling.sql.expressions.aggregate.HyperLogLogPlusPlus import HyperLogLogPlusPlus
from pysparkling.sql.column import parse, Column
from pysparkling.sql.expressions.mappers import *


def _get_stat_helper():
    from pysparkling.stat_counter import ColumnStatHelper
    return ColumnStatHelper


def col(colName):
    """
    :rtype: Column
    """
    return Column(colName)


def column(colName):
    """
    :rtype: Column
    """
    return Column(colName)


def lit(literal):
    """
    :rtype: Column
    """
    return typedLit(literal)


def typedLit(literal):
    """
    :rtype: Column
    """
    return parse(literal)


def asc(columnName):
    """
    :rtype: Column
    """
    return Column(columnName).asc


def asc_nulls_first(columnName):
    """
    :rtype: Column
    """
    return Column(columnName).asc_nulls_first


def asc_nulls_last(columnName):
    """
    :rtype: Column
    """
    return Column(columnName).asc_nulls_last


def desc(columnName):
    """
    :rtype: Column
    """
    return Column(columnName).desc


def desc_nulls_first(columnName):
    """
    :rtype: Column
    """
    return Column(columnName).desc_nulls_first


def desc_nulls_last(columnName):
    """
    :rtype: Column
    """
    return Column(columnName).desc_nulls_last


def approx_count_distinct(e, rsd=0.05):
    """
    :rtype: Column
    """
    return HyperLogLogPlusPlus(parse(e), relativeSD=rsd)


def avg(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import Avg
    return col(Avg(column=parse(e)))


def collect_list(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.collectors import CollectList
    return col(CollectList(column=parse(e)))


def collect_set(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.collectors import CollectSet
    return CollectSet(column=parse(e))


def corr(column1, column2):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.covariance_aggregations import Corr
    return Corr(
        column1=parse(column1),
        column2=parse(column2)
    )


def count(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import Count
    return col(Count(column=parse(e)))


def countDistinct(*exprs):
    """
    :rtype: Column
    """
    columns = [parse(e) for e in exprs]
    from pysparkling.sql.expressions.aggregate.collectors import CountDistinct
    return CountDistinct(columns=columns)


def covar_pop(column1, column2):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.covariance_aggregations import CovarPop
    return CovarPop(
        column1=parse(column1),
        column2=parse(column2)
    )


def covar_samp(column1, column2):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.covariance_aggregations import CovarSamp
    return CovarSamp(
        column1=parse(column1),
        column2=parse(column2)
    )


def first(e, ignoreNulls=False):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.collectors import First
    return First(parse(e), ignoreNulls)


def grouping(e):
    """
    :rtype: Column
    """
    return Column(Grouping(parse(e)))


def grouping_id(*cols):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in e]
    return Column(GroupingID(cols))


def kurtosis(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import KurtosisSamp
    return col(KurtosisSamp(column=parse(e)))


def last(e, ignoreNulls=False):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.collectors import Last
    return Last(parse(e), ignoreNulls)


def max(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import Max
    return col(Max(column=parse(e)))


def mean(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import Avg
    # Discrepancy between name and object (mean vs Avg) replicate a discrepancy in PySpark
    return col(Avg(column=parse(e)))


def min(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import Min
    return col(Min(column=parse(e)))


def skewness(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import Skewness
    return col(Skewness(column=parse(e)))


def stddev(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import StddevSamp
    return col(StddevSamp(column=parse(e)))


def stddev_samp(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import StddevSamp
    return col(StddevSamp(column=parse(e)))


def stddev_pop(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import StddevPop
    return col(StddevPop(column=parse(e)))


def sum(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import Sum
    return col(Sum(column=parse(e)))


def sumDistinct(e):
    """
    :rtype: Column
    """
    return Collect(column=parse(e), mapper=lambda x: sum(set(x)))


def variance(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import VarSamp
    return col(VarSamp(column=parse(e)))


def var_samp(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import VarSamp
    return col(VarSamp(column=parse(e)))


def var_pop(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.stat_aggregations import VarPop
    return col(VarPop(column=parse(e)))

# //////////////////////////////////////////////////////////////////////////////////////////////
# // Window functions
# //////////////////////////////////////////////////////////////////////////////////////////////

# def cume_dist():
#     """
#     :rtype: Column
#     """
#     return
#
#
# def dense_rank():
#     """
#     :rtype: Column
#     """
#     return
#
#
# def lag(e, offset, defaultValue=None):
#     """
#     :rtype: Column
#     """
#     return
#
#
# def lead(e, offset, defaultValue=None):
#     """
#     :rtype: Column
#     """
#     return
#
#
# def ntile(n):
#     """
#     :rtype: Column
#     """
#     return
#
#
# def percent_rank():
#     """
#     :rtype: Column
#     """
#     return
#
#
# def rank():
#     """
#     :rtype: Column
#     """
#     return
#
#
# def row_number():
#     """
#     :rtype: Column
#     """
#     return


def array(*exprs):
    """
    :rtype: Column
    """
    columns = [parse(e) for e in exprs]
    return col(ArrayColumn(columns))


def create_map(*exprs):
    """Creates a new map column.

    :param exprs: list of column names (string) or list of :class:`Column` expressions that are
        grouped as key-value pairs, e.g. (key1, value1, key2, value2, ...).

    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([Row(age=2, name='Alice'), Row(age=5, name='Bob')])
    >>> df.select(create_map('name', 'age').alias("map")).collect()
    [Row(map={'Alice': 2}), Row(map={'Bob': 5})]
    >>> df.select(create_map([df.name, df.age]).alias("map")).collect()
    [Row(map={'Alice': 2}), Row(map={'Bob': 5})]
    """
    if len(exprs) == 1 and isinstance(exprs[0], (list, set)):
        exprs = exprs[0]
    cols = [parse(e) for e in exprs]

    return col(MapColumn(*cols))


def map_from_arrays(col1, col2):
    """Creates a new map from two arrays.

    :param col1: name of column containing a set of keys. All elements should not be null
    :param col2: name of column containing a set of values
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([([2, 5], ['a', 'b'])], ['k', 'v'])
    >>> df.select(map_from_arrays(df.k, df.v).alias("map")).show()
    +----------------+
    |             map|
    +----------------+
    |[2 -> a, 5 -> b]|
    +----------------+
    """
    key_col = parse(col1)
    value_col = parse(col2)
    return col(MapFromArraysColumn(key_col, value_col))


def broadcast(df):
    """
    :rtype: Column
    """
    # Broadcast is not implemented as Pysparkling is not distributed
    return df


def coalesce(*exprs):
    """
    :rtype: Column
    """
    columns = [parse(e) for e in exprs]
    return Coalesce(columns)


def input_file_name():
    """
    :rtype: Column
    """
    return InputFileName()


def isnan(e):
    """
    :rtype: Column
    """
    return IsNaN(parse(e))


def isnull(e):
    """
    :rtype: Column
    """
    return IsNull(parse(e))


def monotonically_increasing_id():
    """
    :rtype: Column
    """
    return MonotonicallyIncreasingID()


def nanvl(col1, col2):
    """
    :rtype: Column
    """
    return NaNvl(parse(col1), parse(col2))


def negate(e):
    """
    :rtype: Column
    """
    return -parse(e)


# todo: name should be not but the word is reserved. It is not exposed in Pyspark
# def not(e):
#     """
#     :rtype: Column
#     """
#     return


def rand(seed=None):
    """

    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.range(4, numPartitions=2)
    >>> df.select((rand(seed=42) * 3).alias("rand")).show()
    +------------------+
    |              rand|
    +------------------+
    |2.3675439190260485|
    |1.8992753422855404|
    |1.5878851952491426|
    |0.8800146499990725|
    +------------------+
    """
    return col(Rand(seed))


def randn(seed=None):
    """

    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.range(4, numPartitions=2)
    >>> df.select((randn(seed=42) * 3).alias("randn")).show()
    +------------------+
    |             randn|
    +------------------+
    | 3.662337823324239|
    |1.6855413955465237|
    |0.7870748709777542|
    |-5.552412872005739|
    +------------------+
    """
    return col(Randn(seed))


def spark_partition_id():
    """
    :rtype: Column
    """
    return col(SparkPartitionID())


def sqrt(e):
    """
    :rtype: Column
    """
    return col(Sqrt(parse(e)))


def struct(*exprs):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([Row(age=2, name='Alice'), Row(age=5, name='Bob')])
    >>> df.select(struct("age", col("name")).alias("struct")).collect()
    [Row(struct=Row(age=2, name='Alice')), Row(struct=Row(age=5, name='Bob'))]
    >>> df.select(struct("age", col("name"))).show()
    +----------------------------------+
    |named_struct(age, age, name, name)|
    +----------------------------------+
    |                        [2, Alice]|
    |                          [5, Bob]|
    +----------------------------------+

    """
    cols = [parse(e) for e in exprs]
    return col(CreateStruct(cols))


def when(condition, value):
    """
    :rtype: Column
    """
    CaseWhen(parse(condition), parse(value))


def bitwiseNOT(e):
    """
    :rtype: Column
    """
    BitwiseNot(parse(e))


def expr(expr):
    """
    :rtype: Column
    """
    return parse(expr)


def abs(e):
    """
    :rtype: Column
    """
    return Abs(parse(e))


def acos(e):
    """
    :rtype: Column
    """
    return Acos(parse(e))


def asin(e):
    """
    :rtype: Column
    """
    return Asin(parse(e))


def atan(e):
    """
    :rtype: Column
    """
    return Atan(parse(e))


def atan2(y, x):
    """
    :rtype: Column
    """
    return Atan2(parse(x), parse(y))


def bin(e):
    """
    :rtype: Column
    """
    return Bin(parse(e))


def cbrt(e):
    """
    :rtype: Column
    """
    return Cbrt(parse(e))


def ceil(e):
    """
    :rtype: Column
    """
    return Ceil(parse(e))


def conv(num, fromBase, toBase):
    """
    :rtype: Column
    """
    return Conv(parse(num), fromBase, toBase)


def cos(e):
    """
    :rtype: Column
    """
    return Cos(parse(e))


def cosh(e):
    """
    :rtype: Column
    """
    return Cosh(parse(e))


def exp(e):
    """
    :rtype: Column
    """
    return Exp(parse(e))


def expm1(e):
    """
    :rtype: Column
    """
    return ExpM1(parse(e))


def factorial(e):
    """
    :rtype: Column
    """
    return Factorial(parse(e))


def floor(e):
    """
    :rtype: Column
    """
    return Floor(parse(e))


def greatest(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return Greatest(cols)


def hex(column):
    """
    :rtype: Column
    """
    return Hex(parse(e))


def unhex(column):
    """
    :rtype: Column
    """
    return Unhex(parse(e))


def hypot(l, r):
    """
    :rtype: Column
    """
    return Hypot(parse(l), parse(r))


def least(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return Least(cols)


def log(arg1, arg2=None):
    """
    :rtype: Column
    """
    if arg2 is None:
        base, value = math.e, parse(arg1)
    else:
        base, value = arg1, parse(arg2)
    return Log(base, value)


def log10(e):
    """
    :rtype: Column
    """
    return Log10(parse(e))


def log1p(e):
    """
    :rtype: Column
    """
    return Log1p(parse(e))


def log2(e):
    """
    :rtype: Column
    """
    return Log2(parse(e))


def pow(l, r):
    """
    :rtype: Column
    """
    return Pow(parse(l), parse(r))


def pmod(dividend, divisor):
    """
    :rtype: Column
    """
    return Pmod(dividend, divisor)


def rint(e):
    """
    :rtype: Column
    """
    return Rint(parse(e))


def round(e, scale=0):
    """
    :rtype: Column
    """
    return Round(parse(e), scale)


def bround(e, scale=0):
    """
    :rtype: Column
    """
    return Bround(parse(e), scale)


def shiftLeft(e, numBits):
    """
    :rtype: Column
    """
    return ShiftLeft(parse(e), numBits)


def shiftRight(e, numBits):
    """
    :rtype: Column
    """
    return ShiftRight(parse(e), numBits)


def shiftRightUnsigned(e, numBits):
    """
    :rtype: Column
    """
    return ShiftRightUnsigned(parse(e), numBits)


def signum(e):
    """
    :rtype: Column
    """
    return Signum(parse(e))


def sin(e):
    """
    :rtype: Column
    """
    return Sin(parse(e))


def sinh(e):
    """
    :rtype: Column
    """
    return Sinh(parse(e))


def tan(e):
    """
    :rtype: Column
    """
    return Tan(parse(e))


def tanh(e):
    """
    :rtype: Column
    """
    return Tanh(parse(e))


def degrees(e):
    """
    :rtype: Column
    """
    return ToDegrees(parse(e))


def radians(e):
    """
    :rtype: Column
    """
    return ToRadians(parse(e))


def md5(e):
    """
    :rtype: Column
    """
    return Md5(parse(e))


def sha1(e):
    """
    :rtype: Column
    """
    return Sha1(parse(e))


def sha2(e, numBits):
    """
    :rtype: Column
    """
    if numBits not in (0, 224, 256, 384, 512):
        raise Exception(
            "numBits {0} is not in the permitted values (0, 224, 256, 384, 512)".format(numBits)
        )
    return Sha2(parse(e))


def crc32(e):
    """
    :rtype: Column
    """
    return Crc32(parse(e))


def hash(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return Hash(cols)


def xxhash64(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return XxHash64(cols)


def ascii(e):
    """
    :rtype: Column
    """
    return Ascii(parse(e))


def base64(e):
    """
    :rtype: Column
    """
    return Base64(parse(e))


def concat_ws(sep, *exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return ConcatWs(sep, cols)


def decode(value, charset):
    """
    :rtype: Column
    """
    return Decode(parse(value), charset)


def encode(value, charset):
    """
    :rtype: Column
    """
    return Encode(parse(value), charset)


def format_number(x, d):
    """
    :rtype: Column
    """
    return FormatNumber(parse(x), d)


def format_string(format, *exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return FormatString(format, cols)


def initcap(e):
    """
    :rtype: Column
    """
    return InitCap(parse(e))


def instr(str, substring):
    """
    :rtype: Column
    """
    return StringInStr(parse(str), substring)


def length(e):
    """
    :rtype: Column
    """
    return Length(parse(e))


def lower(e):
    """
    :rtype: Column
    """
    return Lower(parse(e))


def levenshtein(l, r):
    """
    :rtype: Column
    """
    return Levenshtein(l, r)


def locate(substr, str, pos=1):
    """
    :rtype: Column
    """
    return StringLocate(substr, str, pos)


def lpad(str, len, pad):
    """
    :rtype: Column
    """
    return StringLPad(str, len, pad)


def ltrim(e, trimString=" "):
    """
    :rtype: Column
    """
    return StringLTrim(e, trimString)


def regexp_extract(e, exp, groupIdx):
    """
    :rtype: Column
    """
    return RegExpExtract(e, exp, groupIdx)


def regexp_replace(e, pattern, replacement):
    """
    :rtype: Column
    """
    return RegExpReplace(e, parse(pattern), replacement)


def unbase64(e):
    """
    :rtype: Column
    """
    return UnBase64(parse(e))


def rpad(str, len, pad):
    """
    :rtype: Column
    """
    return StringRPad(str, len, pad)


def repeat(str, n):
    """
    :rtype: Column
    """
    return StringRepeat(str, n)


def rtrim(e, trimString=" "):
    """
    :rtype: Column
    """
    return StringRTrim(e, trimString)


def soundex(e):
    """
    :rtype: Column
    """
    return SoundEx(e)


def split(str, regex, limit=None):
    """
    :rtype: Column
    """
    return StringSplit(str, regex, limit)


def substring(str, pos, len):
    """
    :rtype: Column
    """
    return Substring(str, pos, len)


def substring_index(str, delim, count):
    """
    :rtype: Column
    """
    return SubstringIndex(str, delim, count)


def translate(src, matchingString, replaceString):
    """
    :rtype: Column
    """
    return StringTranslate(str, matchingString, replaceString)


def trim(e, trimString=" "):
    """
    :rtype: Column
    """
    return StringTrim(e, trimString)


def upper(e):
    """
    :rtype: Column
    """
    return Upper(e)


def add_months(startDate, numMonths):
    """
    :rtype: Column
    """
    return AddMonths(startDate, numMonths)


def current_date():
    """
    :rtype: Column
    """
    return CurrentDate()


def current_timestamp():
    """
    :rtype: Column
    """
    return CurrentTimestamp()


def date_format(dateExpr, format):
    """
    :rtype: Column
    """
    return DateFormatClass(dateExpr, format)


def date_add(start, days):
    """
    :rtype: Column
    """
    return DateAdd(start, days)


def date_sub(start, days):
    """
    :rtype: Column
    """
    return DateSub(start, days)


def datediff(end, start):
    """
    :rtype: Column
    """
    return DateDiff(start, days)


def year(e):
    """
    :rtype: Column
    """
    return Year(e)


def quarter(e):
    """
    :rtype: Column
    """
    return Quarter(e)


def month(e):
    """
    :rtype: Column
    """
    return Month(e)


def dayofweek(e):
    """
    :rtype: Column
    """
    return DayOfWeek(e)


def dayofmonth(e):
    """
    :rtype: Column
    """
    return DayOfMonth(e)


def dayofyear(e):
    """
    :rtype: Column
    """
    return DayOfYear(e)


def hour(e):
    """
    :rtype: Column
    """
    return Hour(e)


def last_day(e):
    """
    :rtype: Column
    """
    return LastDay(e)


def minute(e):
    """
    :rtype: Column
    """
    return Minute(e)


def months_between(end, start, roundOff=True):
    """
    :rtype: Column
    """
    return MonthsBetween(end, start, roundOff)


def next_day(date, dayOfWeek):
    """
    :rtype: Column
    """
    return NextDay(date, dayOfWeek)


def second(e):
    """
    :rtype: Column
    """
    return Second(e)


def weekofyear(e):
    """
    :rtype: Column
    """
    return WeekOfYear(e)


def from_unixtime(ut, f="yyyy-MM-dd HH:mm:ss"):
    """
    :rtype: Column
    """
    return FromUnixTime(ut, f)


def unix_timestamp(s=None, p="yyyy-MM-dd HH:mm:ss"):
    """
    :rtype: Column
    """
    if s is None:
        s = CurrentTimestamp()
    return UnixTimestamp(s, p)


def to_timestamp(s, fmt=None):
    """
    :rtype: Column
    """
    return ParseToTimestamp(s, fmt)


def to_date(e, fmt=None):
    """
    :rtype: Column
    """
    return ParseToDate(e, fmt)


def trunc(date, format):
    """
    :rtype: Column
    """
    return TruncDate(date, format)


def date_trunc(format, timestamp):
    """
    :rtype: Column
    """
    return TruncTimestamp(format, timestamp)


def from_utc_timestamp(ts, tz):
    """
    :rtype: Column
    """
    return FromUTCTimestamp(ts, parse(tz))


def to_utc_timestamp(ts, tz):
    """
    :rtype: Column
    """
    return ToUTCTimestamp(ts, parse(tz))


# def window(timeColumn, windowDuration, slideDuration=None, startTime="0 second"):
#     """
#     :rtype: Column
#     """
#     if slideDuration:
#         slideDuration = windowDuration
#     return


def array_contains(column, value):
    """
    :rtype: Column
    """
    return ArrayContains(column, value)


def arrays_overlap(a1, a2):
    """
    :rtype: Column
    """
    return ArraysOverlap(a1, a2)


def slice(x, start, length):
    """
    :rtype: Column
    """
    return Slice(x, start, length)


def array_join(column, delimiter, nullReplacement=None):
    """
    :rtype: Column
    """
    return ArrayJoin(column, delimiter, nullReplacement)


def concat(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return Concat(cols)


def array_position(column, value):
    """
    :rtype: Column
    """
    return ArrayPosition(column, value)


def element_at(column, value):
    """
    :rtype: Column
    """
    return ElementAt(column, value)


def array_sort(e):
    """
    :rtype: Column
    """
    return ArraySort(e)


def array_remove(column, element):
    """
    :rtype: Column
    """
    return ArrayRemove(column, element)


def array_distinct(e):
    """
    :rtype: Column
    """
    return ArrayDistinct(e)


def array_intersect(col1, col2):
    """
    :rtype: Column
    """
    return ArrayIntersect(col1, col2)


def array_union(col1, col2):
    """
    :rtype: Column
    """
    return ArrayUnion(col1, col2)


def array_except(col1, col2):
    """
    :rtype: Column
    """
    return ArrayExcept(col1, col2)


def explode(e):
    """
    :rtype: Column
    """
    return Explode(e)


def explode_outer(e):
    """
    :rtype: Column
    """
    return ExplodeOuter(e)


def posexplode(e):
    """
    :rtype: Column
    """
    return PosExplode(e)


def posexplode_outer(e):
    """
    :rtype: Column
    """
    return PosExplodeOuter(e)


def get_json_object(e, path):
    """
    :rtype: Column
    """
    return GetJsonObject(e, path)


def json_tuple(json, *fields):
    """
    :rtype: Column
    """
    return JsonTuple(json, fields)


def from_json(e, schema, options=None):
    """
    :rtype: Column
    """
    return JsonToStructs(schema, options, e)


def schema_of_json(json, options=None):
    """
    :rtype: Column
    """
    return SchemaOfJson(json)


def to_json(e, options=None):
    """
    :rtype: Column
    """
    return StructsToJson(e, options)


def size(e):
    """
    :rtype: Column
    """
    return Size(e)


def sort_array(e, asc=True):
    """
    :rtype: Column
    """
    return SortArray(e, asc)


def array_min(e):
    """
    :rtype: Column
    """
    return ArrayMin(e)


def array_max(e):
    """
    :rtype: Column
    """
    return ArrayMax(e)


def shuffle(e):
    """
    :rtype: Column
    """
    return Shuffle(e)


def reverse(e):
    """
    :rtype: Column
    """
    return Reverse(e)


def flatten(e):
    """
    :rtype: Column
    """
    return Flatten(e)


def sequence(start, stop, step=None):
    """
    :rtype: Column
    """
    return Sequence(start, stop, step)


def array_repeat(e, count):
    """
    :rtype: Column
    """
    return ArrayRepeat(e, count)


def map_keys(e):
    """
    :rtype: Column
    """
    return MapKeys(e)


def map_values(e):
    """
    :rtype: Column
    """
    return MapValues(e)


def map_entries(e):
    """
    :rtype: Column
    """
    return MapEntries(e)


def map_from_entries(e):
    """
    :rtype: Column
    """
    return MapFromEntries(e)


def arrays_zip(*exprs):
    """
    :rtype: Column
    """
    return ArraysZip(exprs)


def map_concat(*cols):
    """
    :rtype: Column
    """
    return MapConcat(cols)


def from_csv(e, schema, options=None):
    """
    :rtype: Column
    """
    return CsvToStructs(schema, options, e)


def schema_of_csv(csv, options=None):
    """
    :rtype: Column
    """
    return SchemaOfCsv(csv.expr)


def to_csv(e, options=None):
    """
    :rtype: Column
    """
    return StructsToCsv(options.asScala.toMap, e.expr)


def udf(f, returnType=StringType()):
    """
    :rtype: Column
    """
    return UserDefinedFunction(f, returnType)


def callUDF(udfName, *cols):
    """
    :rtype: Column
    """
    return UnresolvedFunction(udfName, cols)
