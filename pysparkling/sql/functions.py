from pysparkling.sql.expressions.dates import *
from pysparkling.sql.expressions.strings import *
from pysparkling.sql.types import StringType

from pysparkling.sql.column import parse, Column
from pysparkling.sql.expressions.aggregate.collectors import SumDistinct
from pysparkling.sql.expressions.explodes import *
from pysparkling.sql.expressions.arrays import *
from pysparkling.sql.expressions.mappers import *
from pysparkling.sql.expressions.literals import Literal
from pysparkling.sql.expressions.aggregate.HyperLogLogPlusPlus import HyperLogLogPlusPlus
from pysparkling.sql.expressions.aggregate.stat_aggregations import *
from pysparkling.sql.expressions.aggregate.covariance_aggregations import *


def col(colName):
    """
    :rtype: Column
    """
    return Column(colName)


def column(colName):
    """
    :rtype: Column
    """
    return col(colName)


def lit(literal):
    """
    :rtype: Column
    """
    return col(typedLit(literal))


def typedLit(literal):
    """
    :rtype: Column
    """
    return Literal(literal)


def asc(columnName):
    """
    :rtype: Column
    """
    return col(Column(columnName).asc)


def asc_nulls_first(columnName):
    """
    :rtype: Column
    """
    return col(Column(columnName).asc_nulls_first)


def asc_nulls_last(columnName):
    """
    :rtype: Column
    """
    return col(Column(columnName).asc_nulls_last)


def desc(columnName):
    """
    :rtype: Column
    """
    return col(Column(columnName).desc)


def desc_nulls_first(columnName):
    """
    :rtype: Column
    """
    return col(Column(columnName).desc_nulls_first)


def desc_nulls_last(columnName):
    """
    :rtype: Column
    """
    return col(Column(columnName).desc_nulls_last)


def approx_count_distinct(e, rsd=0.05):
    """
    :rtype: Column
    """
    return col(HyperLogLogPlusPlus(parse(e), relativeSD=rsd))


def avg(e):
    """
    :rtype: Column
    """
    return col(Avg(column=parse(e)))


def collect_list(e):
    """
    :rtype: Column
    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5)
    >>> df.repartition(2).select(collect_list("id").alias("all_ids")).show()
    +---------------+
    |        all_ids|
    +---------------+
    |[0, 1, 2, 3, 4]|
    +---------------+
    """
    from pysparkling.sql.expressions.aggregate.collectors import CollectList
    return col(CollectList(column=parse(e)))


def collect_set(e):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.collectors import CollectSet
    return col(CollectSet(column=parse(e)))


def corr(column1, column2):
    """
    :rtype: Column
    """
    return col(Corr(
        column1=parse(column1),
        column2=parse(column2)
    ))


def count(e):
    """
    :rtype: Column
    """
    return col(Count(column=e))


def countDistinct(*exprs):
    """
    :rtype: Column
    """
    columns = [parse(e) for e in exprs]
    from pysparkling.sql.expressions.aggregate.collectors import CountDistinct
    return col(CountDistinct(columns=columns))


def covar_pop(column1, column2):
    """
    :rtype: Column
    """
    return col(CovarPop(
        column1=parse(column1),
        column2=parse(column2)
    ))


def covar_samp(column1, column2):
    """
    :rtype: Column
    """
    return col(CovarSamp(
        column1=parse(column1),
        column2=parse(column2)
    ))


def first(e, ignoreNulls=False):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.collectors import First
    return col(First(parse(e), ignoreNulls))


def grouping(e):
    """
    :rtype: Column
    """
    return col(Column(Grouping(parse(e))))


def grouping_id(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return col(Column(GroupingID(cols)))


def kurtosis(e):
    """
    :rtype: Column
    """
    return col(Kurtosis(column=parse(e)))


def last(e, ignoreNulls=False):
    """
    :rtype: Column
    """
    from pysparkling.sql.expressions.aggregate.collectors import Last
    return col(Last(parse(e), ignoreNulls))


# noinspection PyShadowingBuiltins
def max(e):
    """
    :rtype: Column
    """
    return col(Max(column=parse(e)))


def mean(e):
    """
    :rtype: Column
    """
    # Discrepancy between name and object (mean vs Avg) replicate a discrepancy in PySpark
    return col(Avg(column=parse(e)))


# noinspection PyShadowingBuiltins
def min(e):
    """
    :rtype: Column
    """
    return col(Min(column=parse(e)))


def skewness(e):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.range(100, numPartitions=20).select((col("id")**2).alias("n"))
    >>> df.groupBy().agg(skewness("n")).show()
    +------------------+
    |       skewness(n)|
    +------------------+
    |0.6440904335963368|
    +------------------+

    """
    return col(Skewness(column=parse(e)))


def stddev(e):
    """
    :rtype: Column
    """
    return col(StddevSamp(column=parse(e)))


def stddev_samp(e):
    """
    :rtype: Column
    """
    return col(StddevSamp(column=parse(e)))


def stddev_pop(e):
    """
    :rtype: Column
    """
    return col(StddevPop(column=parse(e)))


def sum(e):
    """
    :rtype: Column
    """
    return col(Sum(column=parse(e)))


def sumDistinct(e):
    """
    :rtype: Column
    """
    return col(SumDistinct(column=parse(e)))


def variance(e):
    """
    :rtype: Column
    """
    return col(VarSamp(column=parse(e)))


def var_samp(e):
    """
    :rtype: Column
    """
    return col(VarSamp(column=parse(e)))


def var_pop(e):
    """
    :rtype: Column
    """
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
    return col(Coalesce(columns))


def input_file_name():
    """
    :rtype: Column
    """
    return col(InputFileName())


def isnan(e):
    """
    :rtype: Column
    """
    return col(IsNaN(parse(e)))


def isnull(e):
    """
    :rtype: Column
    """
    return col(IsNull(parse(e)))


def monotonically_increasing_id():
    """
    :rtype: Column
    """
    return col(MonotonicallyIncreasingID())


def nanvl(col1, col2):
    """
    :rtype: Column
    """
    return col(NaNvl(parse(col1), parse(col2)))


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
    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame(
    ...    [Row(age=2, name='Alice'), Row(age=5, name='Bob'), Row(age=4, name='Lisa')]
    ... )
    >>> df.select(df.name, when(df.age > 4, -1).when(df.age < 3, 1).otherwise(0)).show()
    +-----+------------------------------------------------------------+
    | name|CASE WHEN (age > 4) THEN -1 WHEN (age < 3) THEN 1 ELSE 0 END|
    +-----+------------------------------------------------------------+
    |Alice|                                                           1|
    |  Bob|                                                          -1|
    | Lisa|                                                           0|
    +-----+------------------------------------------------------------+

    :rtype: Column
    """
    return CaseWhen(parse(condition), parse(value))


def bitwiseNOT(e):
    """
    :rtype: Column
    """
    BitwiseNot(parse(e))


# todo: replace parse with expr
def expr(expression):
    """
    :rtype: Column
    """
    return parse(expression)


# noinspection PyShadowingBuiltins
def abs(e):
    """
    :rtype: Column
    """
    return col(Abs(parse(e)))


def acos(e):
    """
    :rtype: Column
    """
    return col(Acos(parse(e)))


def asin(e):
    """
    :rtype: Column
    """
    return col(Asin(parse(e)))


def atan(e):
    """
    :rtype: Column
    """
    return col(Atan(parse(e)))


def atan2(y, x):
    """
    :rtype: Column
    """
    return col(Atan2(parse(y), parse(x)))


# noinspection PyShadowingBuiltins
def bin(e):
    """
    :rtype: Column
    """
    return col(Bin(parse(e)))


def cbrt(e):
    """
    :rtype: Column
    """
    return col(Cbrt(parse(e)))


def ceil(e):
    """
    :rtype: Column
    """
    return col(Ceil(parse(e)))


def conv(num, fromBase, toBase):
    """
    :rtype: Column
    """
    return col(Conv(parse(num), fromBase, toBase))


def cos(e):
    """
    :rtype: Column
    """
    return col(Cos(parse(e)))


def cosh(e):
    """
    :rtype: Column
    """
    return col(Cosh(parse(e)))


def exp(e):
    """
    :rtype: Column
    """
    return col(Exp(parse(e)))


def expm1(e):
    """
    :rtype: Column
    """
    return col(ExpM1(parse(e)))


def factorial(e):
    """
    :rtype: Column
    """
    return col(Factorial(parse(e)))


def floor(e):
    """
    :rtype: Column
    """
    return col(Floor(parse(e)))


def greatest(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return col(Greatest(cols))


def hex(column):
    """
    :rtype: Column
    """
    return col(Hex(parse(column)))


def unhex(column):
    """
    :rtype: Column
    """
    return col(Unhex(parse(column)))


def hypot(l, r):
    """
    :rtype: Column
    """
    return col(Hypot(parse(l), parse(r)))


def least(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return col(Least(cols))


def log(arg1, arg2=None):
    """
    :rtype: Column
    """
    if arg2 is None:
        base, value = math.e, parse(arg1)
    else:
        base, value = arg1, parse(arg2)
    return col(Log(base, value))


def log10(e):
    """
    :rtype: Column
    """
    return col(Log10(parse(e)))


def log1p(e):
    """
    :rtype: Column
    """
    return col(Log1p(parse(e)))


def log2(e):
    """
    :rtype: Column
    """
    return col(Log2(parse(e)))


def pow(l, r):
    """
    :rtype: Column
    """
    return col(Pow(parse(l), parse(r)))


def pmod(dividend, divisor):
    """
    :rtype: Column
    """
    return col(Pmod(dividend, divisor))


def rint(e):
    """
    :rtype: Column
    """
    return col(Rint(parse(e)))


def round(e, scale=0):
    """
    :rtype: Column
    """
    return col(Round(parse(e), scale))


def bround(e, scale=0):
    """
    :rtype: Column
    """
    return col(Bround(parse(e), scale))


def shiftLeft(e, numBits):
    """
    :rtype: Column
    """
    return col(ShiftLeft(parse(e), numBits))


def shiftRight(e, numBits):
    """
    :rtype: Column
    """
    return col(ShiftRight(parse(e), numBits))


def shiftRightUnsigned(e, numBits):
    """
    :rtype: Column
    """
    return col(ShiftRightUnsigned(parse(e), numBits))


def signum(e):
    """
    :rtype: Column
    """
    return col(Signum(parse(e)))


def sin(e):
    """
    :rtype: Column
    """
    return col(Sin(parse(e)))


def sinh(e):
    """
    :rtype: Column
    """
    return col(Sinh(parse(e)))


def tan(e):
    """
    :rtype: Column
    """
    return col(Tan(parse(e)))


def tanh(e):
    """
    :rtype: Column
    """
    return col(Tanh(parse(e)))


def degrees(e):
    """
    :rtype: Column
    """
    return col(ToDegrees(parse(e)))


def radians(e):
    """
    :rtype: Column
    """
    return col(ToRadians(parse(e)))


def md5(e):
    """
    :rtype: Column
    """
    return col(Md5(parse(e)))


def sha1(e):
    """
    :rtype: Column
    """
    return col(Sha1(parse(e)))


def sha2(e, numBits):
    """
    :rtype: Column
    """
    if numBits not in (0, 224, 256, 384, 512):
        raise Exception(
            "numBits {0} is not in the permitted values (0, 224, 256, 384, 512)".format(numBits)
        )
    return col(Sha2(parse(e)))


def crc32(e):
    """
    :rtype: Column
    """
    return col(Crc32(parse(e)))


def hash(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return col(Hash(cols))


def xxhash64(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return col(XxHash64(cols))


def ascii(e):
    """
    :rtype: Column
    """
    return col(Ascii(parse(e)))


def base64(e):
    """
    :rtype: Column
    """
    return col(Base64(parse(e)))


def concat_ws(sep, *exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return col(ConcatWs(sep, cols))


def decode(value, charset):
    """
    :rtype: Column
    """
    return col(Decode(parse(value), charset))


def encode(value, charset):
    """
    :rtype: Column
    """
    return col(Encode(parse(value), charset))


def format_number(x, d):
    """
    :rtype: Column
    """
    return col(FormatNumber(parse(x), d))


def format_string(format, *exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return col(FormatString(format, cols))


def initcap(e):
    """
    :rtype: Column
    """
    return col(InitCap(parse(e)))


def instr(str, substring):
    """
    :rtype: Column
    """
    return col(StringInStr(parse(str), substring))


def length(e):
    """
    :rtype: Column
    """
    return col(Length(parse(e)))


def lower(e):
    """
    :rtype: Column
    """
    return col(Lower(parse(e)))


def levenshtein(l, r):
    """
    :rtype: Column
    """
    return col(Levenshtein(l, r))


def locate(substr, str, pos=1):
    """
    :rtype: Column
    """
    return col(StringLocate(substr, parse(str), pos))


def lpad(str, len, pad):
    """
    :rtype: Column
    """
    return col(StringLPad(parse(str), len, pad))


def ltrim(e):
    """
    :rtype: Column
    """
    return col(StringLTrim(e))


def regexp_extract(e, exp, groupIdx):
    """
    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([('100-200',)], ['str'])
    >>> df.collect()
    [Row(str='100-200')]
    >>> df.select(Column('str').alias('range')).collect()
    [Row(range='100-200')]
    >>> df.select(regexp_extract(df.str, r'(\d+)-(\d+)', 1).alias('d')).collect()
    [Row(d='100')]

    :rtype: Column
    """
    return col(RegExpExtract(e, exp, groupIdx))


def regexp_replace(e, pattern, replacement):
    """
    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([('100-200',)], ['str'])
    >>> df.collect()
    [Row(str='100-200')]
    >>> df.select(Column('str').alias('range')).collect()
    [Row(range='100-200')]
    >>> df.select(regexp_replace(df.str, r'-(\d+)', '-300').alias('d')).collect()
    [Row(d='100-300')]

    :rtype: Column
    """
    return col(RegExpReplace(e, pattern, replacement))


def unbase64(e):
    """
    :rtype: Column
    """
    return col(UnBase64(parse(e)))


def rpad(str, len, pad):
    """
    :rtype: Column
    """
    return col(StringRPad(parse(str), len, pad))


def repeat(str, n):
    """
    :rtype: Column
    """
    return col(StringRepeat(parse(str), n))


def rtrim(e):
    """
    :rtype: Column
    """
    return col(StringRTrim(e))


def soundex(e):
    """
    :rtype: Column
    """
    return col(SoundEx(e))


def split(str, regex, limit=None):
    """
    :rtype: Column
    """
    return col(StringSplit(parse(str), regex, limit))


def substring(str, pos, len):
    """
    :rtype: Column
    """
    return col(Substring(str, pos, len))


def substring_index(str, delim, count):
    """
    :rtype: Column
    """
    return col(SubstringIndex(str, delim, count))


def translate(srcCol, matchingString, replaceString):
    """
    :rtype: Column
    """
    return col(StringTranslate(parse(srcCol), matchingString, replaceString))


def trim(e):
    """
    :rtype: Column
    """
    return col(StringTrim(parse(e)))


def upper(e):
    """
    :rtype: Column
    """
    return col(Upper(e))


def add_months(startDate, numMonths):
    """
    :rtype: Column
    """
    return col(AddMonths(parse(startDate), numMonths))


def current_date():
    """
    :rtype: Column
    """
    return col(CurrentDate())


def current_timestamp():
    """
    :rtype: Column
    """
    return col(CurrentTimestamp())


def date_format(dateExpr, format):
    """
    :rtype: Column
    """
    return col(DateFormatClass(dateExpr, format))


def date_add(start, days):
    """
    :rtype: Column
    """
    return col(DateAdd(start, days))


def date_sub(start, days):
    """
    :rtype: Column
    """
    return col(DateSub(start, days))


def datediff(end, start):
    """
    :rtype: Column
    """
    return col(DateDiff(start, days))


def year(e):
    """
    :rtype: Column
    """
    return col(Year(e))


def quarter(e):
    """
    :rtype: Column
    """
    return col(Quarter(e))


def month(e):
    """
    :rtype: Column
    """
    return col(Month(e))


def dayofweek(e):
    """
    :rtype: Column
    """
    return col(DayOfWeek(e))


def dayofmonth(e):
    """
    :rtype: Column
    """
    return col(DayOfMonth(e))


def dayofyear(e):
    """
    :rtype: Column
    """
    return col(DayOfYear(e))


def hour(e):
    """
    :rtype: Column
    """
    return col(Hour(e))


def last_day(e):
    """
    :rtype: Column
    """
    return col(LastDay(e))


def minute(e):
    """
    :rtype: Column
    """
    return col(Minute(e))


def months_between(end, start, roundOff=True):
    """
    :rtype: Column
    """
    return col(MonthsBetween(end, start, roundOff))


def next_day(date, dayOfWeek):
    """
    :rtype: Column
    """
    return col(NextDay(date, dayOfWeek))


def second(e):
    """
    :rtype: Column
    """
    return col(Second(e))


def weekofyear(e):
    """
    :rtype: Column
    """
    return col(WeekOfYear(e))


def from_unixtime(ut, f="yyyy-MM-dd HH:mm:ss"):
    """
    :rtype: Column
    """
    return col(FromUnixTime(ut, f))


def unix_timestamp(s=None, p="yyyy-MM-dd HH:mm:ss"):
    """
    :rtype: Column
    """
    if s is None:
        s = CurrentTimestamp()
    return col(UnixTimestamp(s, p))


def to_timestamp(s, fmt=None):
    """
    :rtype: Column
    """
    return col(ParseToTimestamp(s, fmt))


def to_date(e, fmt=None):
    """
    :rtype: Column
    """
    return col(ParseToDate(e, fmt))


def trunc(date, format):
    """
    :rtype: Column
    """
    return col(TruncDate(date, format))


def date_trunc(format, timestamp):
    """
    :rtype: Column
    """
    return col(TruncTimestamp(format, timestamp))


def from_utc_timestamp(ts, tz):
    """
    :rtype: Column
    """
    return col(FromUTCTimestamp(ts, parse(tz)))


def to_utc_timestamp(ts, tz):
    """
    :rtype: Column
    """
    return col(ToUTCTimestamp(ts, parse(tz)))


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
    return col(ArrayContains(parse(column), value))


def arrays_overlap(a1, a2):
    """
    :rtype: Column
    """
    return col(ArraysOverlap(parse(a1), parse(a2)))


def slice(x, start, length):
    """
    :rtype: Column
    """
    return col(Slice(x, start, length))


def array_join(column, delimiter, nullReplacement=None):
    """
    :rtype: Column
    """
    return col(ArrayJoin(column, delimiter, nullReplacement))


def concat(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return col(Concat(cols))


def array_position(column, value):
    """
    :rtype: Column
    """
    return col(ArrayPosition(column, value))


def element_at(column, value):
    """
    :rtype: Column
    """
    return col(ElementAt(column, value))


def array_sort(e):
    """
    :rtype: Column
    """
    return col(ArraySort(e))


def array_remove(column, element):
    """
    :rtype: Column
    """
    return col(ArrayRemove(column, element))


def array_distinct(e):
    """
    :rtype: Column
    """
    return col(ArrayDistinct(e))


def array_intersect(col1, col2):
    """
    :rtype: Column
    """
    return col(ArrayIntersect(col1, col2))


def array_union(col1, col2):
    """
    :rtype: Column
    """
    return col(ArrayUnion(col1, col2))


def array_except(col1, col2):
    """
    :rtype: Column
    """
    return col(ArrayExcept(col1, col2))


def explode(e):
    """
    :rtype: Column
    """
    return col(Explode(parse(e)))


def explode_outer(e):
    """
    :rtype: Column
    """
    return col(ExplodeOuter(e))


def posexplode(e):
    """
    :rtype: Column
    """
    return col(PosExplode(e))


def posexplode_outer(e):
    """
    :rtype: Column
    """
    return col(PosExplodeOuter(e))


def get_json_object(e, path):
    """
    :rtype: Column
    """
    return col(GetJsonObject(e, path))


def json_tuple(json, *fields):
    """
    :rtype: Column
    """
    return col(JsonTuple(json, fields))


def from_json(e, schema, options=None):
    """
    :rtype: Column
    """
    return col(JsonToStructs(schema, options, e))


def schema_of_json(json, options=None):
    """
    :rtype: Column
    """
    return col(SchemaOfJson(json))


def to_json(e, options=None):
    """
    :rtype: Column
    """
    return col(StructsToJson(e, options))


def size(e):
    """
    :rtype: Column
    """
    return col(Size(e))


def sort_array(e, asc=True):
    """
    :rtype: Column
    """
    return col(SortArray(parse(e), asc))


def array_min(e):
    """
    :rtype: Column
    """
    return col(ArrayMin(e))


def array_max(e):
    """
    :rtype: Column
    """
    return col(ArrayMax(e))


def shuffle(e):
    """
    :rtype: Column
    """
    return col(Shuffle(e))


def reverse(e):
    """
    :rtype: Column
    """
    return col(Reverse(e))


def flatten(e):
    """
    :rtype: Column
    """
    return col(Flatten(parse(e)))


def sequence(start, stop, step=None):
    """
    :rtype: Column
    """
    return col(Sequence(
        parse(start),
        parse(stop),
        parse(step) if step is not None else None
    ))


def array_repeat(e, count):
    """
    :rtype: Column
    """
    return col(ArrayRepeat(parse(e), count))


def map_keys(e):
    """
    :rtype: Column
    """
    return col(MapKeys(e))


def map_values(e):
    """
    :rtype: Column
    """
    return col(MapValues(e))


def map_entries(e):
    """
    :rtype: Column
    """
    return col(MapEntries(e))


def map_from_entries(e):
    """
    :rtype: Column
    """
    return col(MapFromEntries(e))


def arrays_zip(*exprs):
    """
    :rtype: Column
    """
    return col(ArraysZip([parse(e) for e in exprs]))


def map_concat(*cols):
    """
    :rtype: Column
    """
    return col(MapConcat(cols))


def from_csv(e, schema, options=None):
    """
    :rtype: Column
    """
    return col(CsvToStructs(schema, options, e))


def schema_of_csv(csv, options=None):
    """
    :rtype: Column
    """
    return col(SchemaOfCsv(csv.expr))


def to_csv(e, options=None):
    """
    :rtype: Column
    """
    return col(StructsToCsv(options.asScala.toMap, e.expr))


def udf(f, returnType=StringType()):
    """
    :rtype: Column
    """
    return col(UserDefinedFunction(f, returnType))


def callUDF(udfName, *cols):
    """
    :rtype: Column
    """
    return col(UnresolvedFunction(udfName, cols))
