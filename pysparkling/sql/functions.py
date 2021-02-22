import math

from .column import Column, ensure_column, parse
from .expressions.aggregate.collectors import (
    ApproxCountDistinct, CollectList, CollectSet, CountDistinct, First, Last, SumDistinct
)
from .expressions.aggregate.covariance_aggregations import Corr, CovarPop, CovarSamp
from .expressions.aggregate.stat_aggregations import (
    Avg, Count, Kurtosis, Max, Min, Skewness, StddevPop, StddevSamp, Sum, VarPop, VarSamp
)
from .expressions.arrays import (
    ArrayColumn, ArrayContains, ArrayDistinct, ArrayExcept, ArrayIntersect, ArrayJoin, ArrayMax, ArrayMin,
    ArrayPosition, ArrayRemove, ArrayRepeat, ArraySort, ArraysOverlap, ArraysZip, ArrayUnion, ElementAt, Flatten,
    MapColumn, MapFromArraysColumn, Sequence, Size, Slice, SortArray
)
from .expressions.csvs import SchemaOfCsv
from .expressions.dates import (
    AddMonths, CurrentDate, CurrentTimestamp, DateAdd, DateDiff, DateFormat, DateSub, DayOfMonth, DayOfWeek, DayOfYear,
    FromUnixTime, FromUTCTimestamp, Hour, LastDay, Minute, Month, MonthsBetween, NextDay, ParseToDate,
    ParseToTimestamp, Quarter, Second, ToUTCTimestamp, TruncDate, TruncTimestamp, UnixTimestamp, WeekOfYear, Year
)
from .expressions.explodes import Explode, ExplodeOuter, PosExplode, PosExplodeOuter
from .expressions.jsons import StructsToJson
from .expressions.literals import Literal
from .expressions.mappers import (
    Abs, Acos, Ascii, Asin, Atan, Atan2, Base64, Bin, Bround, CaseWhen, Cbrt, Ceil, Coalesce, Concat, ConcatWs, Conv,
    Cos, Cosh, CreateStruct, Exp, ExpM1, Factorial, Floor, FormatNumber, Greatest, Grouping, GroupingID, Hex, Hypot,
    InputFileName, IsNaN, Least, Length, Log, Log1p, Log2, Log10, Lower, MapConcat, MapEntries, MapFromEntries,
    MapKeys, MapValues, MonotonicallyIncreasingID, NaNvl, Rand, Randn, RegExpExtract, RegExpReplace, Reverse, Rint,
    Round, ShiftLeft, ShiftRight, ShiftRightUnsigned, Signum, Sin, Sinh, SparkPartitionID, Sqrt, StringSplit,
    SubstringIndex, Tan, Tanh, ToDegrees, ToRadians, UnBase64, Unhex, Upper
)
from .expressions.operators import BitwiseNot, IsNull, Pow, Substring
from .expressions.strings import (
    InitCap, Levenshtein, SoundEx, StringInStr, StringLocate, StringLPad, StringLTrim, StringRepeat, StringRPad,
    StringRTrim, StringTranslate, StringTrim
)
from .expressions.userdefined import UserDefinedFunction
from .types import DataType
from .utils import AnalysisException


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

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import when, col
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5).withColumn(
    ...   "order", when(col('id')%2 == 0, col('id'))
    ... ).orderBy(asc("order")).show()
    +---+-----+
    | id|order|
    +---+-----+
    |  1| null|
    |  3| null|
    |  0|    0|
    |  2|    2|
    |  4|    4|
    +---+-----+
    """
    return parse(columnName).asc()


def asc_nulls_first(columnName):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import when, col
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5).withColumn(
    ...   "order", when(col('id')%2 == 0, col('id'))
    ... ).orderBy(asc_nulls_first("order")).show()
    +---+-----+
    | id|order|
    +---+-----+
    |  1| null|
    |  3| null|
    |  0|    0|
    |  2|    2|
    |  4|    4|
    +---+-----+
    """
    return parse(columnName).asc_nulls_first()


def asc_nulls_last(columnName):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import when, col
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5).withColumn(
    ...   "order", when(col('id')%2 == 0, col('id'))
    ... ).orderBy(asc_nulls_last("order")).show()
    +---+-----+
    | id|order|
    +---+-----+
    |  0|    0|
    |  2|    2|
    |  4|    4|
    |  1| null|
    |  3| null|
    +---+-----+
    """
    return parse(columnName).asc_nulls_last()


def desc(columnName):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import when, col
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5).withColumn(
    ...   "order", when(col('id')%2 == 0, col('id'))
    ... ).orderBy(desc("order")).show()
    +---+-----+
    | id|order|
    +---+-----+
    |  4|    4|
    |  2|    2|
    |  0|    0|
    |  1| null|
    |  3| null|
    +---+-----+
    """
    return parse(columnName).desc()


def desc_nulls_first(columnName):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import when, col
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5).withColumn(
    ...   "order", when(col('id')%2 == 0, col('id'))
    ... ).orderBy(desc_nulls_first("order")).show()
    +---+-----+
    | id|order|
    +---+-----+
    |  1| null|
    |  3| null|
    |  4|    4|
    |  2|    2|
    |  0|    0|
    +---+-----+
    """
    return parse(columnName).desc_nulls_first()


def desc_nulls_last(columnName):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import when, col
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5).withColumn(
    ...   "order", when(col('id')%2 == 0, col('id'))
    ... ).orderBy(desc_nulls_last("order")).show()
    +---+-----+
    | id|order|
    +---+-----+
    |  4|    4|
    |  2|    2|
    |  0|    0|
    |  1| null|
    |  3| null|
    +---+-----+
    """
    return parse(columnName).desc_nulls_last()


def approx_count_distinct(e, rsd=0.05):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(100).select((col("id")%10).alias("n")).select(approx_count_distinct("n")).show()
    +------------------------+
    |approx_count_distinct(n)|
    +------------------------+
    |                      10|
    +------------------------+
    """
    # NB: This function returns the exact number of distinct values in pysparkling
    # as it does not rely on HyperLogLogPlusPlus or another estimator
    return col(ApproxCountDistinct(column=parse(e)))


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
    return col(CollectList(column=parse(e)))


def corr(column1, column2):
    """
    :rtype: Column
    """
    return col(Corr(
        column1=parse(column1),
        column2=parse(column2)
    ))


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
    return col(CaseWhen([parse(condition)], [parse(value)]))


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
    return col(Rand(parse(seed)))


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
    return col(Randn(lit(seed)))


def struct(*exprs):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([Row(age=2, name='Alice'), Row(age=5, name='Bob')])
    >>> df.select(struct([df.age, df.name]).alias("struct")).collect()
    [Row(struct=Row(age=2, name='Alice')), Row(struct=Row(age=5, name='Bob'))]
    >>> df.select(struct('age', 'name').alias("struct")).collect()
    [Row(struct=Row(age=2, name='Alice')), Row(struct=Row(age=5, name='Bob'))]
    >>> df.select(struct("age", col("name"))).show()
    +-----------------+
    |struct(age, name)|
    +-----------------+
    |       [2, Alice]|
    |         [5, Bob]|
    +-----------------+
    >>> df.select(struct("age", col("name"))).collect()
    [Row(struct(age, name)=Row(age=2, name='Alice')), Row(struct(age, name)=Row(age=5, name='Bob'))]
    """
    if len(exprs) == 1 and isinstance(exprs[0], list):
        exprs = exprs[0]
    cols = [parse(e) for e in exprs]
    return col(CreateStruct(cols))


def array(*exprs):
    """
    :rtype: Column
    """
    columns = [parse(e) for e in exprs]
    return col(ArrayColumn(columns))


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


def count(e):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(5).select(count("*")).show()
    +--------+
    |count(1)|
    +--------+
    |       5|
    +--------+
    """
    return col(Count(column=parse(e)))


def countDistinct(*exprs):
    """
    :rtype: Column
    """
    columns = [parse(e) for e in exprs]
    return col(CountDistinct(columns=columns))


def collect_set(e):
    """
    :rtype: Column
    """
    return col(CollectSet(column=parse(e)))


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
    return col(First(parse(e), lit(ignoreNulls)))


def last(e, ignoreNulls=False):
    """
    :rtype: Column
    """
    return col(Last(parse(e), lit(ignoreNulls)))


def grouping(e):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([(2, 'Alice'), (5, 'Bob'), (5, 'Carl')], ["age", "name"])
    >>> (df.cube("name", df.age)
    ...    .agg(count("*"), grouping(df.age))
    ...    .orderBy("name", "age")
    ... ).show()
    +-----+----+--------+-------------+
    | name| age|count(1)|grouping(age)|
    +-----+----+--------+-------------+
    | null|null|       3|            1|
    | null|   2|       1|            0|
    | null|   5|       2|            0|
    |Alice|null|       1|            1|
    |Alice|   2|       1|            0|
    |  Bob|null|       1|            1|
    |  Bob|   5|       1|            0|
    | Carl|null|       1|            1|
    | Carl|   5|       1|            0|
    +-----+----+--------+-------------+

    """
    return col(Grouping(parse(e)))


def grouping_id(*exprs):
    """

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame(
    ...     [(2, 'Alice', 3), (5, 'Bob', 4), (5, None, 6)],
    ...     ["age", "name", "id"]
    ... )
    >>> (df.cube("name", df.age)
    ...    .agg(count("*"), grouping_id())
    ...     .orderBy("name", "age", "count(1)")
    ... ).show()
    +-----+----+--------+-------------+
    | name| age|count(1)|grouping_id()|
    +-----+----+--------+-------------+
    | null|null|       1|            1|
    | null|null|       3|            3|
    | null|   2|       1|            2|
    | null|   5|       1|            0|
    | null|   5|       2|            2|
    |Alice|null|       1|            1|
    |Alice|   2|       1|            0|
    |  Bob|null|       1|            1|
    |  Bob|   5|       1|            0|
    +-----+----+--------+-------------+
    >>> (df
    ...   .rollup("name", df.age)
    ...   .agg(count("*"), grouping_id())
    ...   .orderBy("name", "age", "count(1)")
    ...  ).show()
    +-----+----+--------+-------------+
    | name| age|count(1)|grouping_id()|
    +-----+----+--------+-------------+
    | null|null|       1|            1|
    | null|null|       3|            3|
    | null|   5|       1|            0|
    |Alice|null|       1|            1|
    |Alice|   2|       1|            0|
    |  Bob|null|       1|            1|
    |  Bob|   5|       1|            0|
    +-----+----+--------+-------------+

    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return col(GroupingID(cols))


def kurtosis(e):
    """
    :rtype: Column
    """
    return col(Kurtosis(column=parse(e)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
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
# pylint: disable=W0622
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


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
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

def cume_dist():
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def dense_rank():
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def lag(e, offset, defaultValue=None):
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def lead(e, offset, defaultValue=None):
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def ntile(n):
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def percent_rank():
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def rank():
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def row_number():
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


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

    return col(MapColumn(cols))


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

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(10).repartition(1).select("id", monotonically_increasing_id()).show()
    +---+-----------------------------+
    | id|monotonically_increasing_id()|
    +---+-----------------------------+
    |  0|                            0|
    |  1|                            1|
    |  2|                            2|
    |  3|                            3|
    |  4|                            4|
    |  5|                            5|
    |  6|                            6|
    |  7|                            7|
    |  8|                            8|
    |  9|                            9|
    +---+-----------------------------+
    >>> spark.range(3).repartition(5).select("id", monotonically_increasing_id()).show()
    +---+-----------------------------+
    | id|monotonically_increasing_id()|
    +---+-----------------------------+
    |  0|                   8589934592|
    |  1|                  25769803776|
    |  2|                  34359738368|
    +---+-----------------------------+

    """
    return col(MonotonicallyIncreasingID())


def nanvl(col1, col2):
    """
    :rtype: Column
    """
    return col(NaNvl(parse(col1), parse(col2)))


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


def bitwiseNOT(e):
    """
    :rtype: Column
    """
    return col(BitwiseNot(parse(e)))


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
    return col(Conv(parse(num), lit(fromBase), lit(toBase)))


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


# noinspection PyShadowingBuiltins
# pylint: disable=W0621
def hex(column):
    """
    :rtype: Column
    """
    return col(Hex(parse(column)))


# noinspection PyShadowingNames
# pylint: disable=W0621
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
    return col(Log(lit(base), value))


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


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def pow(l, r):
    """
    :rtype: Column
    """
    return col(Pow(parse(l), parse(r)))


def rint(e):
    """
    :rtype: Column
    """
    return col(Rint(parse(e)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def round(e, scale=0):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(
    ...     round(lit(9.1)),
    ...     round(lit(9.9)),
    ...     round(lit(9.5)),
    ...     round(lit(8.5)),
    ...     round(lit(15), -1),
    ...     round(lit(25), -1)
    ... ).show()
    +-------------+-------------+-------------+-------------+-------------+-------------+
    |round(9.1, 0)|round(9.9, 0)|round(9.5, 0)|round(8.5, 0)|round(15, -1)|round(25, -1)|
    +-------------+-------------+-------------+-------------+-------------+-------------+
    |          9.0|         10.0|         10.0|          9.0|           20|           30|
    +-------------+-------------+-------------+-------------+-------------+-------------+


    """
    return col(Round(parse(e), lit(scale)))


def bround(e, scale=0):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(
    ...     bround(lit(9.1)),
    ...     bround(lit(9.9)),
    ...     bround(lit(9.5)),
    ...     bround(lit(8.5)),
    ...     bround(lit(15), -1),
    ...     bround(lit(25), -1)
    ... ).show()
    +--------------+--------------+--------------+--------------+--------------+--------------+
    |bround(9.1, 0)|bround(9.9, 0)|bround(9.5, 0)|bround(8.5, 0)|bround(15, -1)|bround(25, -1)|
    +--------------+--------------+--------------+--------------+--------------+--------------+
    |           9.0|          10.0|          10.0|           8.0|            20|            20|
    +--------------+--------------+--------------+--------------+--------------+--------------+
    """
    return col(Bround(parse(e), lit(scale)))


def shiftLeft(e, numBits):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import shiftLeft, shiftRight, shiftRightUnsigned
    >>> spark = SparkSession(Context())
    >>> df = spark.range(-5, 4)
    >>> df.select("id", shiftLeft("id", 1)).show()
    +---+----------------+
    | id|shiftleft(id, 1)|
    +---+----------------+
    | -5|             -10|
    | -4|              -8|
    | -3|              -6|
    | -2|              -4|
    | -1|              -2|
    |  0|               0|
    |  1|               2|
    |  2|               4|
    |  3|               6|
    +---+----------------+
    """
    return col(ShiftLeft(parse(e), lit(numBits)))


def shiftRight(e, numBits):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import shiftLeft, shiftRight, shiftRightUnsigned
    >>> spark = SparkSession(Context())
    >>> df = spark.range(-5, 4)
    >>> df.select("id", shiftRight("id", 1)).show()
    +---+-----------------+
    | id|shiftright(id, 1)|
    +---+-----------------+
    | -5|               -3|
    | -4|               -2|
    | -3|               -2|
    | -2|               -1|
    | -1|               -1|
    |  0|                0|
    |  1|                0|
    |  2|                1|
    |  3|                1|
    +---+-----------------+

    """
    return col(ShiftRight(parse(e), lit(numBits)))


def shiftRightUnsigned(e, numBits):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import shiftLeft, shiftRight, shiftRightUnsigned
    >>> spark = SparkSession(Context())
    >>> df = spark.range(-5, 4)
    >>> df.select("id", shiftRight("id", 1), shiftRightUnsigned("id", 1)).show()
    +---+-----------------+-------------------------+
    | id|shiftright(id, 1)|shiftrightunsigned(id, 1)|
    +---+-----------------+-------------------------+
    | -5|               -3|      9223372036854775805|
    | -4|               -2|      9223372036854775806|
    | -3|               -2|      9223372036854775806|
    | -2|               -1|      9223372036854775807|
    | -1|               -1|      9223372036854775807|
    |  0|                0|                        0|
    |  1|                0|                        0|
    |  2|                1|                        1|
    |  3|                1|                        1|
    +---+-----------------+-------------------------+
    """
    return col(ShiftRightUnsigned(parse(e), lit(numBits)))


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
    raise NotImplementedError("Pysparkling does not support yet this function")


def sha1(e):
    raise NotImplementedError("Pysparkling does not support yet this function")


def sha2(e, numBits):
    raise NotImplementedError("Pysparkling does not support yet this function")


def crc32(e):
    raise NotImplementedError("Pysparkling does not support yet this function")


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def hash(*exprs):
    raise NotImplementedError("Pysparkling does not support yet this function")


def xxhash64(*exprs):
    raise NotImplementedError("Pysparkling does not support yet this function")


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def ascii(e):
    """
    :rtype: Column
    """
    return col(Ascii(parse(e)))


def base64(e):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(base64(lit("Hello world!"))).show()
    +--------------------+
    |base64(Hello world!)|
    +--------------------+
    |    SGVsbG8gd29ybGQh|
    +--------------------+
    """
    return col(Base64(parse(e)))


def concat_ws(sep, *exprs):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(3).select(concat_ws("---", "id", lit(2), "id")).show()
    +-------------------------+
    |concat_ws(---, id, 2, id)|
    +-------------------------+
    |                0---2---0|
    |                1---2---1|
    |                2---2---2|
    +-------------------------+
    >>> spark.range(1).select(concat_ws("---")).show()
    +--------------+
    |concat_ws(---)|
    +--------------+
    |              |
    +--------------+

    """
    cols = [parse(e) for e in exprs]
    return col(ConcatWs(lit(sep), cols))


def decode(value, charset):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def encode(value, charset):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def format_number(x, d):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(format_number(lit(1000000.8725), 3)).show()
    +------------------------------+
    |format_number(1000000.8725, 3)|
    +------------------------------+
    |                 1,000,000.873|
    +------------------------------+
    """
    return col(FormatNumber(parse(x), lit(d)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def format_string(format, *exprs):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def initcap(e):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(initcap(lit("hello world:o_O"))).show()
    +------------------------+
    |initcap(hello world:o_O)|
    +------------------------+
    |         Hello World:o_o|
    +------------------------+
    """
    return col(InitCap(parse(e)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def instr(str, substring):
    """
    :rtype: Column
    """
    return col(StringInStr(parse(str), lit(substring)))


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

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(levenshtein(lit("kitten"), lit("sitting"))).show()
    +----------------------------+
    |levenshtein(kitten, sitting)|
    +----------------------------+
    |                           3|
    +----------------------------+

    """
    return col(Levenshtein(parse(l), parse(r)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def locate(substr, str, pos=1):
    """
    :rtype: Column
    """
    return col(StringLocate(lit(substr), parse(str), lit(pos)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def lpad(str, len, pad):
    """
    :rtype: Column
    """
    return col(StringLPad(parse(str), lit(len), lit(pad)))


def ltrim(e):
    """
    :rtype: Column
    """
    return col(StringLTrim(parse(e)))


def regexp_extract(e, exp, groupIdx):
    """
    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([('100-200',)], ['str'])
    >>> df.collect()
    [Row(str='100-200')]
    >>> df.select(regexp_extract(df.str, r'(\\d+)-(\\d+)', 1).alias('d')).collect()
    [Row(d='100')]

    :rtype: Column
    """
    return col(RegExpExtract(parse(e), lit(exp), lit(groupIdx)))


def regexp_replace(e, pattern, replacement):
    """
    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([('100-200',)], ['str'])
    >>> df.collect()
    [Row(str='100-200')]
    >>> df.select(regexp_replace(df.str, r'-(\\d+)', '-300').alias('d')).collect()
    [Row(d='100-300')]

    :rtype: Column
    """
    return col(RegExpReplace(parse(e), lit(pattern), lit(replacement)))


def unbase64(e):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(unbase64(lit("SGVsbG8gd29ybGQh"))).collect()
    [Row(unbase64(SGVsbG8gd29ybGQh)=bytearray(b'Hello world!'))]
    """
    return col(UnBase64(parse(e)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def rpad(str, len, pad):
    """
    :rtype: Column
    """
    return col(StringRPad(parse(str), lit(len), lit(pad)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def repeat(str, n):
    """
    :rtype: Column
    """
    return col(StringRepeat(parse(str), lit(n)))


def rtrim(e):
    """
    :rtype: Column
    """
    return col(StringRTrim(parse(e)))


def soundex(e):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([
    ...   ("Robert", ),
    ...   ("Rupert", ),
    ...   ("Rubin", ),
    ...   ("Ashcraft", ),
    ...   ("Ashcroft", ),
    ...   ("Tymczak", ),
    ...   ("Honeyman", ),
    ... ], ["str"])
    >>> df.select("str", soundex("str")).orderBy("str").show()
    +--------+------------+
    |     str|soundex(str)|
    +--------+------------+
    |Ashcraft|        A261|
    |Ashcroft|        A261|
    |Honeyman|        H555|
    |  Robert|        R163|
    |   Rubin|        R150|
    |  Rupert|        R163|
    | Tymczak|        T522|
    +--------+------------+
    """
    return SoundEx(parse(e))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def split(str, regex, limit=None):
    """
    :rtype: Column
    """
    return col(StringSplit(parse(str), lit(regex), lit(limit)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def substring(str, pos, len):
    """
    :rtype: Column
    """
    return col(Substring(str, lit(pos), lit(len)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def substring_index(str, delim, count):
    """

    Returns the substring from string str before count occurrences of the delimiter delim.
    If count is positive, everything the left of the final delimiter (counting from left) is
    returned. If count is negative, every to the right of the final delimiter (counting from the
    right) is returned. substring_index performs a case-sensitive match when searching for delim.

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([["a.b.c.d"],["a.b"],["a"]], ["s"])
    >>> df.select(substring_index(df.s, ".", 3)).show()
    +------------------------+
    |substring_index(s, ., 3)|
    +------------------------+
    |                   a.b.c|
    |                     a.b|
    |                       a|
    +------------------------+
    >>> df.select(substring_index(df.s, ".", -3)).show()
    +-------------------------+
    |substring_index(s, ., -3)|
    +-------------------------+
    |                    b.c.d|
    |                      a.b|
    |                        a|
    +-------------------------+

    :rtype: Column
    """
    return col(SubstringIndex(parse(str), lit(delim), lit(count)))


def translate(srcCol, matchingString, replaceString):
    """
    :rtype: Column
    """
    return col(StringTranslate(parse(srcCol), lit(matchingString), lit(replaceString)))


def trim(e):
    """
    :rtype: Column
    """
    return col(StringTrim(parse(e)))


def upper(e):
    """
    :rtype: Column
    """
    return col(Upper(parse(e)))


def add_months(startDate, numMonths):
    """
    :rtype: Column
    """
    return col(AddMonths(ensure_column(startDate), lit(numMonths)))


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


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def date_format(dateExpr, format):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(date_format(lit("2019-10-31"), 'MM/dd/yyy')).show()
    +----------------------------------+
    |date_format(2019-10-31, MM/dd/yyy)|
    +----------------------------------+
    |                        10/31/2019|
    +----------------------------------+
    """
    return col(DateFormat(ensure_column(dateExpr), lit(format)))


def date_add(start, days):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(date_add(lit("2019-02-27"), 2)).show()
    +-----------------------+
    |date_add(2019-02-27, 2)|
    +-----------------------+
    |             2019-03-01|
    +-----------------------+

    """
    return col(DateAdd(ensure_column(start), lit(days)))


def date_sub(start, days):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(date_sub(lit("2019-03-01"), 2)).show()
    +-----------------------+
    |date_sub(2019-03-01, 2)|
    +-----------------------+
    |             2019-02-27|
    +-----------------------+
    """
    return col(DateSub(ensure_column(start), lit(days)))


def datediff(end, start):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(datediff(lit("2019-05-01"), lit("2019-04-01"))).show()
    +--------------------------------+
    |datediff(2019-05-01, 2019-04-01)|
    +--------------------------------+
    |                              30|
    +--------------------------------+

    >>> spark.range(1).select(datediff(lit("2018-05-01"), lit("2019-04-01"))).show()
    +--------------------------------+
    |datediff(2018-05-01, 2019-04-01)|
    +--------------------------------+
    |                            -335|
    +--------------------------------+

    """
    return col(DateDiff(ensure_column(end), ensure_column(start)))


def year(e):
    """
    :rtype: Column
    """
    return col(Year(ensure_column(e)))


def quarter(e):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(quarter(lit("2019-04-01"))).show()
    +-------------------+
    |quarter(2019-04-01)|
    +-------------------+
    |                  2|
    +-------------------+
    """
    return col(Quarter(ensure_column(e)))


def month(e):
    """
    :rtype: Column
    """
    return col(Month(ensure_column(e)))


def dayofweek(e):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1, 10).withColumn(
    ...     "date", concat(lit("2019-01-0"), "id")
    ... ).withColumn(
    ...     "dayOfWeek", dayofweek("date")
    ... ).show()
    +---+----------+---------+
    | id|      date|dayOfWeek|
    +---+----------+---------+
    |  1|2019-01-01|        3|
    |  2|2019-01-02|        4|
    |  3|2019-01-03|        5|
    |  4|2019-01-04|        6|
    |  5|2019-01-05|        7|
    |  6|2019-01-06|        1|
    |  7|2019-01-07|        2|
    |  8|2019-01-08|        3|
    |  9|2019-01-09|        4|
    +---+----------+---------+

    """
    return col(DayOfWeek(ensure_column(e)))


def dayofmonth(e):
    """
    :rtype: Column
    """
    return col(DayOfMonth(ensure_column(e)))


def dayofyear(e):
    """

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1, 10).withColumn(
    ...     "date", concat(lit("2019-01-0"), "id")
    ... ).withColumn(
    ...     "dayOfYear", dayofyear("date")
    ... ).show()
    +---+----------+---------+
    | id|      date|dayOfYear|
    +---+----------+---------+
    |  1|2019-01-01|        1|
    |  2|2019-01-02|        2|
    |  3|2019-01-03|        3|
    |  4|2019-01-04|        4|
    |  5|2019-01-05|        5|
    |  6|2019-01-06|        6|
    |  7|2019-01-07|        7|
    |  8|2019-01-08|        8|
    |  9|2019-01-09|        9|
    +---+----------+---------+

    :rtype: Column
    """
    return col(DayOfYear(ensure_column(e)))


def hour(e):
    """
    :rtype: Column
    """
    return col(Hour(ensure_column(e)))


def last_day(e):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(last_day(lit("2019-10-15"))).show()
    +--------------------+
    |last_day(2019-10-15)|
    +--------------------+
    |          2019-10-31|
    +--------------------+
    """
    return col(LastDay(ensure_column(e)))


def minute(e):
    """
    :rtype: Column
    """
    return col(Minute(ensure_column(e)))


def months_between(end, start, roundOff=True):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(months_between(lit("2019-05-02"), lit("2019-05-01"))).show()
    +--------------------------------------------+
    |months_between(2019-05-02, 2019-05-01, true)|
    +--------------------------------------------+
    |                                  0.03225806|
    +--------------------------------------------+
    >>> spark.range(1).select(months_between(lit("2019-04-30"), lit("2019-05-01"))).show()
    +--------------------------------------------+
    |months_between(2019-04-30, 2019-05-01, true)|
    +--------------------------------------------+
    |                                 -0.06451613|
    +--------------------------------------------+
    >>> spark.range(1).select(months_between(lit("2019-05-01"), lit("2019-04-30"), False)).show()
    +---------------------------------------------+
    |months_between(2019-05-01, 2019-04-30, false)|
    +---------------------------------------------+
    |                          0.06451612903225812|
    +---------------------------------------------+
    >>> spark.range(1).select(
    ...   months_between(lit("2019-05-01 01:23:45.678910"),
    ...   lit("2019-04-01 12:00:00"))
    ... ).show()
    +---------------------------------------------------------------------+
    |months_between(2019-05-01 01:23:45.678910, 2019-04-01 12:00:00, true)|
    +---------------------------------------------------------------------+
    |                                                                  1.0|
    +---------------------------------------------------------------------+
    >>> spark.range(1).select(
    ...   months_between(lit("2019-05-02 01:23:45.678910"), lit("2019-04-01 12:00:00"))
    ... ).show()
    +---------------------------------------------------------------------+
    |months_between(2019-05-02 01:23:45.678910, 2019-04-01 12:00:00, true)|
    +---------------------------------------------------------------------+
    |                                                           1.01800515|
    +---------------------------------------------------------------------+
    >>> spark.range(1).select(
    ...   months_between(lit("2019-05-31 01:23:45.678910"), lit("2019-02-28 12:00:00"))
    ... ).show()
    +---------------------------------------------------------------------+
    |months_between(2019-05-31 01:23:45.678910, 2019-02-28 12:00:00, true)|
    +---------------------------------------------------------------------+
    |                                                                  3.0|
    +---------------------------------------------------------------------+
    """
    return col(MonthsBetween(ensure_column(end), ensure_column(start), lit(roundOff)))


def next_day(date, dayOfWeek):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1, 10).select(next_day(concat(lit("2019-11-0"), col("id")), "Mon")).show()
    +------------------------------------+
    |next_day(concat(2019-11-0, id), Mon)|
    +------------------------------------+
    |                          2019-11-04|
    |                          2019-11-04|
    |                          2019-11-04|
    |                          2019-11-11|
    |                          2019-11-11|
    |                          2019-11-11|
    |                          2019-11-11|
    |                          2019-11-11|
    |                          2019-11-11|
    +------------------------------------+
    """
    return col(NextDay(ensure_column(date), lit(dayOfWeek)))


def second(e):
    """
    :rtype: Column
    """
    return col(Second(ensure_column(e)))


def weekofyear(e):
    """
    :rtype: Column
    """
    return col(WeekOfYear(ensure_column(e)))


def from_unixtime(ut, f="yyyy-MM-dd HH:mm:ss"):
    """
    :rtype: Column

    >>> import os, time
    >>> os.environ['TZ'] = 'Europe/Paris'
    >>> if hasattr(time, 'tzset'): time.tzset()
    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1, 4).select(from_unixtime(2000000000 + col("id"))).show()
    +-----------------------------------------------------+
    |from_unixtime((id + 2000000000), yyyy-MM-dd HH:mm:ss)|
    +-----------------------------------------------------+
    |                                  2033-05-18 05:33:21|
    |                                  2033-05-18 05:33:22|
    |                                  2033-05-18 05:33:23|
    +-----------------------------------------------------+
    """
    return col(FromUnixTime(parse(ut), lit(f)))


def unix_timestamp(s=None, p="yyyy-MM-dd HH:mm:ss"):
    """
    :rtype: Column

    >>> import os, time
    >>> os.environ['TZ'] = 'Europe/Paris'
    >>> if hasattr(time, 'tzset'): time.tzset()
    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(unix_timestamp(lit("2033-05-18 05:33:21"))).show()
    +--------------------------------------------------------+
    |unix_timestamp(2033-05-18 05:33:21, yyyy-MM-dd HH:mm:ss)|
    +--------------------------------------------------------+
    |                                              2000000001|
    +--------------------------------------------------------+
    >>> spark.range(1).select(unix_timestamp(lit("2019-01-01"), "yyyy-MM-dd")).show()
    +--------------------------------------+
    |unix_timestamp(2019-01-01, yyyy-MM-dd)|
    +--------------------------------------+
    |                            1546297200|
    +--------------------------------------+
    """
    if s is None:
        s = col(CurrentTimestamp())
    return col(UnixTimestamp(ensure_column(s), lit(p)))


def to_timestamp(s, fmt=None):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(to_timestamp(lit("2033-05-18 05:33:21"))).show()
    +-----------------------------------+
    |to_timestamp('2033-05-18 05:33:21')|
    +-----------------------------------+
    |                2033-05-18 05:33:21|
    +-----------------------------------+
    >>> spark.range(1).select(to_timestamp(lit("2019-01-01"), "yyyy-MM-dd")).show()
    +----------------------------------------+
    |to_timestamp('2019-01-01', 'yyyy-MM-dd')|
    +----------------------------------------+
    |                     2019-01-01 00:00:00|
    +----------------------------------------+
    """
    return col(ParseToTimestamp(ensure_column(s), lit(fmt)))


def to_date(e, fmt=None):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(to_date(lit("2033-05-18 05:33:21"))).show()
    +------------------------------+
    |to_date('2033-05-18 05:33:21')|
    +------------------------------+
    |                    2033-05-18|
    +------------------------------+

    >>> spark.range(1).select(to_date(lit("2019-01-01"), "yyyy-MM-dd")).show()
    +-----------------------------------+
    |to_date('2019-01-01', 'yyyy-MM-dd')|
    +-----------------------------------+
    |                         2019-01-01|
    +-----------------------------------+

    """
    return col(ParseToDate(ensure_column(e), lit(fmt)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def trunc(date, format):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(trunc(lit("2019-11-05"), "year")).show()
    +-----------------------+
    |trunc(2019-11-05, year)|
    +-----------------------+
    |             2019-01-01|
    +-----------------------+

    >>> spark.range(1).select(trunc(lit("2019-11-05"), "month")).show()
    +------------------------+
    |trunc(2019-11-05, month)|
    +------------------------+
    |              2019-11-01|
    +------------------------+

    >>> spark.range(1).select(trunc(lit("2019-11-05"), "quarter")).show()
    +--------------------------+
    |trunc(2019-11-05, quarter)|
    +--------------------------+
    |                      null|
    +--------------------------+


    """
    return col(TruncDate(parse(date), lit(format)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def date_trunc(format, timestamp):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(date_trunc("hour", concat(lit("2019-11-05 04:55")))).show()
    +------------------------------------------+
    |date_trunc(hour, concat(2019-11-05 04:55))|
    +------------------------------------------+
    |                       2019-11-05 04:00:00|
    +------------------------------------------+
    >>> spark.range(1, 10).select(date_trunc("week", concat(lit("2019-11-0"), col("id")))).show()
    +---------------------------------------+
    |date_trunc(week, concat(2019-11-0, id))|
    +---------------------------------------+
    |                    2019-10-28 00:00:00|
    |                    2019-10-28 00:00:00|
    |                    2019-10-28 00:00:00|
    |                    2019-11-04 00:00:00|
    |                    2019-11-04 00:00:00|
    |                    2019-11-04 00:00:00|
    |                    2019-11-04 00:00:00|
    |                    2019-11-04 00:00:00|
    |                    2019-11-04 00:00:00|
    +---------------------------------------+

    """
    return col(TruncTimestamp(lit(format), parse(timestamp)))


def from_utc_timestamp(ts, tz):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(from_utc_timestamp(lit("2019-11-05 04:55"), "Europe/Paris")).show()
    +--------------------------------------------------+
    |from_utc_timestamp(2019-11-05 04:55, Europe/Paris)|
    +--------------------------------------------------+
    |                               2019-11-05 05:55:00|
    +--------------------------------------------------+
    >>> spark.range(1).select(from_utc_timestamp(lit("2019-11-05 04:55"), "GMT+1")).show()
    +-------------------------------------------+
    |from_utc_timestamp(2019-11-05 04:55, GMT+1)|
    +-------------------------------------------+
    |                        2019-11-05 05:55:00|
    +-------------------------------------------+
    >>> spark.range(1).select(from_utc_timestamp(lit("2019-11-05 04:55"), "GMT-1:49")).show()
    +----------------------------------------------+
    |from_utc_timestamp(2019-11-05 04:55, GMT-1:49)|
    +----------------------------------------------+
    |                           2019-11-05 03:06:00|
    +----------------------------------------------+
    """
    return col(FromUTCTimestamp(ensure_column(ts), lit(tz)))


def to_utc_timestamp(ts, tz):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(to_utc_timestamp(lit("2019-11-05 04:55"), "Europe/Paris")).show()
    +------------------------------------------------+
    |to_utc_timestamp(2019-11-05 04:55, Europe/Paris)|
    +------------------------------------------------+
    |                             2019-11-05 03:55:00|
    +------------------------------------------------+
    >>> spark.range(1).select(to_utc_timestamp(lit("2019-11-05 04:55"), "GMT+1")).show()
    +-----------------------------------------+
    |to_utc_timestamp(2019-11-05 04:55, GMT+1)|
    +-----------------------------------------+
    |                      2019-11-05 03:55:00|
    +-----------------------------------------+
    >>> spark.range(1).select(to_utc_timestamp(lit("2019-11-05 04:55"), "GMT-1:49")).show()
    +--------------------------------------------+
    |to_utc_timestamp(2019-11-05 04:55, GMT-1:49)|
    +--------------------------------------------+
    |                         2019-11-05 06:44:00|
    +--------------------------------------------+
    """
    return col(ToUTCTimestamp(ensure_column(ts), lit(tz)))


def window(timeColumn, windowDuration, slideDuration=None, startTime="0 second"):
    raise NotImplementedError("Pysparkling does not support yet this function")


def array_contains(column, value):
    """
    :rtype: Column
    """
    return col(ArrayContains(parse(column), lit(value)))


def arrays_overlap(a1, a2):
    """
    :rtype: Column
    """
    return col(ArraysOverlap(parse(a1), parse(a2)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def slice(x, start, length):
    """
    :rtype: Column
    """
    return col(Slice(ensure_column(x), lit(start), lit(length)))


def array_join(column, delimiter, nullReplacement=None):
    """
    :rtype: Column
    """
    return col(ArrayJoin(ensure_column(column), lit(delimiter), lit(nullReplacement)))


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
    return col(ArrayPosition(ensure_column(column), lit(value)))


def element_at(column, value):
    """
    :rtype: Column
    """
    return col(ElementAt(ensure_column(column), lit(value)))


def array_sort(e):
    """
    :rtype: Column
    """
    return col(ArraySort(ensure_column(e)))


def array_remove(column, element):
    """
    :rtype: Column
    """
    return col(ArrayRemove(ensure_column(column), lit(element)))


def array_distinct(e):
    """
    :rtype: Column
    """
    return col(ArrayDistinct(ensure_column(e)))


def array_intersect(col1, col2):
    """
    :rtype: Column
    """
    return col(ArrayIntersect(parse(col1), parse(col2)))


def array_union(col1, col2):
    """
    :rtype: Column
    """
    return col(ArrayUnion(parse(col1), parse(col2)))


def array_except(col1, col2):
    """
    :rtype: Column
    """
    return col(ArrayExcept(parse(col1), parse(col2)))


def explode(e):
    """
    :rtype: Column
    """
    return col(Explode(parse(e)))


def explode_outer(e):
    """
    :rtype: Column
    """
    return col(ExplodeOuter(ensure_column(e)))


def posexplode(e):
    """
    :rtype: Column
    """
    return col(PosExplode(ensure_column(e)))


def posexplode_outer(e):
    """
    :rtype: Column
    """
    return col(PosExplodeOuter(ensure_column(e)))


def get_json_object(e, path):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def json_tuple(json, *fields):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def from_json(e, schema, options=None):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def schema_of_json(json, options=None):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def to_json(e, options=None):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> from pysparkling import Row
    >>> df = spark.createDataFrame([(
    ...   1,
    ...   [Row(name='Alice', age=2),
    ...    Row(name='Bob', age=3)]
    ...  )],
    ...  ("key", "value")
    ... )
    >>> df.select(to_json(df.value)).show(truncate=False)
    +-------------------------------------------------+
    |structstojson(value)                             |
    +-------------------------------------------------+
    |[{"age":2,"name":"Alice"},{"age":3,"name":"Bob"}]|
    +-------------------------------------------------+
    >>> df = spark.createDataFrame([(1, [{"name": "Alice"}, {"name": "Bob"}])], ("key", "value"))
    >>> df.select(to_json(df.value)).show(truncate=False)
    +---------------------------------+
    |structstojson(value)             |
    +---------------------------------+
    |[{"name":"Alice"},{"name":"Bob"}]|
    +---------------------------------+
    >>> df = spark.createDataFrame([(1, ["Alice", "Bob"])], ("key", "value"))
    >>> df.select(to_json("value")).show(truncate=False)
    +--------------------+
    |structstojson(value)|
    +--------------------+
    |["Alice","Bob"]     |
    +--------------------+

    """
    return col(StructsToJson(parse(e), options))


def size(e):
    """
    :rtype: Column
    """
    return col(Size(parse(e)))


def sort_array(e, asc=True):
    """
    :rtype: Column
    """
    return col(SortArray(parse(e), lit(asc)))


def array_min(e):
    """
    :rtype: Column
    """
    return col(ArrayMin(ensure_column(e)))


def array_max(e):
    """
    :rtype: Column
    """
    return col(ArrayMax(ensure_column(e)))


def shuffle(e):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def reverse(e):
    """
    :rtype: Column
    """
    return col(Reverse(ensure_column(e)))


def flatten(e):
    """
    :rtype: Column
    """
    return col(Flatten(ensure_column(e)))


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
    return col(ArrayRepeat(parse(e), lit(count)))


def map_keys(e):
    """
    :rtype: Column
    """
    return col(MapKeys(ensure_column(e)))


def map_values(e):
    """
    :rtype: Column
    """
    return col(MapValues(ensure_column(e)))


def map_entries(e):
    """
    :rtype: Column
    """
    return col(MapEntries(ensure_column(e)))


def map_from_entries(e):
    """
    :rtype: Column
    """
    return col(MapFromEntries(ensure_column(e)))


def arrays_zip(*exprs):
    """
    :rtype: Column
    """
    return col(ArraysZip([parse(e) for e in exprs]))


def map_concat(*exprs):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame(
    ...   [([1, 2], ['a', 'b'], [2, 3], ['c', 'd'])],
    ...   ['k1', 'v1', 'k2', 'v2']
    ... )
    >>> df2 = df.select(
    ...   map_from_arrays(df.k1, df.v1).alias("m1"), map_from_arrays(df.k2, df.v2).alias("m2")
    ... )
    >>> df2.select(map_concat("m1", "m2")).collect()
    [Row(map_concat(m1, m2)={1: 'a', 2: 'c', 3: 'd'})]

    """
    cols = [parse(e) for e in exprs]
    return col(MapConcat(cols))


def from_csv(e, schema, options=None):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def schema_of_csv(csv, options=None):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(schema_of_csv("1,test,2020-05-05")).show(20, False)
    +-------------------------------------+
    |schema_of_csv(1,test,2020-05-05)     |
    +-------------------------------------+
    |struct<_c0:int,_c1:string,_c2:string>|
    +-------------------------------------+
    """
    if isinstance(csv, str):
        csv = lit(csv)
    elif not isinstance(csv, Column) or not isinstance(csv.expr, Literal):
        raise AnalysisException(
            "type mismatch: The input csv should be a string literal and not null;"
            f" however, got {csv}."
        )
    return col(SchemaOfCsv(parse(csv), options))


def to_csv(e, options=None):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def udf(f, returnType=DataType()):
    """
    :rtype: function

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(3).select(udf(lambda x: x)("id")).show()
    +------------+
    |<lambda>(id)|
    +------------+
    |           0|
    |           1|
    |           2|
    +------------+
    >>> def my_pow(a, b):
    ...   return a ** b
    ...
    >>> spark.range(3).select(udf(my_pow)("id", lit(2))).show()
    +-------------+
    |my_pow(id, 2)|
    +-------------+
    |            0|
    |            1|
    |            4|
    +-------------+


    """

    def wrapper(*args, **kwargs):
        if kwargs:
            raise TypeError(f"wrapper() got an unexpected keyword argument '{list(kwargs)}'")
        exprs = [parse(arg) for arg in args]
        return col(UserDefinedFunction(f, returnType, *exprs))

    return wrapper


def pandas_udf(f=None, returnType=None, functionType=None):
    raise NotImplementedError("Pysparkling does not support yet this function")


def callUDF(udfName, *cols):
    raise NotImplementedError("Pysparkling does not support yet this function")


__all__ = [
    'abs', 'acos', 'add_months', 'approx_count_distinct', 'array', 'array_contains',
    'array_distinct', 'array_except', 'array_intersect', 'array_join', 'array_max', 'array_min',
    'array_position', 'array_remove', 'array_repeat', 'array_sort', 'array_union', 'arrays_overlap',
    'arrays_zip', 'asc', 'asc_nulls_first', 'asc_nulls_last', 'ascii', 'asin', 'atan', 'atan2',
    'avg', 'base64', 'bin', 'bitwiseNOT', 'broadcast', 'bround', 'callUDF', 'cbrt', 'ceil',
    'coalesce', 'col', 'collect_list', 'collect_set', 'column', 'concat', 'concat_ws', 'conv',
    'corr', 'cos', 'cosh', 'count', 'countDistinct', 'covar_pop', 'covar_samp', 'crc32',
    'create_map', 'cume_dist', 'current_date', 'current_timestamp', 'date_add', 'date_format',
    'date_sub', 'date_trunc', 'datediff', 'dayofmonth', 'dayofweek', 'dayofyear', 'decode',
    'degrees', 'dense_rank', 'desc', 'desc_nulls_first', 'desc_nulls_last', 'element_at', 'encode',
    'exp', 'explode', 'explode_outer', 'expm1', 'expr', 'factorial', 'first', 'flatten', 'floor',
    'format_number', 'format_string', 'from_csv', 'from_json', 'from_unixtime',
    'from_utc_timestamp', 'get_json_object', 'greatest', 'grouping', 'grouping_id', 'hash', 'hex',
    'hour', 'hypot', 'initcap', 'input_file_name', 'instr', 'isnan', 'isnull', 'json_tuple',
    'kurtosis', 'lag', 'last', 'last_day', 'lead', 'least', 'length', 'levenshtein', 'lit',
    'locate', 'log', 'log10', 'log1p', 'log2', 'lower', 'lpad', 'ltrim', 'map_concat',
    'map_entries', 'map_from_arrays', 'map_from_entries', 'map_keys', 'map_values', 'math', 'max',
    'md5', 'mean', 'min', 'minute', 'monotonically_increasing_id', 'month', 'months_between',
    'nanvl', 'next_day', 'ntile', 'pandas_udf', 'parse', 'percent_rank', 'posexplode',
    'posexplode_outer', 'pow', 'quarter', 'radians', 'rand', 'randn', 'rank', 'regexp_extract',
    'regexp_replace', 'repeat', 'reverse', 'rint', 'round', 'row_number', 'rpad', 'rtrim',
    'schema_of_csv', 'schema_of_json', 'second', 'sequence', 'sha1', 'sha2', 'shiftLeft',
    'shiftRight', 'shiftRightUnsigned', 'shuffle', 'signum', 'sin', 'sinh', 'size', 'skewness',
    'slice', 'sort_array', 'soundex', 'spark_partition_id', 'split', 'sqrt', 'stddev', 'stddev_pop',
    'stddev_samp', 'struct', 'substring', 'substring_index', 'sum', 'sumDistinct', 'tan', 'tanh',
    'to_csv', 'to_date', 'to_json', 'to_timestamp', 'to_utc_timestamp', 'translate', 'trim',
    'trunc', 'typedLit', 'udf', 'unbase64', 'unhex', 'unix_timestamp', 'upper', 'var_pop',
    'var_samp', 'variance', 'weekofyear', 'when', 'window', 'xxhash64', 'year'
]
