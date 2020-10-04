from pysparkling.sql.column import Column, parse
from pysparkling.sql.expressions.aggregate.collectors import CollectSet
from pysparkling.sql.expressions.aggregate.stat_aggregations import Count
from pysparkling.sql.expressions.arrays import ArrayColumn, MapFromArraysColumn
from pysparkling.sql.expressions.mappers import CaseWhen, Rand, CreateStruct
from pysparkling.sql.expressions.literals import Literal


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


def when(condition, value):
    """
    # >>> from pysparkling import Context, Row
    # >>> from pysparkling.sql.session import SparkSession
    # >>> spark = SparkSession(Context())
    # >>> df = spark.createDataFrame(
    # ...    [Row(age=2, name='Alice'), Row(age=5, name='Bob'), Row(age=4, name='Lisa')]
    # ... )
    # >>> df.select(df.name, when(df.age > 4, -1).when(df.age < 3, 1).otherwise(0)).show()
    # +-----+------------------------------------------------------------+
    # | name|CASE WHEN (age > 4) THEN -1 WHEN (age < 3) THEN 1 ELSE 0 END|
    # +-----+------------------------------------------------------------+
    # |Alice|                                                           1|
    # |  Bob|                                                          -1|
    # | Lisa|                                                           0|
    # +-----+------------------------------------------------------------+

    :rtype: Column
    """
    return col(CaseWhen([parse(condition)], [parse(value)]))


def rand(seed=None):
    """

    :rtype: Column

    # >>> from pysparkling import Context, Row
    # >>> from pysparkling.sql.session import SparkSession
    # >>> spark = SparkSession(Context())
    # >>> df = spark.range(4, numPartitions=2)
    # >>> df.select((rand(seed=42) * 3).alias("rand")).show()
    # +------------------+
    # |              rand|
    # +------------------+
    # |2.3675439190260485|
    # |1.8992753422855404|
    # |1.5878851952491426|
    # |0.8800146499990725|
    # +------------------+
    """
    return col(Rand(seed))


def struct(*exprs):
    """
    :rtype: Column

    # >>> from pysparkling import Context, Row
    # >>> from pysparkling.sql.session import SparkSession
    # >>> spark = SparkSession(Context())
    # >>> df = spark.createDataFrame([Row(age=2, name='Alice'), Row(age=5, name='Bob')])
    # >>> df.select(struct("age", col("name")).alias("struct")).collect()
    # [Row(struct=Row(age=2, name='Alice')), Row(struct=Row(age=5, name='Bob'))]
    # >>> df.select(struct("age", col("name"))).show()
    # +----------------------------------+
    # |named_struct(age, age, name, name)|
    # +----------------------------------+
    # |                        [2, Alice]|
    # |                          [5, Bob]|
    # +----------------------------------+

    """
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

    # >>> from pysparkling import Context, Row
    # >>> from pysparkling.sql.session import SparkSession
    # >>> spark = SparkSession(Context())
    # >>> df = spark.createDataFrame([([2, 5], ['a', 'b'])], ['k', 'v'])
    # >>> df.select(map_from_arrays(df.k, df.v).alias("map")).show()
    # +----------------+
    # |             map|
    # +----------------+
    # |[2 -> a, 5 -> b]|
    # +----------------+
    """
    key_col = parse(col1)
    value_col = parse(col2)
    return col(MapFromArraysColumn(key_col, value_col))


def count(e):
    """
    :rtype: Column

    # >>> from pysparkling import Context, Row
    # >>> from pysparkling.sql.session import SparkSession
    # >>> spark = SparkSession(Context())
    # >>> spark.range(5).select(count("*")).show()
    # +--------+
    # |count(1)|
    # +--------+
    # |       5|
    +--------+
    """
    return col(Count(column=parse(e)))


def collect_set(e):
    """
    :rtype: Column
    """
    return col(CollectSet(column=parse(e)))
