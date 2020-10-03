from pysparkling.sql.column import Column, parse
from pysparkling.sql.expressions.mappers import CaseWhen
from pysparkling.sql.expressions.literals import Literal


def col(colName):
    """
    :rtype: Column
    """
    return Column(colName)


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
