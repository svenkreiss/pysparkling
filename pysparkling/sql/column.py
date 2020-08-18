from pysparkling.sql.expressions.literals import Literal
from pysparkling.sql.expressions.mappers import StarOperator
from pysparkling.sql.expressions.operators import Negate, Add, Minus, Time, Divide, Mod, Pow, Equal, LessThan, \
    LessThanOrEqual, GreaterThanOrEqual, GreaterThan, EqNullSafe, And, Or, Invert, BitwiseOr, BitwiseAnd, BitwiseXor, \
    GetField, Contains, IsNull, IsNotNull, StartsWith, EndsWith, Substring, IsIn, Alias, Cast
from pysparkling.sql.expressions.orders import DescNullsLast, DescNullsFirst, Desc, AscNullsLast, AscNullsFirst, Asc
from pysparkling.sql.types import string_to_type, DataType


class Column(object):
    """
    A column in a DataFrame.

    :class:`Column` instances can be created by::

        # 1. Select a column out of a DataFrame

        df.colName
        df["colName"]

        # 2. Create from an expression
        df.colName + 1
        1 / df.colName

    """

    def __init__(self, expr):
        self.expr = expr

    # arithmetic operators
    def __neg__(self):
        return Column(Negate(self))

    def __add__(self, other):
        return Column(Add(self, parse_operator(other)))

    def __sub__(self, other):
        return Column(Minus(self, parse_operator(other)))

    def __mul__(self, other):
        return Column(Time(self, parse_operator(other)))

    def __div__(self, other):
        return Column(Divide(self, parse_operator(other)))

    def __truediv__(self, other):
        return Column(Divide(self, parse_operator(other)))

    def __mod__(self, other):
        return Column(Mod(self, parse_operator(other)))

    def __radd__(self, other):
        return Column(Add(self, parse_operator(other)))

    def __rsub__(self, other):
        return Column(Minus(parse_operator(other), self))

    def __rmul__(self, other):
        return Column(Time(parse_operator(other), self))

    def __rdiv__(self, other):
        return Column(Divide(parse_operator(other), self))

    def __rtruediv__(self, other):
        return Column(Divide(parse_operator(other), self))

    def __rmod__(self, other):
        return Column(Mod(parse_operator(other), self))

    def __pow__(self, power):
        return Column(Pow(self, parse_operator(power)))

    def __rpow__(self, power):
        return Column(Pow(parse_operator(power), self))

    # comparison operators
    def __eq__(self, other):
        return Column(Equal(self, parse_operator(other)))

    def __ne__(self, other):
        return Column(Negate(Equal(self, parse_operator(other))))

    def __lt__(self, other):
        return Column(LessThan(self, parse_operator(other)))

    def __le__(self, other):
        return Column(LessThanOrEqual(self, parse_operator(other)))

    def __ge__(self, other):
        return Column(GreaterThanOrEqual(self, parse_operator(other)))

    def __gt__(self, other):
        return Column(GreaterThan(self, parse_operator(other)))

    def between(self, lowerBound, upperBound):
        """
        A boolean expression that is evaluated to true if the value of this
        expression is between the given columns.

        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df.select(df.name, df.age.between(2, 4)).show()
        +-----+---------------------------+
        | name|((age >= 2) AND (age <= 4))|
        +-----+---------------------------+
        |Alice|                       true|
        |  Bob|                      false|
        +-----+---------------------------+
        """
        return (self >= lowerBound) & (self <= upperBound)

    def eqNullSafe(self, other):
        return Column(EqNullSafe(self, parse_operator(other)))

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so bitwise operators are used as boolean operators
    def __and__(self, other):
        return Column(And(self, parse_operator(other)))

    def __or__(self, other):
        return Column(Or(self, parse_operator(other)))

    def __invert__(self):
        return Column(Invert(self))

    def __rand__(self, other):
        return Column(And(parse_operator(other), self))

    def __ror__(self, other):
        return Column(Or(parse_operator(other), self))

    def __contains__(self, item):
        raise ValueError("Cannot apply 'in' operator against a column: please use 'contains' "
                         "in a string column or 'array_contains' function for an array column.")

    def bitwiseOR(self, other):
        return Column(BitwiseOr(self, parse_operator(other)))

    def bitwiseAND(self, other):
        return Column(BitwiseAnd(self, parse_operator(other)))

    def bitwiseXOR(self, other):
        return Column(BitwiseXor(self, parse_operator(other)))

    def getItem(self, key):
        """
        An expression that gets an item at position ``ordinal`` out of a list,
        or gets an item by key out of a dict.

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame([([1, 2], {"key": "value"})], ["l", "d"])
        >>> df.select(df.l.getItem(0), df.d.getItem("key")).show()
        +----+------+
        |l[0]|d[key]|
        +----+------+
        |   1| value|
        +----+------+
        >>> df.select(df.l[0], df.d["key"]).show()
        +----+------+
        |l[0]|d[key]|
        +----+------+
        |   1| value|
        +----+------+
        """
        return self[key]

    def getField(self, name):
        """
        An expression that gets a field by name in a StructField.

        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame([Row(r=Row(a=1, b="b"))])
        >>> df.select(df.r.getField("b")).show()
        +---+
        |r.b|
        +---+
        |  b|
        +---+
        >>> df.select(df.r.a).show()
        +---+
        |r.a|
        +---+
        |  1|
        +---+
        """
        if not isinstance(name, Column):
            name = Literal(name)
        return Column(GetField(self, name))

    def __getattr__(self, item):
        if item.startswith("__"):
            raise AttributeError(item)
        return self.getField(item)

    def __getitem__(self, k):
        if isinstance(k, slice):
            if k.step is not None:
                raise ValueError("slice with step is not supported.")
            return self.substr(k.start, k.stop)
        return self.getField(k)

    def __iter__(self):
        raise TypeError("Column is not iterable")

    def contains(self, other):
        return Column(Contains(self, parse_operator(other)))

    # pylint: disable=W0511
    # todo: Like
    def rlike(self, other):
        raise NotImplementedError("rlike is not yet implemented in pysparkling")

    def like(self, other):
        raise NotImplementedError("like is not yet implemented in pysparkling")

    def startswith(self, substr):
        return Column(StartsWith(self, parse_operator(substr)))

    def endswith(self, substr):
        return Column(EndsWith(self, parse_operator(substr)))

    def substr(self, startPos, length):
        """
        Return a :class:`Column` which is a substring of the column.

        :param startPos: start position (int or Column)
        :param length:  length of the substring (int or Column)

        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df.select(df.name.substr(1, 3).alias("col")).collect()
        [Row(col='Ali'), Row(col='Bob')]
        """
        if not isinstance(startPos, type(length)):
            raise TypeError(
                "startPos and length must be the same type. "
                "Got {0} and {1}, respectively.".format(type(startPos), type(length))
            )
        return Column(Substring(self, startPos, length))

    def isin(self, *exprs):
        """
        A boolean expression that is evaluated to true if the value of this
        expression is contained by the evaluated values of the arguments.

        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df[df.name.isin("Bob", "Mike")].collect()
        [Row(age=5, name='Bob')]
        >>> df[df.age.isin([1, 2, 3])].collect()
        [Row(age=2, name='Alice')]
        """
        if len(exprs) == 1 and isinstance(exprs[0], (list, set)):
            exprs = exprs[0]
        return Column(IsIn(self, exprs))

    def asc(self):
        """
        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import when, col
        >>> spark = SparkSession(Context())
        >>> # asc is the default order
        >>> df = spark.range(5).withColumn(
        ...   "order", when(col('id')%2 == 0, col('id'))
        ... ).orderBy("order").show()
        +---+-----+
        | id|order|
        +---+-----+
        |  1| null|
        |  3| null|
        |  0|    0|
        |  2|    2|
        |  4|    4|
        +---+-----+
        >>> df = spark.range(5).withColumn(
        ...   "order", when(col('id')%2 == 0, col('id'))
        ... ).orderBy(col("order").asc()).show()
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
        return Column(Asc(self))

    def asc_nulls_first(self):
        """
        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import when, col
        >>> spark = SparkSession(Context())
        >>> df = spark.range(5).withColumn("order",
        ...   when(col('id')%2 == 0, col('id'))
        ... ).orderBy(col("order").asc_nulls_first()).show()
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
        return Column(AscNullsFirst(self))

    def asc_nulls_last(self):
        """
        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import when, col
        >>> spark = SparkSession(Context())
        >>> df = spark.range(5).withColumn("order",
        ...   when(col('id')%2 == 0, col('id'))
        ... ).orderBy(col("order").asc_nulls_last()).show()
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
        return Column(AscNullsLast(self))

    def desc(self):
        """
        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import when, col
        >>> spark = SparkSession(Context())
        >>> df = spark.range(5).withColumn("order",
        ...   when(col('id')%2 == 0, col('id'))
        ... ).orderBy(col("order").desc()).show()
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
        return Column(Desc(self))

    def desc_nulls_first(self):
        """
        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import when, col
        >>> spark = SparkSession(Context())
        >>> df = spark.range(5).withColumn("order",
        ...   when(col('id')%2 == 0, col('id'))
        ... ).orderBy(col("order").desc_nulls_first()).show()
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
        return Column(DescNullsFirst(self))

    def desc_nulls_last(self):
        """
        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import when, col
        >>> spark = SparkSession(Context())
        >>> df = spark.range(5).withColumn("order",
        ...   when(col('id')%2 == 0, col('id'))
        ... ).orderBy(col("order").desc_nulls_last()).show()
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
        return Column(DescNullsLast(self))

    def isNull(self):
        return Column(IsNull(self))

    def isNotNull(self):
        return Column(IsNotNull(self))

    def alias(self, *alias, **kwargs):
        """
        Returns this column aliased with a new name or names (in the case of expressions that
        return more than one column, such as explode).

        :param alias: strings of desired column names (collects all positional arguments passed)
        :param metadata: a dict of information to be stored in ``metadata`` attribute of the
            corresponding :class: `StructField` (optional, keyword only argument)

        .. versionchanged:: 2.2
           Added optional ``metadata`` argument.

        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df.select(df.age.alias("age2")).collect()
        [Row(age2=2), Row(age2=5)]
        >>> from pysparkling.sql.functions import map_from_arrays, array
        >>> spark.range(3).select(map_from_arrays(array("id"), array("id"))).show()
        +-------------------------------------+
        |map_from_arrays(array(id), array(id))|
        +-------------------------------------+
        |                             [0 -> 0]|
        |                             [1 -> 1]|
        |                             [2 -> 2]|
        +-------------------------------------+

        """

        metadata = kwargs.pop('metadata', None)
        assert not kwargs, 'Unexpected kwargs where passed: %s' % kwargs

        if metadata:
            # pylint: disable=W0511
            # todo: support it
            raise ValueError('Pysparkling does not support alias with metadata')

        if len(alias) == 1:
            return Column(Alias(self, alias[0]))
        # pylint: disable=W0511
        # todo: support it
        raise ValueError('Pysparkling does not support multiple aliases')

    def name(self, *alias, **kwargs):
        return self.alias(*alias, **kwargs)

    def cast(self, dataType):
        """ Convert the column into type ``dataType``.

        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.types import StringType
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df.select(df.name, df.age.between(2, 4).alias('taapero')).collect()
        [Row(name='Alice', taapero=True), Row(name='Bob', taapero=False)]
        >>> df.select(df.age.cast("string").alias('ages')).collect()
        [Row(ages='2'), Row(ages='5')]
        >>> df.select(df.age.cast(StringType()).alias('ages')).collect()
        [Row(ages='2'), Row(ages='5')]
        >>> df.select(df.age.cast('float')).show()
        +---+
        |age|
        +---+
        |2.0|
        |5.0|
        +---+
        >>> df.select(df.age.cast('decimal(5, 0)')).show()
        +---+
        |age|
        +---+
        |  2|
        |  5|
        +---+

        """

        if isinstance(dataType, str):
            dataType = string_to_type(dataType)
        elif not isinstance(dataType, DataType):
            raise NotImplementedError("Unknown cast type: {}".format(dataType))

        return Column(Cast(self, dataType))

    def astype(self, dataType):
        return self.cast(dataType)


def parse_operator(arg):
    """
    Column operations such as df.name == "Alice" consider "Alice" as a lit, not a column

    :rtype: Column
    """
    if isinstance(arg, Column):
        return arg
    if arg == "*":
        return Column(StarOperator())
    return Literal(value=arg)
