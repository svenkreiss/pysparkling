import sys

from pyspark.sql.types import DataType

from pysparkling.sql.expressions.mappers import *
from pysparkling.sql.expressions.literals import Literal

if sys.version >= '3':
    basestring = str
    long = int


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

    .. versionadded:: 1.3
    """

    def __init__(self, expr):
        self.expr = expr

    # arithmetic operators
    def __neg__(self):
        return Column(Negate(self))

    def __add__(self, other):
        return Column(Add(self, parse(other)))

    def __sub__(self, other):
        return Column(Minus(self, parse(other)))

    def __mul__(self, other):
        return Column(Time(self, parse(other)))

    def __div__(self, other):
        return Column(Divide(self, parse(other)))

    def __truediv__(self, other):
        return Column(Divide(self, parse(other)))

    def __mod__(self, other):
        return Column(Mod(self, parse(other)))

    def __radd__(self, other):
        return Column(Add(parse(other), self))

    def __rsub__(self, other):
        return Column(Minus(parse(other), self))

    def __rmul__(self, other):
        return Column(Time(parse(other), self))

    def __rdiv__(self, other):
        return Column(Divide(parse(other), self))

    def __rtruediv__(self, other):
        return Column(Divide(parse(other), self))

    def __rmod__(self, other):
        return Column(Mod(parse(other), self))

    def __pow__(self, power):
        return Column(Pow(self, parse(power)))

    def __rpow__(self, power):
        return Column(Pow(parse(power), self))

    # logistic operators
    def __eq__(self, other):
        return Column(Equal(self, parse(other)))

    def __ne__(self, other):
        return Column(Negate(Equal(self, parse(other))))

    def __lt__(self, other):
        return Column(LessThan(self, parse(other)))

    def __le__(self, other):
        return Column(LessThanOrEqual(self, parse(other)))

    def __ge__(self, other):
        return Column(GreaterThanOrEqual(self, parse(other)))

    def __gt__(self, other):
        return Column(GreaterThan(self, parse(other)))

    def eqNullSafe(self, other):
        return Column(EqNullSafe(self, parse(other)))

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so use bitwise operators as boolean operators
    def __and__(self, other):
        return Column(And(self, parse(other)))

    def __or__(self, other):
        return Column(Or(self, parse(other)))

    def __invert__(self):
        return Column(Invert(self))

    def __rand__(self, other):
        return Column(And(parse(other), self))

    def __ror__(self, other):
        return Column(Or(parse(other), self))

    # container operators
    def __contains__(self, item):
        raise ValueError("Cannot apply 'in' operator against a column: please use 'contains' "
                         "in a string column or 'array_contains' function for an array column.")

    def bitwiseOR(self, other):
        return Column(BitwiseOr(self, parse(other)))

    def bitwiseAND(self, other):
        return Column(BitwiseAnd(self, parse(other)))

    def bitwiseXOR(self, other):
        return Column(BitwiseXor(self, parse(other)))

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

        >>> from pyspark.sql import Row
        >>> from pysparkling import Context
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
        sys.exit(name)
        return self[name]

    def __getattr__(self, item):
        if item.startswith("__"):
            raise AttributeError(item)
        return self.getField(item)

    def __getitem__(self, k):
        if isinstance(k, slice):
            if k.step is not None:
                raise ValueError("slice with step is not supported.")
            return self.substr(k.start, k.stop)
        else:
            return self.apply(k)

    def __iter__(self):
        raise TypeError("Column is not iterable")

    def contains(self, other):
        return Column(Contains(self, parse(other)))

    # todo: Like
    # def rlike(self, other):
    #     return Column(RegexLike(self, parse(other)))
    #
    # def like(self, other):
    #     return Column(Like(self, parse(other)))

    def startswith(self, substr):
        return Column(StartsWith(self, parse(substr)))

    def endswith(self, substr):
        return Column(EndsWith(self, parse(substr)))

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
        [Row(col=u'Ali'), Row(col=u'Bob')]
        """
        if type(startPos) != type(length):
            raise TypeError(
                "startPos and length must be the same type. "
                "Got {startPos_t} and {length_t}, respectively."
                    .format(
                    startPos_t=type(startPos),
                    length_t=type(length),
                ))
        return Column(Substr(self, parse(startPos), parse(length)))

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
        [Row(age=5, name=u'Bob')]
        >>> df[df.age.isin([1, 2, 3])].collect()
        [Row(age=2, name=u'Alice')]
        """
        if len(exprs) == 1 and isinstance(exprs[0], (list, set)):
            exprs = exprs[0]
        cols = [parse(e) for e in exprs]
        return Column(IsIn(self, cols))

    # todo: Ordering
    # def asc(self):
    #     return Column(Asc(self))
    #
    # def asc_nulls_first(self):
    #     return Column(AscNullsFirst(self))
    #
    # def asc_nulls_last(self):
    #     return Column(AscNullsLast(self))
    #
    # def desc(self):
    #     return Column(Desc(self))
    #
    # def desc_nulls_first(self):
    #     return Column(DescNullsFirst(self))
    #
    # def desc_nulls_last(self):
    #     return Column(DescNullsLast(self))

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
        >>> df.select(df.age.alias("age3", metadata={'max': 99})).schema['age3'].metadata['max']
        99
        """

        metadata = kwargs.pop('metadata', None)
        assert not kwargs, 'Unexpected kwargs where passed: %s' % kwargs

        if metadata:
            # todo: support it
            raise ValueError('Pysparkling does not support alias with metadata')

        if len(alias) == 1:
            return Column(Alias(self, alias[0]))
        else:
            # todo: support it
            raise ValueError('Pysparkling does not support multiple aliases')

    def name(self, *alias, **kwargs):
        return self.alias(*alias, **kwargs)

    def cast(self, dataType):
        """ Convert the column into type ``dataType``.

        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df.select(df.age.cast("string").alias('ages')).collect()
        [Row(ages=u'2'), Row(ages=u'5')]
        >>> df.select(df.age.cast(StringType()).alias('ages')).collect()
        [Row(ages=u'2'), Row(ages=u'5')]
        """
        if isinstance(dataType, basestring):
            jc = self._jc.cast(dataType)
        elif isinstance(dataType, DataType):
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            jdt = spark._jsparkSession.parseDataType(dataType.json())
            jc = self._jc.cast(jdt)
        else:
            raise TypeError("unexpected type: %s" % type(dataType))
        return Column(jc)

    def astype(self, dataType):
        return self.cast(dataType)

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

    def when(self, condition, value):
        """
        Evaluates a list of conditions and returns one of multiple possible result expressions.
        If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

        See :func:`pyspark.sql.functions.when` for example usage.

        :param condition: a boolean :class:`Column` expression.
        :param value: a literal value, or a :class:`Column` expression.

        >>> from pyspark.sql import functions as F
        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df.select(df.name, F.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0)).show()
        +-----+------------------------------------------------------------+
        | name|CASE WHEN (age > 4) THEN 1 WHEN (age < 3) THEN -1 ELSE 0 END|
        +-----+------------------------------------------------------------+
        |Alice|                                                          -1|
        |  Bob|                                                           1|
        +-----+------------------------------------------------------------+
        """
        if not isinstance(condition, Column):
            raise TypeError("condition should be a Column")
        v = value._jc if isinstance(value, Column) else value
        jc = self._jc.when(condition._jc, v)
        return Column(jc)

    def otherwise(self, value):
        """
        Evaluates a list of conditions and returns one of multiple possible result expressions.
        If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

        See :func:`pyspark.sql.functions.when` for example usage.

        :param value: a literal value, or a :class:`Column` expression.

        >>> from pyspark.sql import functions as F
        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df.select(df.name, F.when(df.age > 3, 1).otherwise(0)).show()
        +-----+-------------------------------------+
        | name|CASE WHEN (age > 3) THEN 1 ELSE 0 END|
        +-----+-------------------------------------+
        |Alice|                                    0|
        |  Bob|                                    1|
        +-----+-------------------------------------+
        """
        v = value._jc if isinstance(value, Column) else value
        jc = self._jc.otherwise(v)
        return Column(jc)

    def eval(self, row):
        if isinstance(self.expr, Expression):
            return self.expr.eval(row)
        try:
            return row[str(self)]
        except ValueError as e:
            raise ValueError("Unable to find the column '{0}' among {1}".format(
                str(self),
                row.__fields__
            )) from None

    @property
    def may_output_multiple_cols(self):
        if isinstance(self.expr, Expression):
            return self.expr.may_output_multiple_cols
        return False

    @property
    def may_output_multiple_rows(self):
        if isinstance(self.expr, Expression):
            return self.expr.may_output_multiple_rows
        return False

    def output_cols(self, row):
        if isinstance(self.expr, Expression):
            return self.expr.output_cols(row)
        return [str(self)]

    def merge(self, row):
        if isinstance(self.expr, Expression):
            self.expr.recursive_merge(row)
        return self

    def mergeStats(self, row):
        if isinstance(self.expr, Expression):
            self.expr.recursive_merge_stats(row)
        return self

    def __str__(self):
        return str(self.expr)

    def initialize(self, partition_index):
        if isinstance(self.expr, Expression):
            self.expr.recursive_initialize(partition_index)
        return self

    # todo: window functions
    # def over(self, window):
    #     """
    #     Define a windowing column.
    #
    #     :param window: a :class:`WindowSpec`
    #     :return: a Column
    #
    #     >>> from pyspark.sql import Window
    #     >>> window = Window.partitionBy("name").orderBy("age").rowsBetween(-1, 1)
    #     >>> from pysparkling.sql.functions import rank, min
    #     >>> # df.select(rank().over(window), min('age').over(window))
    #     """
    #     from pyspark.sql.window import WindowSpec
    #     if not isinstance(window, WindowSpec):
    #         raise TypeError("window should be WindowSpec")
    #     jc = self._jc.over(window._jspec)
    #     return Column(jc)

    def __nonzero__(self):
        raise ValueError("Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
                         "'~' for 'not' when building DataFrame boolean expressions.")

    __bool__ = __nonzero__

    def __repr__(self):
        return 'Column<%s>' % self._jc.toString().encode('utf8')


def resolve_column(col, row):
    output_cols = col.output_cols(row)

    output_values = col.eval(row)

    if col.may_output_multiple_cols:
        output_values = list(output_values)
    else:
        output_values = [output_values]

    if not col.may_output_multiple_rows:
        output_values = [[row_content] for row_content in output_values]

    return output_cols, output_values


def parse(arg):
    """
    :rtype: Column
    """
    if isinstance(arg, Column):
        return arg
    if arg == "*":
        return Column(StarOperator())
    if isinstance(arg, str) or isinstance(arg, Expression):
        return Column(arg)
    return Literal(value=arg)
