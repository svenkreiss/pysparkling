from pysparkling.sql.expressions.literals import Literal
from pysparkling.sql.expressions.mappers import StarOperator
from pysparkling.sql.expressions.operators import Negate, Add, Minus, Time, Divide, Mod, Pow, Equal, LessThan, \
    LessThanOrEqual, GreaterThanOrEqual, GreaterThan, EqNullSafe, And, Or, Invert, BitwiseOr, BitwiseAnd, BitwiseXor, \
    GetField, Contains, IsNull, IsNotNull, StartsWith


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

    def isNull(self):
        return Column(IsNull(self))

    def isNotNull(self):
        return Column(IsNotNull(self))


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
