from pysparkling.sql.expressions.literals import Literal
from pysparkling.sql.expressions.mappers import StarOperator
from pysparkling.sql.expressions.operators import Negate, Add, Minus, Time, Divide, Mod, Pow, Equal, LessThan, \
    LessThanOrEqual, GreaterThanOrEqual, GreaterThan, EqNullSafe, And, Or, Invert


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
