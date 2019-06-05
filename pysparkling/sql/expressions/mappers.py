import math


from pysparkling.sql.expressions.expressions import Expression
from pysparkling.utils import XORShiftRandom, row_from_keyed_values


class StarOperator(Expression):
    @property
    def may_output_multiple_cols(self):
        return True

    def output_cols(self, row):
        return [col for col in row.__fields__]

    def eval(self, row):
        return (row[col] for col in row.__fields__)

    def __str__(self):
        return "*"


class UnaryExpression(Expression):
    def __init__(self, column):
        super().__init__(column)
        self.column = column

    def eval(self, row):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError


class IsNull(UnaryExpression):
    def eval(self, row):
        return self.column.eval(row) is None

    def __str__(self):
        return "({0} IS NULL)".format(self.column)


class IsNotNull(UnaryExpression):
    def eval(self, row):
        return self.column.eval(row) is not None

    def __str__(self):
        return "({0} IS NOT NULL)".format(self.column)


class Contains(Expression):
    def __init__(self, expr, value):
        super().__init__(expr, value)
        self.expr = expr
        self.value = value

    def eval(self, row):
        return self.value in self.expr.eval(row)

    def __str__(self):
        return "contains({0}, {1})".format(self.expr, self.value)


class Negate(UnaryExpression):
    def eval(self, row):
        return not self.column.eval(row)

    def __str__(self):
        return "(- {0})".format(self.column)


class Add(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) + self.arg2.eval(row)

    def __str__(self):
        return "({0} + {1})".format(self.arg1, self.arg2)


class Minus(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) - self.arg2.eval(row)

    def __str__(self):
        return "({0} - {1})".format(self.arg1, self.arg2)


class Time(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) * self.arg2.eval(row)

    def __str__(self):
        return "({0} * {1})".format(self.arg1, self.arg2)


class Divide(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) / self.arg2.eval(row)

    def __str__(self):
        return "({0} / {1})".format(self.arg1, self.arg2)


class Mod(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) % self.arg2.eval(row)

    def __str__(self):
        return "({0} % {1})".format(self.arg1, self.arg2)


class Pow(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) ** self.arg2.eval(row)

    def __str__(self):
        return "POWER({0}, {1})".format(self.arg1, self.arg2)


class Equal(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        value_1 = self.arg1.eval(row)
        value_2 = self.arg2.eval(row)
        if value_1 is None or value_2 is None:
            return None
        return value_1 == value_2

    def __str__(self):
        return "({0} = {1})".format(self.arg1, self.arg2)


class EqNullSafe(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) == self.arg2.eval(row)

    def __str__(self):
        return "({0} <=> {1})".format(self.arg1, self.arg2)


class LessThan(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) < self.arg2.eval(row)

    def __str__(self):
        return "({0} < {1})".format(self.arg1, self.arg2)


class LessThanOrEqual(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) <= self.arg2.eval(row)

    def __str__(self):
        return "({0} <= {1})".format(self.arg1, self.arg2)


class GreaterThan(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) > self.arg2.eval(row)

    def __str__(self):
        return "({0} > {1})".format(self.arg1, self.arg2)


class GreaterThanOrEqual(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) >= self.arg2.eval(row)

    def __str__(self):
        return "({0} >= {1})".format(self.arg1, self.arg2)


class And(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) and self.arg2.eval(row)

    def __str__(self):
        return "({0} AND {1})".format(self.arg1, self.arg2)


class Or(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) or self.arg2.eval(row)

    def __str__(self):
        return "({0} OR {1})".format(self.arg1, self.arg2)


class Invert(UnaryExpression):
    def eval(self, row):
        return not self.column.eval(row)

    def __str__(self):
        return "(NOT {0})".format(self.column)


class BitwiseOr(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) | self.arg2.eval(row)

    def __str__(self):
        return "({0} | {1})".format(self.arg1, self.arg2)


class BitwiseAnd(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) & self.arg2.eval(row)

    def __str__(self):
        return "({0} & {1})".format(self.arg1, self.arg2)


class BitwiseXor(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) ^ self.arg2.eval(row)

    def __str__(self):
        return "({0} ^ {1})".format(self.arg1, self.arg2)


class BitwiseNot(UnaryExpression):
    def eval(self, row):
        return ~(self.column.eval(row))

    def __str__(self):
        return "~{0}".format(self.column)


class StartsWith(Expression):
    def __init__(self, arg1, substr):
        super().__init__(arg1, substr)
        self.arg1 = arg1
        self.substr = substr

    def eval(self, row):
        return str(self.arg1.eval(row)).startswith(self.substr)

    def __str__(self):
        return "startswith({0}, {1})".format(self.arg1, self.substr)


class EndsWith(Expression):
    def __init__(self, arg1, substr):
        super().__init__(arg1, substr)
        self.arg1 = arg1
        self.substr = substr

    def eval(self, row):
        return str(self.arg1.eval(row)).endswith(self.substr)

    def __str__(self):
        return "endswith({0}, {1})".format(self.arg1, self.substr)


class Substr(Expression):
    def __init__(self, expr, start, end):
        super().__init__(expr, start, end)
        self.expr = expr
        self.start = start
        self.end = end

    def eval(self, row):
        return str(self.expr.eval(row))[self.start:self.end]

    def __str__(self):
        return "substring({0}, {1}, {2})".format(self.expr, self.start, self.end)


class IsIn(Expression):
    def __init__(self, arg1, cols):
        super().__init__(arg1, cols)
        self.arg1 = arg1
        self.cols = cols

    def eval(self, row):
        return str(self.arg1.eval(row)) in [col.eval(row) for col in self.cols]

    def __str__(self):
        return "({0} IN ({1}))".format(
            self.arg1,
            ", ".join(str(col) for col in self.cols)
        )


class Alias(Expression):
    def __init__(self, expr, alias):
        super().__init__(expr, alias)
        self.expr = expr
        self.alias = alias

    def eval(self, row):
        return self.expr.eval(row)

    def __str__(self):
        return self.alias


class ArrayColumn(Expression):
    def __init__(self, columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row):
        return [col.eval(row) for col in self.columns]

    def __str__(self):
        return "array({0})".format(", ".join(str(col) for col in self.columns))


class MapColumn(Expression):
    def __init__(self, *columns):
        super().__init__(columns)
        self.columns = columns
        self.keys = columns[::2]
        self.values = columns[1::2]

    def eval(self, row):
        return dict((key.eval(row), value.eval(row)) for key, value in zip(self.keys, self.values))

    def __str__(self):
        return "map({0})".format(", ".join(str(col) for col in self.columns))


class MapFromArraysColumn(Expression):
    def __init__(self, keys, values):
        super().__init__(keys, values)
        self.keys = keys
        self.values = values

    def eval(self, row):
        return dict(zip(self.keys.eval(row), self.values.eval(row)))

    def __str__(self):
        return "map_from_arrays({0}, {1})".format(
            self.keys,
            self.values
        )


class Coalesce(Expression):
    def __init__(self, columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row):
        for col in self.columns:
            col_value = col.eval(row)
            if col_value is not None:
                return col_value
        return None

    def __str__(self):
        return "coalesce({0})".format(", ".join(self.columns))


class IsNaN(UnaryExpression):
    def eval(self, row):
        return self.eval(row) == float("nan")

    def __str__(self):
        return "isnan({0})".format(", ".join(self.column))


class NaNvl(Expression):
    def __init__(self, col1, col2):
        super().__init__(col1, col2)
        self.col1 = col1
        self.col2 = col2

    def eval(self, row):
        nan = float("nan")
        col1_value = self.col1.eval(row)
        if col1_value != nan:
            return float(col1_value)
        return float(self.col2.eval(row))

    def __str__(self):
        return "nanvl({0}, {1})".format(self.col1, self.col2)


class Sqrt(UnaryExpression):
    def eval(self, row):
        return math.sqrt(self.column.eval(row))

    def __str__(self):
        return "SQRT({0})".format(self.column)


class Cbrt(UnaryExpression):
    def eval(self, row):
        return self.column.eval(row) ** 1./3.

    def __str__(self):
        return "CBRT({0})".format(self.column)


class Abs(UnaryExpression):
    def eval(self, row):
        return abs(self.column.eval(row))

    def __str__(self):
        return "ABS({0})".format(self.column)


class Acos(UnaryExpression):
    def eval(self, row):
        return math.acos(self.column.eval(row))

    def __str__(self):
        return "ACOS({0})".format(self.column)


class Asin(UnaryExpression):
    def eval(self, row):
        return math.asin(self.column.eval(row))

    def __str__(self):
        return "ASIN({0})".format(self.column)


class Atan(UnaryExpression):
    def eval(self, row):
        return math.atan(self.column.eval(row))

    def __str__(self):
        return "ATAN({0})".format(self.column)


class Tan(UnaryExpression):
    def eval(self, row):
        return math.tan(self.column.eval(row))

    def __str__(self):
        return "TAN({0})".format(self.column)


class Tanh(UnaryExpression):
    def eval(self, row):
        return math.tanh(self.column.eval(row))

    def __str__(self):
        return "TANH({0})".format(self.column)


class Cos(UnaryExpression):
    def eval(self, row):
        return math.cos(self.column.eval(row))

    def __str__(self):
        return "COS({0})".format(self.column)


class Cosh(UnaryExpression):
    def eval(self, row):
        return math.cosh(self.column.eval(row))

    def __str__(self):
        return "COSH({0})".format(self.column)


class Sin(UnaryExpression):
    def eval(self, row):
        return math.sin(self.column.eval(row))

    def __str__(self):
        return "SIN({0})".format(self.column)


class Sinh(UnaryExpression):
    def eval(self, row):
        return math.sinh(self.column.eval(row))

    def __str__(self):
        return "SINH({0})".format(self.column)


class Exp(UnaryExpression):
    def eval(self, row):
        return math.exp(self.column.eval(row))

    def __str__(self):
        return "EXP({0})".format(self.column)


class ExpM1(UnaryExpression):
    def eval(self, row):
        return math.expm1(self.column.eval(row))

    def __str__(self):
        return "EXPM1({0})".format(self.column)


class Factorial(UnaryExpression):
    def eval(self, row):
        return math.factorial(self.column.eval(row))

    def __str__(self):
        return "factorial({0})".format(self.column)


class Floor(UnaryExpression):
    def eval(self, row):
        return math.floor(self.column.eval(row))

    def __str__(self):
        return "FLOOR({0})".format(self.column)


class Ceil(UnaryExpression):
    def eval(self, row):
        return math.ceil(self.column.eval(row))

    def __str__(self):
        return "CEIL({0})".format(self.column)


class Log10(UnaryExpression):
    def eval(self, row):
        return math.log10(self.column.eval(row))

    def __str__(self):
        return "LOG10({0})".format(self.column)


class Log2(UnaryExpression):
    def eval(self, row):
        return math.log2(self.column.eval(row))

    def __str__(self):
        return "LOG2({0})".format(self.column)


class Log1p(UnaryExpression):
    def eval(self, row):
        return math.log1p(self.column.eval(row))

    def __str__(self):
        return "LOG1P({0})".format(self.column)


class Rint(UnaryExpression):
    def eval(self, row):
        return round(self.column.eval(row))

    def __str__(self):
        return "ROUND({0})".format(self.column)


class Signum(UnaryExpression):
    def eval(self, row):
        column_value = self.column.eval(row)
        if column_value == 0:
            return 0
        elif column_value > 0:
            return 1.0
        else:
            return -1.0

    def __str__(self):
        return "ROUND({0})".format(self.column)


class ToDegrees(UnaryExpression):
    def eval(self, row):
        return math.degrees(self.column.eval(row))

    def __str__(self):
        return "DEGREES({0})".format(self.column)


class ToRadians(UnaryExpression):
    def eval(self, row):
        return math.radians(self.column.eval(row))

    def __str__(self):
        return "RADIANS({0})".format(self.column)


class Rand(Expression):
    def __init__(self, seed=None):
        super().__init__()
        self.seed = seed
        self.random_generator = None

    def eval(self, row):
        return self.random_generator.nextDouble()

    def initialize(self, partition_index):
        self.random_generator = XORShiftRandom(self.seed + partition_index)

    def __str__(self):
        return "rand({0})".format(self.seed)


class Randn(Expression):
    def __init__(self, seed=None):
        super().__init__()
        self.seed = seed
        self.random_generator = None

    def eval(self, row):
        return self.random_generator.nextGaussian()

    def initialize(self, partition_index):
        self.random_generator = XORShiftRandom(self.seed + partition_index)

    def __str__(self):
        return "randn({0})".format(self.seed)


class SparkPartitionID(Expression):
    def __init__(self):
        super().__init__()
        self.partition_index = None

    def eval(self, row):
        return self.partition_index

    def initialize(self, partition_index):
        self.partition_index = partition_index

    def __str__(self):
        return "SPARK_PARTITION_ID()"


class CreateStruct(Expression):
    def __init__(self, columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row):
        return row_from_keyed_values([
            (str(col), col.eval(row)) for col in self.columns
        ])

    def __str__(self):
        return "named_struct({0})".format(", ".join("{0}, {0}".format(col) for col in self.columns))


class Bin(UnaryExpression):
    def eval(self, row):
        return format(self.column.eval(row), 'b')

    def __str__(self):
        return "bin({0})".format(self.column)


class Greatest(Expression):
    def __init__(self, *columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row):
        return max(col.eval(row) for col in self.columns)

    def __str__(self):
        return "greatest({0})".format(", ".join(str(col) for col in self.columns))


class Least(Expression):
    def __init__(self, *columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row):
        return min(col.eval(row) for col in self.columns)

    def __str__(self):
        return "least({0})".format(", ".join(str(col) for col in self.columns))


class Length(UnaryExpression):
    def eval(self, row):
        return len(str(self.column))

    def __str__(self):
        return "length({0})".format(self.column)


class Lower(UnaryExpression):
    def eval(self, row):
        return str(self.column).lower()

    def __str__(self):
        return "lower({0})".format(self.column)


class Upper(UnaryExpression):
    def eval(self, row):
        return str(self.column).upper()

    def __str__(self):
        return "Upper({0})".format(self.column)


class Concat(Expression):
    def __init__(self, columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row):
        return "".join(str(col.eval(row)) for col in self.columns)

    def __str__(self):
        return "concat({0})".format(", ".join(col for col in self.columns))


class Size(UnaryExpression):
    def eval(self, row):
        column_value = self.column.eval(row)
        if isinstance(column_value, (list, set, dict)):
            return len(column_value)
        raise Expression("{0} value should be a list, set or a dict, got {1}".format(self.column, type(column_value)))

    def __str__(self):
        return "size({0})".format(self.column)


class ArraySort(UnaryExpression):
    def eval(self, row):
        return sorted(self.column.eval(row))

    def __str__(self):
        return "array_sort({0})".format(self.column)


class ArrayMin(UnaryExpression):
    def eval(self, row):
        return min(self.column.eval(row))

    def __str__(self):
        return "array_min({0})".format(self.column)


class ArrayMax(UnaryExpression):
    def eval(self, row):
        return max(self.column.eval(row))

    def __str__(self):
        return "array_max({0})".format(self.column)


class Reverse(UnaryExpression):
    def eval(self, row):
        return str(self.column.eval(row))[::-1]

    def __str__(self):
        return "reverse({0})".format(self.column)


class MapKeys(UnaryExpression):
    def eval(self, row):
        return list(self.column.eval(row).keys())

    def __str__(self):
        return "map_keys({0})".format(self.column)


class MapValues(UnaryExpression):
    def eval(self, row):
        return list(self.column.eval(row).values())

    def __str__(self):
        return "map_values({0})".format(self.column)


class MapEntries(UnaryExpression):
    def eval(self, row):
        return list(self.column.eval(row).items())

    def __str__(self):
        return "map_entries({0})".format(self.column)


class MapFromEntries(UnaryExpression):
    def eval(self, row):
        return dict(self.column.eval(row))

    def __str__(self):
        return "map_from_entries({0})".format(self.column)
