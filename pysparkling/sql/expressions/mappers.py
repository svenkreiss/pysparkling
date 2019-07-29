import math


from pysparkling.sql.expressions.expressions import Expression, UnaryExpression
from pysparkling.utils import XORShiftRandom, row_from_keyed_values


class StarOperator(Expression):
    @property
    def may_output_multiple_cols(self):
        return True

    def output_fields(self, schema):
        return [field for field in schema.fields]

    def eval(self, row, schema):
        return [row[col] for col in row.__fields__]

    def __str__(self):
        return "*"


class IsNull(UnaryExpression):
    def eval(self, row, schema):
        return self.column.eval(row, schema) is None

    def __str__(self):
        return "({0} IS NULL)".format(self.column)


class IsNotNull(UnaryExpression):
    def eval(self, row, schema):
        return self.column.eval(row, schema) is not None

    def __str__(self):
        return "({0} IS NOT NULL)".format(self.column)


class Contains(Expression):
    def __init__(self, expr, value):
        super().__init__(expr, value)
        self.expr = expr
        self.value = value

    def eval(self, row, schema):
        return self.value in self.expr.eval(row, schema)

    def __str__(self):
        return "contains({0}, {1})".format(self.expr, self.value)


class Negate(UnaryExpression):
    def eval(self, row, schema):
        return not self.column.eval(row, schema)

    def __str__(self):
        return "(- {0})".format(self.column)


class Add(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) + self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} + {1})".format(self.arg1, self.arg2)


class Minus(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) - self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} - {1})".format(self.arg1, self.arg2)


class Time(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) * self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} * {1})".format(self.arg1, self.arg2)


class Divide(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) / self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} / {1})".format(self.arg1, self.arg2)


class Mod(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) % self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} % {1})".format(self.arg1, self.arg2)


class Pow(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) ** self.arg2.eval(row, schema)

    def __str__(self):
        return "POWER({0}, {1})".format(self.arg1, self.arg2)


class Equal(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        value_1 = self.arg1.eval(row, schema)
        value_2 = self.arg2.eval(row, schema)
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

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) == self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} <=> {1})".format(self.arg1, self.arg2)


class LessThan(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) < self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} < {1})".format(self.arg1, self.arg2)


class LessThanOrEqual(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) <= self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} <= {1})".format(self.arg1, self.arg2)


class GreaterThan(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) > self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} > {1})".format(self.arg1, self.arg2)


class GreaterThanOrEqual(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) >= self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} >= {1})".format(self.arg1, self.arg2)


class And(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) and self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} AND {1})".format(self.arg1, self.arg2)


class Or(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) or self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} OR {1})".format(self.arg1, self.arg2)


class Invert(UnaryExpression):
    def eval(self, row, schema):
        return not self.column.eval(row, schema)

    def __str__(self):
        return "(NOT {0})".format(self.column)


class BitwiseOr(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) | self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} | {1})".format(self.arg1, self.arg2)


class BitwiseAnd(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) & self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} & {1})".format(self.arg1, self.arg2)


class BitwiseXor(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) ^ self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} ^ {1})".format(self.arg1, self.arg2)


class BitwiseNot(UnaryExpression):
    def eval(self, row, schema):
        return ~(self.column.eval(row, schema))

    def __str__(self):
        return "~{0}".format(self.column)


class StartsWith(Expression):
    def __init__(self, arg1, substr):
        super().__init__(arg1, substr)
        self.arg1 = arg1
        self.substr = substr

    def eval(self, row, schema):
        return str(self.arg1.eval(row, schema)).startswith(self.substr)

    def __str__(self):
        return "startswith({0}, {1})".format(self.arg1, self.substr)


class EndsWith(Expression):
    def __init__(self, arg1, substr):
        super().__init__(arg1, substr)
        self.arg1 = arg1
        self.substr = substr

    def eval(self, row, schema):
        return str(self.arg1.eval(row, schema)).endswith(self.substr)

    def __str__(self):
        return "endswith({0}, {1})".format(self.arg1, self.substr)


class Substr(Expression):
    def __init__(self, expr, start, end):
        super().__init__(expr, start, end)
        self.expr = expr
        self.start = start
        self.end = end

    def eval(self, row, schema):
        return str(self.expr.eval(row, schema))[self.start:self.end]

    def __str__(self):
        return "substring({0}, {1}, {2})".format(self.expr, self.start, self.end)


class IsIn(Expression):
    def __init__(self, arg1, cols):
        super().__init__(arg1, cols)
        self.arg1 = arg1
        self.cols = cols

    def eval(self, row, schema):
        return str(self.arg1.eval(row, schema)) in [col.eval(row, schema) for col in self.cols]

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

    @property
    def may_output_multiple_cols(self):
        return self.expr.may_output_multiple_cols

    @property
    def may_output_multiple_rows(self):
        return self.expr.may_output_multiple_rows

    def eval(self, row, schema):
        return self.expr.eval(row, schema)

    def __str__(self):
        return self.alias


class Coalesce(Expression):
    def __init__(self, columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        for col in self.columns:
            col_value = col.eval(row, schema)
            if col_value is not None:
                return col_value
        return None

    def __str__(self):
        return "coalesce({0})".format(", ".join(self.columns))


class IsNaN(UnaryExpression):
    def eval(self, row, schema):
        return self.eval(row, schema) == float("nan")

    def __str__(self):
        return "isnan({0})".format(", ".join(self.column))


class NaNvl(Expression):
    def __init__(self, col1, col2):
        super().__init__(col1, col2)
        self.col1 = col1
        self.col2 = col2

    def eval(self, row, schema):
        nan = float("nan")
        col1_value = self.col1.eval(row, schema)
        if col1_value != nan:
            return float(col1_value)
        return float(self.col2.eval(row, schema))

    def __str__(self):
        return "nanvl({0}, {1})".format(self.col1, self.col2)


class Hypot(Expression):
    def __init__(self, a, b):
        super().__init__(a, b)
        self.a = a
        self.b = b

    def eval(self, row, schema):
        return math.hypot(self.a, self.b)

    def __str__(self):
        return "hypot({0}, {1})".format(self.a, self.b)


class Sqrt(UnaryExpression):
    def eval(self, row, schema):
        return math.sqrt(self.column.eval(row, schema))

    def __str__(self):
        return "SQRT({0})".format(self.column)


class Cbrt(UnaryExpression):
    def eval(self, row, schema):
        return self.column.eval(row, schema) ** 1./3.

    def __str__(self):
        return "CBRT({0})".format(self.column)


class Abs(UnaryExpression):
    def eval(self, row, schema):
        return abs(self.column.eval(row, schema))

    def __str__(self):
        return "ABS({0})".format(self.column)


class Acos(UnaryExpression):
    def eval(self, row, schema):
        return math.acos(self.column.eval(row, schema))

    def __str__(self):
        return "ACOS({0})".format(self.column)


class Asin(UnaryExpression):
    def eval(self, row, schema):
        return math.asin(self.column.eval(row, schema))

    def __str__(self):
        return "ASIN({0})".format(self.column)


class Atan(UnaryExpression):
    def eval(self, row, schema):
        return math.atan(self.column.eval(row, schema))

    def __str__(self):
        return "ATAN({0})".format(self.column)


class Atan2(Expression):
    def __init__(self, y, x):
        super().__init__(y, x)
        self.y = y
        self.x = x

    def eval(self, row, schema):
        return math.atan2(self.y.eval(row, schema), self.x.eval(row, schema))

    def __str__(self):
        return "ATAN({0}, {1})".format(self.y, self.x)


class Tan(UnaryExpression):
    def eval(self, row, schema):
        return math.tan(self.column.eval(row, schema))

    def __str__(self):
        return "TAN({0})".format(self.column)


class Tanh(UnaryExpression):
    def eval(self, row, schema):
        return math.tanh(self.column.eval(row, schema))

    def __str__(self):
        return "TANH({0})".format(self.column)


class Cos(UnaryExpression):
    def eval(self, row, schema):
        return math.cos(self.column.eval(row, schema))

    def __str__(self):
        return "COS({0})".format(self.column)


class Cosh(UnaryExpression):
    def eval(self, row, schema):
        return math.cosh(self.column.eval(row, schema))

    def __str__(self):
        return "COSH({0})".format(self.column)


class Sin(UnaryExpression):
    def eval(self, row, schema):
        return math.sin(self.column.eval(row, schema))

    def __str__(self):
        return "SIN({0})".format(self.column)


class Sinh(UnaryExpression):
    def eval(self, row, schema):
        return math.sinh(self.column.eval(row, schema))

    def __str__(self):
        return "SINH({0})".format(self.column)


class Exp(UnaryExpression):
    def eval(self, row, schema):
        return math.exp(self.column.eval(row, schema))

    def __str__(self):
        return "EXP({0})".format(self.column)


class ExpM1(UnaryExpression):
    def eval(self, row, schema):
        return math.expm1(self.column.eval(row, schema))

    def __str__(self):
        return "EXPM1({0})".format(self.column)


class Factorial(UnaryExpression):
    def eval(self, row, schema):
        return math.factorial(self.column.eval(row, schema))

    def __str__(self):
        return "factorial({0})".format(self.column)


class Floor(UnaryExpression):
    def eval(self, row, schema):
        return math.floor(self.column.eval(row, schema))

    def __str__(self):
        return "FLOOR({0})".format(self.column)


class Ceil(UnaryExpression):
    def eval(self, row, schema):
        return math.ceil(self.column.eval(row, schema))

    def __str__(self):
        return "CEIL({0})".format(self.column)


class Log(Expression):
    def __init__(self, base, value):
        super().__init__(base, value)
        self.base = base
        self.value = value

    def eval(self, row, schema):
        value_eval = self.value.eval(row, schema)
        if value_eval == 0:
            return None
        return math.log(value_eval, self.base)

    def __str__(self):
        return "LOG({0}{1})".format(
            "{}, ".format(self.base) if self.base != math.e else "",
            self.value
        )


class Log10(UnaryExpression):
    def eval(self, row, schema):
        return math.log10(self.column.eval(row, schema))

    def __str__(self):
        return "LOG10({0})".format(self.column)


class Log2(UnaryExpression):
    def eval(self, row, schema):
        return math.log2(self.column.eval(row, schema))

    def __str__(self):
        return "LOG2({0})".format(self.column)


class Log1p(UnaryExpression):
    def eval(self, row, schema):
        return math.log1p(self.column.eval(row, schema))

    def __str__(self):
        return "LOG1P({0})".format(self.column)


class Rint(UnaryExpression):
    def eval(self, row, schema):
        return round(self.column.eval(row, schema))

    def __str__(self):
        return "ROUND({0})".format(self.column)


class Signum(UnaryExpression):
    def eval(self, row, schema):
        column_value = self.column.eval(row, schema)
        if column_value == 0:
            return 0
        elif column_value > 0:
            return 1.0
        else:
            return -1.0

    def __str__(self):
        return "ROUND({0})".format(self.column)


class ToDegrees(UnaryExpression):
    def eval(self, row, schema):
        return math.degrees(self.column.eval(row, schema))

    def __str__(self):
        return "DEGREES({0})".format(self.column)


class ToRadians(UnaryExpression):
    def eval(self, row, schema):
        return math.radians(self.column.eval(row, schema))

    def __str__(self):
        return "RADIANS({0})".format(self.column)


class Rand(Expression):
    def __init__(self, seed=None):
        super().__init__()
        self.seed = seed
        self.random_generator = None

    def eval(self, row, schema):
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

    def eval(self, row, schema):
        return self.random_generator.nextGaussian()

    def initialize(self, partition_index):
        self.random_generator = XORShiftRandom(self.seed + partition_index)

    def __str__(self):
        return "randn({0})".format(self.seed)


class SparkPartitionID(Expression):
    def __init__(self):
        super().__init__()
        self.partition_index = None

    def eval(self, row, schema):
        return self.partition_index

    def initialize(self, partition_index):
        self.partition_index = partition_index

    def __str__(self):
        return "SPARK_PARTITION_ID()"


class CreateStruct(Expression):
    def __init__(self, columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        return row_from_keyed_values([
            (str(col), col.eval(row, schema)) for col in self.columns
        ])

    def __str__(self):
        return "named_struct({0})".format(", ".join("{0}, {0}".format(col) for col in self.columns))


class Bin(UnaryExpression):
    def eval(self, row, schema):
        return format(self.column.eval(row, schema), 'b')

    def __str__(self):
        return "bin({0})".format(self.column)


class Greatest(Expression):
    def __init__(self, *columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        return max(col.eval(row, schema) for col in self.columns)

    def __str__(self):
        return "greatest({0})".format(", ".join(str(col) for col in self.columns))


class Least(Expression):
    def __init__(self, *columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        return min(col.eval(row, schema) for col in self.columns)

    def __str__(self):
        return "least({0})".format(", ".join(str(col) for col in self.columns))


class Length(UnaryExpression):
    def eval(self, row, schema):
        return len(str(self.column.eval(row, schema)))

    def __str__(self):
        return "length({0})".format(self.column)


class Lower(UnaryExpression):
    def eval(self, row, schema):
        return str(self.column.eval(row, schema)).lower()

    def __str__(self):
        return "lower({0})".format(self.column)


class Upper(UnaryExpression):
    def eval(self, row, schema):
        return str(self.column.eval(row, schema)).upper()

    def __str__(self):
        return "Upper({0})".format(self.column)


class Concat(Expression):
    def __init__(self, columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        return "".join(str(col.eval(row, schema)) for col in self.columns)

    def __str__(self):
        return "concat({0})".format(", ".join(col for col in self.columns))


class Reverse(UnaryExpression):
    def eval(self, row, schema):
        return str(self.column.eval(row, schema))[::-1]

    def __str__(self):
        return "reverse({0})".format(self.column)


class MapKeys(UnaryExpression):
    def eval(self, row, schema):
        return list(self.column.eval(row, schema).keys())

    def __str__(self):
        return "map_keys({0})".format(self.column)


class MapValues(UnaryExpression):
    def eval(self, row, schema):
        return list(self.column.eval(row, schema).values())

    def __str__(self):
        return "map_values({0})".format(self.column)


class MapEntries(UnaryExpression):
    def eval(self, row, schema):
        return list(self.column.eval(row, schema).items())

    def __str__(self):
        return "map_entries({0})".format(self.column)


class MapFromEntries(UnaryExpression):
    def eval(self, row, schema):
        return dict(self.column.eval(row, schema))

    def __str__(self):
        return "map_from_entries({0})".format(self.column)


class StringSplit(Expression):
    def __init__(self, column, regex, limit):
        import re
        super().__init__(column)
        self.column = column
        self.regex = regex
        self.compiled_regex = re.compile(regex)
        self.limit = limit

    def eval(self, row, schema):
        limit = self.limit if self.limit is not None else 0
        return list(self.compiled_regex.split(str(self.column.eval(row, schema)), limit))

    def __str__(self):
        return "split({0}, {1}{2})".format(
            self.column,
            self.regex,
            ", {0}".format(self.limit) if self.limit is not None else ""
        )
