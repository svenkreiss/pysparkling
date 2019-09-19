import math
import random

from pysparkling.sql.types import StructType, MapType, INTERNAL_TYPE_ORDER, python_to_spark_type, NumericType

from pysparkling.sql.casts import get_caster
from pysparkling.sql.internal_utils.column import resolve_column
from pysparkling.sql.expressions.expressions import Expression, UnaryExpression
from pysparkling.sql.utils import AnalysisException
from pysparkling.utils import XORShiftRandom, row_from_keyed_values


class BinaryOperation(Expression):
    """
    Perform a binary operation but return None if any value is None
    """

    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError

    def safe_eval(self, value_1, value_2):
        raise NotImplementedError


class TypeSafeBinaryOperation(BinaryOperation):
    """
    Perform a type and null-safe binary operation using *comparison* type cast rules:

    It converts values if they are of different types following PySpark rules:

    lit(datetime.date(2019, 1, 1))==lit("2019-01-01") is True
    """

    def eval(self, row, schema):
        value_1 = self.arg1.eval(row, schema)
        value_2 = self.arg2.eval(row, schema)
        if value_1 is None or value_2 is None:
            return None

        type_1 = value_1.__class__
        type_2 = value_2.__class__
        if type_1 == type_2:
            return self.safe_eval(value_1, value_2)

        order_1 = INTERNAL_TYPE_ORDER.index(type_1)
        order_2 = INTERNAL_TYPE_ORDER.index(type_2)
        spark_type_1 = python_to_spark_type(type_1)
        spark_type_2 = python_to_spark_type(type_2)

        if order_1 > order_2:
            caster = get_caster(from_type=spark_type_2, to_type=spark_type_1)
            value_2 = caster(value_2)
        elif order_1 < order_2:
            caster = get_caster(from_type=spark_type_1, to_type=spark_type_2)
            value_1 = caster(value_1)

        return self.safe_eval(value_1, value_2)

    def __str__(self):
        raise NotImplementedError

    def safe_eval(self, value_1, value_2):
        raise NotImplementedError


class NullSafeBinaryOperation(BinaryOperation):
    """
    Perform a null-safe binary operation

    It does not converts values if they are of different types:
    lit(datetime.date(2019, 1, 1)) - lit("2019-01-01") raises an error
    """

    def eval(self, row, schema):
        value_1 = self.arg1.eval(row, schema)
        value_2 = self.arg2.eval(row, schema)
        if value_1 is None or value_2 is None:
            return None

        type_1 = value_1.__class__
        type_2 = value_2.__class__
        if type_1 == type_2 or (
                isinstance(type_1, NumericType) and
                isinstance(type_2, NumericType)
        ):
            return self.safe_eval(value_1, value_2)

        raise AnalysisException(
            "Cannot resolve {0} due to data type mismatch, first value is {1}, second value is {2}."
            "".format(self, type_1, type_2)
        )

    def __str__(self):
        raise NotImplementedError

    def safe_eval(self, value_1, value_2):
        raise NotImplementedError


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


class GetField(Expression):
    def __init__(self, item, field):
        super().__init__(item, field)
        self.item = item
        self.field = field

    def eval(self, row, schema):
        try:
            idx = schema.names.index(self.item.col_name)
            if isinstance(schema.fields[idx].dataType, StructType):
                item_value = dict(list(zip(schema.fields[idx].dataType.names, self.item.eval(row, schema))))
            elif isinstance(schema.fields[idx].dataType, MapType):
                item_value = self.item.eval(row, schema)
            else:
                item_value = dict(enumerate(self.item.eval(row, schema)))
        except ValueError:
            item_value = self.item.eval(row, schema)
            pass
        field_value = self.field.eval(row, schema)
        return item_value.get(field_value)

    def __str__(self):
        if isinstance(self.item.expr.field.dataType, StructType):
            return "{0}.{1}".format(self.item, self.field)
        return "{0}[{1}]".format(self.item, self.field)


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


class Cast(Expression):
    def __init__(self, column, destination_type):
        super().__init__(column)
        self.column = column
        self.destination_type = destination_type
        self.caster = get_caster(from_type=self.column.data_type, to_type=destination_type)

    def eval(self, row, schema):
        return self.caster(self.column.eval(row, schema))

    def __str__(self):
        return "{0}".format(self.column)


class CaseWhen(Expression):
    def __init__(self, condition, function):
        super().__init__(condition, function)

        self.conditions = [condition]
        self.functions = [function]
        self.function_b = None

    def otherwise(self, function):
        self.function_b = function
        return self

    def when(self, condition, function):
        self.conditions.append(condition)
        self.functions.append(function)
        return self

    def eval(self, row, schema):
        for condition, function in zip(self.conditions, self.functions):
            trueth = condition.eval(row, schema)
            if trueth:
                return function
                # self.function.eval(row, schema)
        if self.function_b is not None:
            return self.function_b
            # self.function_b.eval(row, schema)

    def __str__(self):
        return "CASE {0}{1} END".format(
            " ".join(
                "WHEN {0} THEN {1}".format(condition, function)
                for condition, function in zip(self.conditions, self.functions)
            ),
            " ELSE {}".format(self.function_b) if self.function_b is not None else ""
        )


class RegExpExtract(Expression):
    def __init__(self, e, exp, groupIdx):
        super().__init__(e, exp, groupIdx)

        import re
        regexp = re.compile(exp)

        def fn(x):
            match = regexp.search(x)
            ret = match.group(groupIdx)
            return ret

        self.fn = fn
        self.exp = exp
        self.groupIdx = groupIdx
        self.e = e

    def eval(self, row, schema):
        return self.fn(self.e.eval(row, schema))

    def __str__(self):
        return "regexp_extract({0}, {1}, {2})".format(self.e, self.exp, self.groupIdx)


class RegExpReplace(Expression):
    def __init__(self, e, exp, replacement):
        super().__init__(e, exp, replacement)

        import re
        regexp = re.compile(exp)

        def fn(x):
            return regexp.sub(replacement, x)

        self.fn = fn
        self.exp = exp
        self.replacement = replacement
        self.e = e

    def eval(self, row, schema):
        return self.fn(self.e.eval(row, schema))

    def __str__(self):
        return "regexp_replace({0}, {1}, {2})".format(self.e, self.exp, self.replacement)


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
        return float(self.arg1.eval(row, schema) ** self.arg2.eval(row, schema))

    def __str__(self):
        return "POWER({0}, {1})".format(self.arg1, self.arg2)


class EqNullSafe(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) == self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} <=> {1})".format(self.arg1, self.arg2)


class Equal(TypeSafeBinaryOperation):
    def safe_eval(self, value_1, value_2):
        return value_1 == value_2

    def __str__(self):
        return "({0} = {1})".format(self.arg1, self.arg2)


class LessThan(TypeSafeBinaryOperation):
    def safe_eval(self, value_1, value_2):
        return value_1 < value_2

    def __str__(self):
        return "({0} < {1})".format(self.arg1, self.arg2)


class LessThanOrEqual(TypeSafeBinaryOperation):
    def safe_eval(self, value_1, value_2):
        return value_1 <= value_2

    def __str__(self):
        return "({0} <= {1})".format(self.arg1, self.arg2)


class GreaterThan(TypeSafeBinaryOperation):
    def safe_eval(self, value_1, value_2):
        return value_1 > value_2

    def __str__(self):
        return "({0} > {1})".format(self.arg1, self.arg2)


class GreaterThanOrEqual(TypeSafeBinaryOperation):
    def safe_eval(self, value_1, value_2):
        return value_1 >= value_2

    def __str__(self):
        return "({0} >= {1})".format(self.arg1, self.arg2)


class And(TypeSafeBinaryOperation):
    def safe_eval(self, value_1, value_2):
        return value_1 and value_2

    def __str__(self):
        return "({0} AND {1})".format(self.arg1, self.arg2)


class Or(TypeSafeBinaryOperation):
    def safe_eval(self, value_1, value_2):
        return value_1 or value_2

    def __str__(self):
        return "({0} OR {1})".format(self.arg1, self.arg2)


class Invert(UnaryExpression):
    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        if value is None:
            return None
        return not value

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


class Substring(Expression):
    def __init__(self, expr, start, length):
        super().__init__(expr)
        self.expr = expr
        self.start = start
        self.length = length

    def eval(self, row, schema):
        return str(self.expr.eval(row, schema))[self.start - 1:self.start - 1 + self.length]

    def __str__(self):
        return "substring({0}, {1}, {2})".format(self.expr, self.start, self.length)


class IsIn(Expression):
    def __init__(self, arg1, cols):
        super().__init__(arg1)
        self.arg1 = arg1
        self.cols = cols

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) in self.cols

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

    @property
    def is_an_aggregation(self):
        return self.expr.is_an_aggregation

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
        return self.column.eval(row, schema) ** 1. / 3.

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
        return "SIGNUM({0})".format(self.column)


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
        self.seed = seed if seed is not None else random.random()
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
        struct_cols, struct_values = [], []
        for col in self.columns:
            output_cols, output_values = resolve_column(col, row, schema, allow_generator=False)
            struct_cols += output_cols
            struct_values += output_values[0]
        return row_from_keyed_values(zip(struct_cols, struct_values))

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
