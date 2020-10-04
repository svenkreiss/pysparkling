import math
import random
import re

from pysparkling.sql.expressions.expressions import Expression, NullSafeColumnOperation, UnaryExpression
from pysparkling.sql.internal_utils.column import resolve_column
from pysparkling.sql.types import create_row
from pysparkling.utils import XORShiftRandom, half_up_round, half_even_round


class StarOperator(Expression):
    @property
    def may_output_multiple_cols(self):
        return True

    def output_fields(self, schema):
        return schema.fields

    def eval(self, row, schema):
        return [row[col] for col in row.__fields__]

    def __str__(self):
        return "*"


class CaseWhen(Expression):
    def __init__(self, conditions, values):
        super(CaseWhen, self).__init__(conditions, values)

        self.conditions = conditions
        self.values = values

    def eval(self, row, schema):
        for condition, function in zip(self.conditions, self.values):
            condition_value = condition.eval(row, schema)
            if condition_value:
                return function.eval(row, schema)
        return None

    def __str__(self):
        return "CASE {0} END".format(
            " ".join(
                "WHEN {0} THEN {1}".format(condition, value)
                for condition, value in zip(self.conditions, self.values)
            )
        )

    def add_when(self, condition, value):
        return CaseWhen(
            self.conditions + [condition],
            self.values + [value]
        )

    def set_otherwise(self, default):
        return Otherwise(
            self.conditions,
            self.values,
            default
        )


class Otherwise(Expression):
    def __init__(self, conditions, values, default):
        super(Otherwise, self).__init__(conditions, values, default)

        self.conditions = conditions
        self.values = values
        self.default = default

    def eval(self, row, schema):
        for condition, function in zip(self.conditions, self.values):
            condition_value = condition.eval(row, schema)
            if condition_value:
                return function.eval(row, schema)
        if self.default is not None:
            return self.default.eval(row, schema)
        return None

    def __str__(self):
        return "CASE {0} ELSE {1} END".format(
            " ".join(
                "WHEN {0} THEN {1}".format(condition, value)
                for condition, value in zip(self.conditions, self.values)
            ),
            self.default
        )


class RegExpExtract(Expression):
    def __init__(self, e, exp, groupIdx):
        super(RegExpExtract, self).__init__(e, exp, groupIdx)

        regexp = re.compile(exp)

        def fn(x):
            match = regexp.search(x)
            if not match:
                return ""
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
        super(RegExpReplace, self).__init__(e, exp, replacement)

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


class Round(NullSafeColumnOperation):
    def __init__(self, column, scale):
        super(Round, self).__init__(column)
        self.scale = scale

    def unsafe_operation(self, value):
        return half_up_round(value, self.scale)

    def __str__(self):
        return "round({0}, {1})".format(self.column, self.scale)


class Bround(NullSafeColumnOperation):
    def __init__(self, column, scale):
        super(Bround, self).__init__(column)
        self.scale = scale

    def unsafe_operation(self, value):
        return half_even_round(value, self.scale)

    def __str__(self):
        return "bround({0}, {1})".format(self.column, self.scale)


class FormatNumber(Expression):
    def __init__(self, column, digits):
        super(FormatNumber, self).__init__(column)
        self.column = column
        self.digits = digits

    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        if self.digits < 0:
            return None
        if not isinstance(value, (int, float)):
            return None
        rounded_value = half_even_round(value, self.digits)
        return "{0:,}".format(rounded_value)

    def __str__(self):
        return "format_number({0}, {1})".format(self.column, self.digits)


class SubstringIndex(Expression):
    def __init__(self, column, delim, count):
        super(SubstringIndex, self).__init__(column)
        self.column = column
        self.delim = delim
        self.count = count

    def eval(self, row, schema):
        parts = str(self.column.eval(row, schema)).split(self.delim)
        return self.delim.join(parts[:self.count] if self.count > 0 else parts[self.count:])

    def __str__(self):
        return "substring_index({0}, {1}, {2})".format(self.column, self.delim, self.count)


class Coalesce(Expression):
    def __init__(self, columns):
        super(Coalesce, self).__init__(columns)
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
        super(NaNvl, self).__init__(col1, col2)
        self.col1 = col1
        self.col2 = col2

    def eval(self, row, schema):
        nan = float("nan")
        col1_value = self.col1.eval(row, schema)
        if col1_value is not nan:
            return float(col1_value)
        return float(self.col2.eval(row, schema))

    def __str__(self):
        return "nanvl({0}, {1})".format(self.col1, self.col2)


class Hypot(Expression):
    def __init__(self, a, b):
        super(Hypot, self).__init__(a, b)
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


class Rand(Expression):
    def __init__(self, seed=None):
        super(Rand, self).__init__()
        self.seed = seed if seed is not None else random.random()
        self.random_generator = None

    def eval(self, row, schema):
        return self.random_generator.nextDouble()

    def initialize(self, partition_index):
        self.random_generator = XORShiftRandom(self.seed + partition_index)

    def __str__(self):
        return "rand({0})".format(self.seed)


class CreateStruct(Expression):
    def __init__(self, columns):
        super(CreateStruct, self).__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        struct_cols, struct_values = [], []
        for col in self.columns:
            output_cols, output_values = resolve_column(col, row, schema, allow_generator=False)
            struct_cols += output_cols
            struct_values += output_values[0]
        return create_row(struct_cols, struct_values)

    def __str__(self):
        return "named_struct({0})".format(", ".join("{0}, {0}".format(col) for col in self.columns))
