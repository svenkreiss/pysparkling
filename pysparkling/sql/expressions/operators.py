from pysparkling.sql.casts import get_caster
from pysparkling.sql.expressions.expressions import Expression, UnaryExpression, \
    NullSafeBinaryOperation, TypeSafeBinaryOperation
from pysparkling.sql.types import StructType, MapType


class Negate(UnaryExpression):
    def eval(self, row, schema):
        return not self.column.eval(row, schema)

    def __str__(self):
        return "(- {0})".format(self.column)


class Add(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 + value2

    def __str__(self):
        return "({0} + {1})".format(self.arg1, self.arg2)


class Minus(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 - value2

    def __str__(self):
        return "({0} - {1})".format(self.arg1, self.arg2)


class Time(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 * value2

    def __str__(self):
        return "({0} * {1})".format(self.arg1, self.arg2)


class Divide(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 / value2 if value2 != 0 else None

    def __str__(self):
        return "({0} / {1})".format(self.arg1, self.arg2)


class Mod(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 % value2

    def __str__(self):
        return "({0} % {1})".format(self.arg1, self.arg2)


class Pow(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return float(value1 ** value2)

    def __str__(self):
        return "POWER({0}, {1})".format(self.arg1, self.arg2)


class Pmod(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 % value2

    def __str__(self):
        return "pmod({0} % {1})".format(self.arg1, self.arg2)


class Equal(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 == value_2

    def __str__(self):
        return "({0} = {1})".format(self.arg1, self.arg2)


class LessThan(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 < value_2

    def __str__(self):
        return "({0} < {1})".format(self.arg1, self.arg2)


class LessThanOrEqual(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 <= value_2

    def __str__(self):
        return "({0} <= {1})".format(self.arg1, self.arg2)


class GreaterThan(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 > value_2

    def __str__(self):
        return "({0} > {1})".format(self.arg1, self.arg2)


class GreaterThanOrEqual(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 >= value_2

    def __str__(self):
        return "({0} >= {1})".format(self.arg1, self.arg2)


class And(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 and value_2

    def __str__(self):
        return "({0} AND {1})".format(self.arg1, self.arg2)


class Or(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
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


class EqNullSafe(Expression):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) == self.arg2.eval(row, schema)

    def __str__(self):
        return "({0} <=> {1})".format(self.arg1, self.arg2)


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


class Contains(Expression):
    def __init__(self, expr, value):
        super().__init__(expr, value)
        self.expr = expr
        self.value = value

    def eval(self, row, schema):
        return self.value in self.expr.eval(row, schema)

    def __str__(self):
        return "contains({0}, {1})".format(self.expr, self.value)


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


class IsNull(UnaryExpression):
    def eval(self, row, schema):
        return self.column.eval(row, schema) is None

    def __str__(self):
        return "({0} IS NULL)".format(self.column)


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


__all__ = [
    "Negate",
    "Add",
    "Minus",
    "Time",
    "Divide",
    "Mod",
    "Pow",
    "Pmod",
    "Equal",
    "LessThan",
    "LessThanOrEqual",
    "GreaterThan",
    "GreaterThanOrEqual",
    "And",
    "Or",
    "Invert",
    "BitwiseOr",
    "BitwiseAnd",
    "BitwiseXor",
    "BitwiseNot",
    "EqNullSafe",
    "GetField",
    "Contains",
    "StartsWith",
    "EndsWith",
    "IsIn",
    "IsNotNull",
    "Cast",
    "Substring",
    "IsNull",
    "Alias"
]
