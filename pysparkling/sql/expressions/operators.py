from ..casts import get_caster
from .._row import Row
from ..types import StructType
from .expressions import BinaryOperation, Expression, NullSafeBinaryOperation, TypeSafeBinaryOperation, UnaryExpression


class Negate(UnaryExpression):
    def eval(self, row, schema):
        return - self.column.eval(row, schema)

    def __str__(self):
        return f"(- {self.column})"


class Add(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 + value2

    def __str__(self):
        return f"({self.arg1} + {self.arg2})"


class Minus(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 - value2

    def __str__(self):
        return f"({self.arg1} - {self.arg2})"


class Time(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 * value2

    def __str__(self):
        return f"({self.arg1} * {self.arg2})"


class Divide(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 / value2 if value2 != 0 else None

    def __str__(self):
        return f"({self.arg1} / {self.arg2})"


class Mod(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 % value2

    def __str__(self):
        return f"({self.arg1} % {self.arg2})"


class Pow(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return float(value1 ** value2)

    def __str__(self):
        return f"POWER({self.arg1}, {self.arg2})"


class Equal(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 == value_2

    def __str__(self):
        return f"({self.arg1} = {self.arg2})"


class LessThan(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 < value_2

    def __str__(self):
        return f"({self.arg1} < {self.arg2})"


class LessThanOrEqual(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 <= value_2

    def __str__(self):
        return f"({self.arg1} <= {self.arg2})"


class GreaterThan(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 > value_2

    def __str__(self):
        return f"({self.arg1} > {self.arg2})"


class GreaterThanOrEqual(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 >= value_2

    def __str__(self):
        return f"({self.arg1} >= {self.arg2})"


class And(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 and value_2

    def __str__(self):
        return f"({self.arg1} AND {self.arg2})"


class Or(TypeSafeBinaryOperation):
    def unsafe_operation(self, value_1, value_2):
        return value_1 or value_2

    def __str__(self):
        return f"({self.arg1} OR {self.arg2})"


class Invert(UnaryExpression):
    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        if value is None:
            return None
        return not value

    def __str__(self):
        return f"(NOT {self.column})"


class BitwiseOr(BinaryOperation):
    def eval(self, row, schema):
        return self.arg1.eval(row, schema) | self.arg2.eval(row, schema)

    def __str__(self):
        return f"({self.arg1} | {self.arg2})"


class BitwiseAnd(BinaryOperation):
    def eval(self, row, schema):
        return self.arg1.eval(row, schema) & self.arg2.eval(row, schema)

    def __str__(self):
        return f"({self.arg1} & {self.arg2})"


class BitwiseXor(BinaryOperation):
    def eval(self, row, schema):
        return self.arg1.eval(row, schema) ^ self.arg2.eval(row, schema)

    def __str__(self):
        return f"({self.arg1} ^ {self.arg2})"


class BitwiseNot(UnaryExpression):
    def eval(self, row, schema):
        return ~(self.column.eval(row, schema))

    def __str__(self):
        return f"~{self.column}"


class EqNullSafe(BinaryOperation):
    def eval(self, row, schema):
        return self.arg1.eval(row, schema) == self.arg2.eval(row, schema)

    def __str__(self):
        return f"({self.arg1} <=> {self.arg2})"


class GetField(Expression):
    def __init__(self, item, field):
        super().__init__(item, field)
        self.item = item
        self.field = field

    def eval(self, row, schema):
        item_eval = self.item.eval(row, schema)
        if isinstance(item_eval, Row):
            item_value = dict(zip(
                item_eval.__fields__,
                item_eval
            ))
        elif isinstance(item_eval, dict):
            item_value = item_eval
        else:
            item_value = dict(enumerate(item_eval))
        field_value = self.field.eval(row, schema)
        return item_value.get(field_value)

    def __str__(self):
        if (hasattr(self.item.expr, "field")
                and hasattr(self.item.expr.field, "dataType")
                and isinstance(self.item.expr.field.dataType, StructType)):
            return f"{self.item}.{self.field}"
        return f"{self.item}[{self.field}]"

    def args(self):
        return (
            self.item,
            self.field
        )


class Contains(Expression):
    pretty_name = "contains"

    def __init__(self, expr, value):
        super().__init__(expr, value)
        self.expr = expr
        self.value = value

    def eval(self, row, schema):
        return self.value.eval(row, schema) in self.expr.eval(row, schema)

    def args(self):
        return (
            self.expr,
            self.value
        )


class StartsWith(Expression):
    pretty_name = "startswith"

    def __init__(self, arg1, substr):
        super().__init__(arg1, substr)
        self.arg1 = arg1
        self.substr = substr

    def eval(self, row, schema):
        return str(self.arg1.eval(row, schema)).startswith(self.substr)

    def args(self):
        return (
            self.arg1,
            self.substr
        )


class EndsWith(Expression):
    pretty_name = "endswith"

    def __init__(self, arg1, substr):
        super().__init__(arg1, substr)
        self.arg1 = arg1
        self.substr = substr

    def eval(self, row, schema):
        return str(self.arg1.eval(row, schema)).endswith(self.substr)

    def args(self):
        return (
            self.arg1,
            self.substr
        )


class IsIn(Expression):
    def __init__(self, arg1, cols):
        super().__init__(arg1)
        self.arg1 = arg1
        self.cols = [c.get_literal_value() for c in cols]

    def eval(self, row, schema):
        return self.arg1.eval(row, schema) in self.cols

    def __str__(self):
        all_cols = ', '.join(str(col) for col in self.cols)
        return f"({self.arg1} IN ({all_cols}))"

    def args(self):
        return [self.arg1] + self.cols


class IsNotNull(UnaryExpression):
    def eval(self, row, schema):
        return self.column.eval(row, schema) is not None

    def __str__(self):
        return f"({self.column} IS NOT NULL)"


class IsNull(UnaryExpression):
    def eval(self, row, schema):
        return self.column.eval(row, schema) is None

    def __str__(self):
        return f"({self.column} IS NULL)"


class Cast(Expression):
    def __init__(self, column, destination_type):
        super().__init__(column)
        self.column = column
        self.destination_type = destination_type
        self.caster = get_caster(
            from_type=self.column.data_type, to_type=destination_type, options={}
        )

    def eval(self, row, schema):
        return self.caster(self.column.eval(row, schema))

    def __str__(self):
        return str(self.column)

    def __repr__(self):
        return f"CAST({self.column} AS {self.destination_type.simpleString().upper()})"

    def args(self):
        return (
            self.column,
            self.destination_type
        )


class Substring(Expression):
    pretty_name = "substring"

    def __init__(self, expr, start, length):
        super().__init__(expr)
        self.expr = expr
        self.start = start.get_literal_value()
        self.length = length.get_literal_value()

    def eval(self, row, schema):
        return str(self.expr.eval(row, schema))[self.start - 1:self.start - 1 + self.length]

    def args(self):
        return (
            self.expr,
            self.start,
            self.length
        )


class Alias(Expression):
    def __init__(self, expr, alias):
        super().__init__(expr, alias)
        self.expr = expr
        self.alias = alias.get_literal_value()

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

    def args(self):
        return (
            self.expr,
            self.alias
        )


class UnaryPositive(UnaryExpression):
    def eval(self, row, schema):
        return self.column.eval(row, schema)

    def __str__(self):
        return f"(+ {self.column})"


__all__ = [
    "Negate",
    "Add",
    "Minus",
    "Time",
    "Divide",
    "Mod",
    "Pow",
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
    "Alias",
    "UnaryPositive",
]
