import base64
import math
import random
import re
import string

from pysparkling.sql.types import StringType

from pysparkling.sql.internal_utils.column import resolve_column
from pysparkling.sql.expressions.expressions import Expression, UnaryExpression, \
    NullSafeColumnOperation
from pysparkling.sql.utils import AnalysisException
from pysparkling.utils import XORShiftRandom, row_from_keyed_values, \
    MonotonicallyIncreasingIDGenerator, half_even_round, half_up_round


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
    def __init__(self, condition, function):
        super(CaseWhen).__init__(condition, function)

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
            true = condition.eval(row, schema)
            if true:
                return function
        if self.function_b is not None:
            return self.function_b
        return None

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
        super(RegExpExtract).__init__(e, exp, groupIdx)

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
        super(RegExpReplace).__init__(e, exp, replacement)

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
        super(Round).__init__(column)
        self.scale = scale

    def unsafe_operation(self, value):
        return half_up_round(value, self.scale)

    def __str__(self):
        return "round({0}, {1})".format(self.column, self.scale)


class Bround(NullSafeColumnOperation):
    def __init__(self, column, scale):
        super(Bround).__init__(column)
        self.scale = scale

    def unsafe_operation(self, value):
        return half_even_round(value, self.scale)

    def __str__(self):
        return "bround({0}, {1})".format(self.column, self.scale)


class FormatNumber(Expression):
    def __init__(self, column, digits):
        super(FormatNumber).__init__(column)
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
        super(SubstringIndex).__init__(column)
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
        super(Coalesce).__init__(columns)
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
        super(NaNvl).__init__(col1, col2)
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
        super(Hypot).__init__(a, b)
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
        super(Atan2).__init__(y, x)
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
        super(Log).__init__(base, value)
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
        if column_value > 0:
            return 1.0
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
        super(Rand).__init__()
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
        super(Randn).__init__()
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
        super(SparkPartitionID).__init__()
        self.partition_index = None

    def eval(self, row, schema):
        return self.partition_index

    def initialize(self, partition_index):
        self.partition_index = partition_index

    def __str__(self):
        return "SPARK_PARTITION_ID()"


class CreateStruct(Expression):
    def __init__(self, columns):
        super(CreateStruct).__init__(columns)
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
        super(Greatest).__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        return max(col.eval(row, schema) for col in self.columns)

    def __str__(self):
        return "greatest({0})".format(", ".join(str(col) for col in self.columns))


class Least(Expression):
    def __init__(self, *columns):
        super(Least).__init__(columns)
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
        super(Concat).__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        return "".join(str(col.eval(row, schema)) for col in self.columns)

    def __str__(self):
        return "concat({0})".format(", ".join(str(col) for col in self.columns))


class ConcatWs(Expression):
    def __init__(self, sep, columns):
        super(ConcatWs).__init__(columns)
        self.sep = sep
        self.columns = columns

    def eval(self, row, schema):
        return self.sep.join(str(col.eval(row, schema)) for col in self.columns)

    def __str__(self):
        return "concat_ws({0}{1})".format(
            self.sep,
            ", {0}".format(", ".join(str(col) for col in self.columns)) if self.columns else ""
        )


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


class MapConcat(Expression):
    def __init__(self, columns):
        super(MapConcat).__init__(*columns)
        self.columns = columns

    def eval(self, row, schema):
        result = {}
        for column in self.columns:
            col_value = column.eval(row, schema)
            if isinstance(col_value, dict):
                result.update(col_value)
        return result

    def __str__(self):
        return "map_concat({0})".format(", ".join(str(col) for col in self.columns))


class StringSplit(Expression):
    def __init__(self, column, regex, limit):
        super(StringSplit).__init__(column)
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


class Conv(Expression):
    def __init__(self, column, from_base, to_base):
        super(Conv).__init__(column)
        self.column = column
        self.from_base = from_base
        self.to_base = to_base

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        return self.convert(
            value,
            self.from_base,
            abs(self.to_base),
            positive_only=self.to_base > 0
        )

    def __str__(self):
        return "conv({0}, {1}, {2})".format(
            self.column,
            self.from_base,
            self.to_base
        )

    @staticmethod
    def convert(from_string, from_base, to_base, positive_only=False):
        """
        from_string: from number as a string
        from_base: from base
        raw_to_base: to base

        Convert a string representation of a number in base from_base to base raw_to_base

        Both base absolute values must be between 2 and 36
        otherwise the function returns None.

        from_base must be positive
        If to_base is

        >>> Conv.convert("1248", 10, 10)
        '1248'
        >>> Conv.convert("1548", 10, 2)
        '11000001100'
        >>> Conv.convert("44953", 10, 36)
        'YOP'
        >>> Conv.convert("YOP", 36, 10)
        '44953'
        >>> Conv.convert("1234", 5, 10)
        '194'
        >>> Conv.convert("-1", 36, 10)
        '-1'
        >>> Conv.convert("-1", 36, 10, positive_only=True)
        '18446744073709551615'
        >>> Conv.convert("YOP", 1, 10)  # returns None if from_base < 2
        >>> Conv.convert("YOP", 40, 10)  # returns None if from_base > 36
        >>> Conv.convert("YOP", 36, 40)  # returns None if to_base > 36
        >>> Conv.convert("YOP", 36, 0)  # returns None if to_base < 2
        >>> Conv.convert("YOP", 10, 2)  # returns None if value is not in the from_base
        """
        if (not (2 <= from_base <= 36 and 2 <= to_base <= 36)
                or from_string is None
                or not from_string):
            return None

        if from_string.startswith("-"):
            value_is_negative = True
            from_numbers = from_string[1:]
        else:
            value_is_negative = False
            from_numbers = from_string

        digits = string.digits + string.ascii_uppercase
        if not set(digits[:from_base]).issuperset(set(from_numbers)):
            return None

        value = sum(
            digits.index(digit) * (from_base ** i)
            for i, digit in enumerate(from_numbers[::-1])
        )

        if value_is_negative and positive_only:
            value = 2 ** 64 - value

        returned_string = ""
        for exp in range(int(math.log(value, to_base)) + 1, -1, -1):
            factor = (to_base ** exp)
            number = value // factor
            value -= number * factor
            returned_string += digits[number]

        if returned_string:
            returned_string = returned_string.lstrip("0")

        if value_is_negative and not positive_only:
            returned_string = "-" + returned_string

        return returned_string


class Hex(UnaryExpression):
    def eval(self, row, schema):
        return Conv.convert(
            self.column.eval(row, schema),
            from_base=10,
            to_base=16,
            positive_only=True
        )

    def __str__(self):
        return "hex({0})".format(self.column)


class Unhex(UnaryExpression):
    def eval(self, row, schema):
        return Conv.convert(
            self.column.eval(row, schema),
            from_base=16,
            to_base=10,
            positive_only=True
        )

    def __str__(self):
        return "unhex({0})".format(self.column)


class Ascii(UnaryExpression):
    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        if value is None:
            return None
        value_as_string = str(value)
        if not value_as_string:
            return None
        return ord(value_as_string[0])

    def __str__(self):
        return "ascii({0})".format(self.column)


class MonotonicallyIncreasingID(Expression):
    def __init__(self):
        super(MonotonicallyIncreasingID).__init__()
        self.generator = None

    def eval(self, row, schema):
        return self.generator.next()

    def initialize(self, partition_index):
        self.generator = MonotonicallyIncreasingIDGenerator(partition_index)

    def __str__(self):
        return "monotonically_increasing_id()"


class Base64(UnaryExpression):
    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        encoded = base64.b64encode(bytes(value, encoding="utf-8"))
        return str(encoded)[2:-1]

    def __str__(self):
        return "base64({0})".format(self.column)


class UnBase64(UnaryExpression):
    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        return bytearray(base64.b64decode(value))

    def __str__(self):
        return "unbase64({0})".format(self.column)


class GroupingID(Expression):
    def __init__(self, columns):
        super(GroupingID).__init__(*columns)
        self.columns = columns

    def eval(self, row, schema):
        metadata = row.get_metadata()
        if metadata is None or "grouping" not in metadata:
            raise AnalysisException("grouping_id() can only be used with GroupingSets/Cube/Rollup")
        id_binary_string_value = "".join(
            "1" if grouping else "0" for grouping in metadata["grouping"]
        )
        return int(id_binary_string_value, 2)

    def __str__(self):
        return "grouping_id({0})".format(
            ", ".join(str(col) for col in self.columns)
        )


class Grouping(UnaryExpression):
    def eval(self, row, schema):
        metadata = row.get_metadata()
        if metadata is None or "grouping" not in metadata:
            raise AnalysisException("grouping_id() can only be used with GroupingSets/Cube/Rollup")
        pos = self.column.find_position_in_schema(schema)
        return int(metadata["grouping"][pos])

    def __str__(self):
        return "grouping({0})".format(self.column)


__all__ = [
    "Grouping", "GroupingID", "Coalesce", "IsNaN", "MonotonicallyIncreasingID", "NaNvl", "Rand",
    "Randn", "SparkPartitionID", "Sqrt", "CreateStruct", "CaseWhen", "Abs", "Acos", "Asin",
    "Atan", "Atan2", "Bin", "Cbrt", "Ceil", "Conv", "Cos", "Cosh", "Exp", "ExpM1", "Factorial",
    "Floor", "Greatest", "Hex", "Unhex", "Hypot", "Least", "Log", "Log10", "Log1p", "Log2",
    "Rint", "Round", "Bround", "Signum", "Sin", "Sinh", "Tan", "Tanh", "ToDegrees",
    "ToRadians", "Ascii", "Base64", "ConcatWs", "FormatNumber", "Length", "Lower",
    "RegExpExtract", "RegExpReplace", "UnBase64", "StringSplit", "SubstringIndex", "Upper",
    "Concat", "Reverse", "MapKeys", "MapValues", "MapEntries", "MapFromEntries",
    "MapConcat", "StarOperator"
]
