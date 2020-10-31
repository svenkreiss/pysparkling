import base64
import math
import random
import re
import string

from pysparkling.sql.expressions.expressions import Expression, NullSafeColumnOperation, \
    UnaryExpression
from pysparkling.sql.internal_utils.column import resolve_column
from pysparkling.sql.types import create_row, StringType
from pysparkling.sql.utils import AnalysisException
from pysparkling.utils import XORShiftRandom, half_up_round, half_even_round, \
    MonotonicallyIncreasingIDGenerator

JVM_MAX_INTEGER_SIZE = 2 ** 63


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

    def args(self):
        return ()


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

    def args(self):
        return (
            self.conditions,
            self.values
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

    def args(self):
        return (
            self.conditions,
            self.values,
            self.default
        )


class RegExpExtract(Expression):
    pretty_name = "regexp_extract"

    def __init__(self, e, exp, groupIdx):
        super(RegExpExtract, self).__init__(e, exp, groupIdx)

        self.exp = exp.get_literal_value()
        self.groupIdx = groupIdx.get_literal_value()
        self.e = e

        regexp = re.compile(self.exp)

        def fn(x):
            match = regexp.search(x)
            if not match:
                return ""
            ret = match.group(self.groupIdx)
            return ret

        self.fn = fn

    def eval(self, row, schema):
        return self.fn(self.e.eval(row, schema))

    def args(self):
        return (
            self.e,
            self.exp,
            self.groupIdx
        )


class RegExpReplace(Expression):
    pretty_name = "regexp_replace"

    def __init__(self, e, exp, replacement):
        super(RegExpReplace, self).__init__(e, exp, replacement)

        self.exp = exp.get_literal_value()
        self.replacement = replacement.get_literal_value()
        self.e = e

        regexp = re.compile(self.exp)
        self.fn = lambda x: regexp.sub(self.replacement, x)

    def eval(self, row, schema):
        return self.fn(self.e.eval(row, schema))

    def args(self):
        return (
            self.e,
            self.exp,
            self.replacement
        )


class Round(NullSafeColumnOperation):
    pretty_name = "round"

    def __init__(self, column, scale):
        super(Round, self).__init__(column)
        self.scale = scale.get_literal_value()

    def unsafe_operation(self, value):
        return half_up_round(value, self.scale)

    def args(self):
        return (
            self.column,
            self.scale
        )


class Bround(NullSafeColumnOperation):
    pretty_name = "bround"

    def __init__(self, column, scale):
        super(Bround, self).__init__(column)
        self.scale = scale.get_literal_value()

    def unsafe_operation(self, value):
        return half_even_round(value, self.scale)

    def args(self):
        return (
            self.column,
            self.scale
        )


class FormatNumber(Expression):
    pretty_name = "format_number"

    def __init__(self, column, digits):
        super(FormatNumber, self).__init__(column)
        self.column = column
        self.digits = digits.get_literal_value()

    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        if self.digits < 0:
            return None
        if not isinstance(value, (int, float)):
            return None
        rounded_value = half_even_round(value, self.digits)
        return "{0:,}".format(rounded_value)

    def args(self):
        return (
            self.column,
            self.digits
        )


class SubstringIndex(Expression):
    pretty_name = "substring_index"

    def __init__(self, column, delim, count):
        super(SubstringIndex, self).__init__(column)
        self.column = column
        self.delim = delim.get_literal_value()
        self.count = count.get_literal_value()

    def eval(self, row, schema):
        parts = str(self.column.eval(row, schema)).split(self.delim)
        return self.delim.join(parts[:self.count] if self.count > 0 else parts[self.count:])

    def args(self):
        return (
            self.column,
            self.delim,
            self.count
        )


class Coalesce(Expression):
    pretty_name = "coalesce"

    def __init__(self, columns):
        super(Coalesce, self).__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        for col in self.columns:
            col_value = col.eval(row, schema)
            if col_value is not None:
                return col_value
        return None

    def args(self):
        return self.columns


class IsNaN(UnaryExpression):
    pretty_name = "isnan"

    def eval(self, row, schema):
        return self.eval(row, schema) is float("nan")


class NaNvl(Expression):
    pretty_name = "nanvl"

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

    def args(self):
        return (
            self.col1,
            self.col2
        )


class Hypot(Expression):
    pretty_name = "hypot"

    def __init__(self, a, b):
        super(Hypot, self).__init__(a, b)
        self.a = a
        self.b = b

    def eval(self, row, schema):
        return math.hypot(self.a, self.b)

    def args(self):
        return (
            self.a,
            self.b
        )


class Sqrt(UnaryExpression):
    pretty_name = "SQRT"

    def eval(self, row, schema):
        return math.sqrt(self.column.eval(row, schema))


class Cbrt(UnaryExpression):
    pretty_name = "CBRT"

    def eval(self, row, schema):
        return self.column.eval(row, schema) ** 1. / 3.


class Abs(UnaryExpression):
    pretty_name = "ABS"

    def eval(self, row, schema):
        return abs(self.column.eval(row, schema))


class Acos(UnaryExpression):
    pretty_name = "ACOS"

    def eval(self, row, schema):
        return math.acos(self.column.eval(row, schema))


class Asin(UnaryExpression):
    pretty_name = "ASIN"

    def eval(self, row, schema):
        return math.asin(self.column.eval(row, schema))


class Atan(UnaryExpression):
    pretty_name = "ATAN"

    def eval(self, row, schema):
        return math.atan(self.column.eval(row, schema))


class Atan2(Expression):
    pretty_name = "ATAN"

    def __init__(self, y, x):
        super(Atan2).__init__(y, x)
        self.y = y
        self.x = x

    def eval(self, row, schema):
        return math.atan2(self.y.eval(row, schema), self.x.eval(row, schema))

    def args(self):
        return (
            self.y,
            self.x
        )


class Tan(UnaryExpression):
    pretty_name = "TAN"

    def eval(self, row, schema):
        return math.tan(self.column.eval(row, schema))


class Tanh(UnaryExpression):
    pretty_name = "TANH"

    def eval(self, row, schema):
        return math.tanh(self.column.eval(row, schema))


class Cos(UnaryExpression):
    pretty_name = "COS"

    def eval(self, row, schema):
        return math.cos(self.column.eval(row, schema))


class Cosh(UnaryExpression):
    pretty_name = "COSH"

    def eval(self, row, schema):
        return math.cosh(self.column.eval(row, schema))


class Sin(UnaryExpression):
    pretty_name = "SIN"

    def eval(self, row, schema):
        return math.sin(self.column.eval(row, schema))


class Sinh(UnaryExpression):
    pretty_name = "SINH"

    def eval(self, row, schema):
        return math.sinh(self.column.eval(row, schema))


class Exp(UnaryExpression):
    pretty_name = "EXP"

    def eval(self, row, schema):
        return math.exp(self.column.eval(row, schema))


class ExpM1(UnaryExpression):
    pretty_name = "EXPM1"

    def eval(self, row, schema):
        return math.expm1(self.column.eval(row, schema))


class Factorial(UnaryExpression):
    pretty_name = "factorial"

    def eval(self, row, schema):
        return math.factorial(self.column.eval(row, schema))


class Floor(UnaryExpression):
    pretty_name = "FLOOR"

    def eval(self, row, schema):
        return math.floor(self.column.eval(row, schema))


class Ceil(UnaryExpression):
    pretty_name = "CEIL"

    def eval(self, row, schema):
        return math.ceil(self.column.eval(row, schema))


class Log(Expression):
    pretty_name = "LOG"

    def __init__(self, base, value):
        super(Log, self).__init__(base, value)
        self.base = base.get_literal_value()
        self.value = value

    def eval(self, row, schema):
        value_eval = self.value.eval(row, schema)
        if value_eval == 0:
            return None
        return math.log(value_eval, self.base)

    def args(self):
        if self.base == math.e:
            return (self.value, )
        return (
            self.base,
            self.value
        )


class Log10(UnaryExpression):
    pretty_name = "LOG10"

    def eval(self, row, schema):
        return math.log10(self.column.eval(row, schema))


class Log2(UnaryExpression):
    pretty_name = "LOG2"

    def eval(self, row, schema):
        return math.log(self.column.eval(row, schema), 2)


class Log1p(UnaryExpression):
    pretty_name = "LOG1P"

    def eval(self, row, schema):
        return math.log1p(self.column.eval(row, schema))


class Rint(UnaryExpression):
    pretty_name = "ROUND"

    def eval(self, row, schema):
        return round(self.column.eval(row, schema))


class Signum(UnaryExpression):
    pretty_name = "SIGNUM"

    def eval(self, row, schema):
        column_value = self.column.eval(row, schema)
        if column_value == 0:
            return 0
        if column_value > 0:
            return 1.0
        return -1.0


class ToDegrees(UnaryExpression):
    pretty_name = "DEGREES"

    def eval(self, row, schema):
        return math.degrees(self.column.eval(row, schema))


class ToRadians(UnaryExpression):
    pretty_name = "RADIANS"

    def eval(self, row, schema):
        return math.radians(self.column.eval(row, schema))


class Rand(Expression):
    pretty_name = "rand"

    def __init__(self, seed=None):
        super(Rand, self).__init__()
        self.seed = seed.get_literal_value() if seed is not None else random.random()
        self.random_generator = None

    def eval(self, row, schema):
        return self.random_generator.nextDouble()

    def initialize(self, partition_index):
        self.random_generator = XORShiftRandom(self.seed + partition_index)

    def args(self):
        return self.seed


class Randn(Expression):
    pretty_name = "randn"

    def __init__(self, seed=None):
        super(Randn, self).__init__()
        self.seed = seed.get_literal_value()
        self.random_generator = None

    def eval(self, row, schema):
        return self.random_generator.nextGaussian()

    def initialize(self, partition_index):
        self.random_generator = XORShiftRandom(self.seed + partition_index)

    def args(self):
        return self.seed


class SparkPartitionID(Expression):
    pretty_name = "SPARK_PARTITION_ID"

    def __init__(self):
        super(SparkPartitionID, self).__init__()
        self.partition_index = None

    def eval(self, row, schema):
        return self.partition_index

    def initialize(self, partition_index):
        self.partition_index = partition_index

    def args(self):
        return ()


class CreateStruct(Expression):
    pretty_name = "struct"

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

    def args(self):
        return self.columns


class Bin(UnaryExpression):
    pretty_name = "bin"

    def eval(self, row, schema):
        return format(self.column.eval(row, schema), 'b')


class ShiftLeft(Expression):
    pretty_name = "shiftleft"

    def __init__(self, arg, num_bits):
        super(ShiftLeft, self).__init__(arg)
        self.arg = arg
        self.num_bits = num_bits.get_literal_value()

    def eval(self, row, schema):
        return self.arg.eval(row, schema) << self.num_bits

    def args(self):
        return (
            self.arg,
            self.num_bits
        )


class ShiftRight(Expression):
    pretty_name = "shiftright"

    def __init__(self, arg, num_bits):
        super(ShiftRight, self).__init__(arg)
        self.arg = arg
        self.num_bits = num_bits.get_literal_value()

    def eval(self, row, schema):
        return self.arg.eval(row, schema) >> self.num_bits

    def args(self):
        return (
            self.arg,
            self.num_bits
        )


class ShiftRightUnsigned(Expression):
    pretty_name = "shiftrightunsigned"

    def __init__(self, arg, num_bits):
        super(ShiftRightUnsigned, self).__init__(arg)
        self.arg = arg
        self.num_bits = num_bits.get_literal_value()

    def eval(self, row, schema):
        rightShifted = self.arg.eval(row, schema) >> self.num_bits
        return rightShifted % JVM_MAX_INTEGER_SIZE

    def args(self):
        return (
            self.arg,
            self.num_bits
        )


class Greatest(Expression):
    pretty_name = "greatest"

    def __init__(self, columns):
        super(Greatest, self).__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        values = (col.eval(row, schema) for col in self.columns)
        return max((value for value in values if value is not None), default=None)

    def args(self):
        return self.columns


class Least(Expression):
    pretty_name = "least"

    def __init__(self, columns):
        super(Least, self).__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        values = (col.eval(row, schema) for col in self.columns)
        return min((value for value in values if value is not None), default=None)

    def args(self):
        return self.columns


class Length(UnaryExpression):
    pretty_name = "length"

    def eval(self, row, schema):
        return len(str(self.column.eval(row, schema)))


class Lower(UnaryExpression):
    pretty_name = "lower"

    def eval(self, row, schema):
        return str(self.column.eval(row, schema)).lower()


class Upper(UnaryExpression):
    pretty_name = "upper"

    def eval(self, row, schema):
        return str(self.column.eval(row, schema)).upper()


class Concat(Expression):
    pretty_name = "concat"

    def __init__(self, columns):
        super(Concat, self).__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        return "".join(str(col.eval(row, schema)) for col in self.columns)

    def args(self):
        return self.columns


class ConcatWs(Expression):
    pretty_name = "concat_ws"

    def __init__(self, sep, columns):
        super(ConcatWs, self).__init__(columns)
        self.sep = sep.get_literal_value()
        self.columns = columns

    def eval(self, row, schema):
        return self.sep.join(str(col.eval(row, schema)) for col in self.columns)

    def args(self):
        if self.columns:
            return [self.sep] + self.columns
        return [self.sep]


class Reverse(UnaryExpression):
    pretty_name = "reverse"

    def eval(self, row, schema):
        return str(self.column.eval(row, schema))[::-1]


class MapKeys(UnaryExpression):
    pretty_name = "map_keys"

    def eval(self, row, schema):
        return list(self.column.eval(row, schema).keys())


class MapValues(UnaryExpression):
    pretty_name = "map_values"

    def eval(self, row, schema):
        return list(self.column.eval(row, schema).values())


class MapEntries(UnaryExpression):
    pretty_name = "map_entries"

    def eval(self, row, schema):
        return list(self.column.eval(row, schema).items())


class MapFromEntries(UnaryExpression):
    pretty_name = "map_from_entries"

    def eval(self, row, schema):
        return dict(self.column.eval(row, schema))


class MapConcat(Expression):
    pretty_name = "map_concat"

    def __init__(self, columns):
        super(MapConcat, self).__init__(*columns)
        self.columns = columns

    def eval(self, row, schema):
        result = {}
        for column in self.columns:
            col_value = column.eval(row, schema)
            if isinstance(col_value, dict):
                result.update(col_value)
        return result

    def args(self):
        return self.columns


class StringSplit(Expression):
    pretty_name = "split"

    def __init__(self, column, regex, limit):
        super(StringSplit, self).__init__(column)
        self.column = column
        self.regex = regex.get_literal_value()
        self.compiled_regex = re.compile(self.regex)
        self.limit = limit.get_literal_value()

    def eval(self, row, schema):
        limit = self.limit if self.limit is not None else 0
        return list(self.compiled_regex.split(str(self.column.eval(row, schema)), limit))

    def args(self):
        if self.limit is None:
            return (
                self.column,
                self.regex
            )
        return (
            self.column,
            self.regex,
            self.limit
        )


class Conv(Expression):
    pretty_name = "conv"

    def __init__(self, column, from_base, to_base):
        super(Conv, self).__init__(column)
        self.column = column
        self.from_base = from_base.get_literal_value()
        self.to_base = to_base.get_literal_value()

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        return self.convert(
            value,
            self.from_base,
            abs(self.to_base),
            positive_only=self.to_base > 0
        )

    def args(self):
        return (
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
    pretty_name = "hex"

    def eval(self, row, schema):
        return Conv.convert(
            self.column.eval(row, schema),
            from_base=10,
            to_base=16,
            positive_only=True
        )


class Unhex(UnaryExpression):
    pretty_name = "unhex"

    def eval(self, row, schema):
        return Conv.convert(
            self.column.eval(row, schema),
            from_base=16,
            to_base=10,
            positive_only=True
        )


class Ascii(UnaryExpression):
    pretty_name = "ascii"

    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        if value is None:
            return None
        value_as_string = str(value)
        if not value_as_string:
            return None
        return ord(value_as_string[0])


class MonotonicallyIncreasingID(Expression):
    pretty_name = "monotonically_increasing_id"

    def __init__(self):
        super(MonotonicallyIncreasingID, self).__init__()
        self.generator = None

    def eval(self, row, schema):
        return self.generator.next()

    def initialize(self, partition_index):
        self.generator = MonotonicallyIncreasingIDGenerator(partition_index)

    def args(self):
        return ()


class Base64(UnaryExpression):
    pretty_name = "base64"

    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        encoded = base64.b64encode(bytes(value, encoding="utf-8"))
        return str(encoded)[2:-1]


class UnBase64(UnaryExpression):
    pretty_name = "unbase64"

    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        return bytearray(base64.b64decode(value))


class GroupingID(Expression):
    pretty_name = "grouping_id"

    def __init__(self, columns):
        super(GroupingID, self).__init__(*columns)
        self.columns = columns

    def eval(self, row, schema):
        metadata = row.get_metadata()
        if metadata is None or "grouping" not in metadata:
            raise AnalysisException("grouping_id() can only be used with GroupingSets/Cube/Rollup")
        id_binary_string_value = "".join(
            "1" if grouping else "0" for grouping in metadata["grouping"]
        )
        return int(id_binary_string_value, 2)

    def args(self):
        return self.columns


class Grouping(UnaryExpression):
    pretty_name = "grouping"

    def eval(self, row, schema):
        metadata = row.get_metadata()
        if metadata is None or "grouping" not in metadata:
            raise AnalysisException("grouping_id() can only be used with GroupingSets/Cube/Rollup")
        pos = self.column.find_position_in_schema(schema)
        return int(metadata["grouping"][pos])


class InputFileName(Expression):
    pretty_name = "input_file_name"

    def eval(self, row, schema):
        metadata = row.get_metadata()
        if metadata is None:
            return None
        return metadata.get("input_file_name", "")

    def args(self):
        return ()


__all__ = [
    "Grouping", "GroupingID", "Coalesce", "IsNaN", "MonotonicallyIncreasingID", "NaNvl", "Rand",
    "Randn", "SparkPartitionID", "Sqrt", "CreateStruct", "CaseWhen", "Abs", "Acos", "Asin",
    "Atan", "Atan2", "Bin", "Cbrt", "Ceil", "Conv", "Cos", "Cosh", "Exp", "ExpM1", "Factorial",
    "Floor", "Greatest", "Hex", "Unhex", "Hypot", "Least", "Log", "Log10", "Log1p", "Log2",
    "Rint", "Round", "Bround", "Signum", "Sin", "Sinh", "Tan", "Tanh", "ToDegrees",
    "ToRadians", "Ascii", "Base64", "ConcatWs", "FormatNumber", "Length", "Lower",
    "RegExpExtract", "RegExpReplace", "UnBase64", "StringSplit", "SubstringIndex", "Upper",
    "Concat", "Reverse", "MapKeys", "MapValues", "MapEntries", "MapFromEntries",
    "MapConcat", "StarOperator", "ShiftLeft", "ShiftRight", "ShiftRightUnsigned"
]
