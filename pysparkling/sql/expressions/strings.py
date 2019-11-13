from pysparkling.sql.expressions.expressions import Expression, UnaryExpression
from pysparkling.sql.types import StringType
from pysparkling.utils import levenshtein_distance


class StringTrim(UnaryExpression):
    def eval(self, row, schema):
        return self.column.eval(row, schema).strip()

    def __str__(self):
        return "trim({0})".format(self.column)


class StringLTrim(UnaryExpression):
    def eval(self, row, schema):
        return self.column.eval(row, schema).lstrip()

    def __str__(self):
        return "ltrim({0})".format(self.column)


class StringRTrim(UnaryExpression):
    def eval(self, row, schema):
        return self.column.eval(row, schema).rstrip()

    def __str__(self):
        return "rtrim({0})".format(self.column)


class StringInStr(Expression):
    def __init__(self, substr, column):
        super().__init__(column)
        self.substr = substr
        self.column = column

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        return int(self.substr in value)

    def __str__(self):
        return "instr({0}, {1})".format(
            self.substr,
            self.column
        )


class StringLocate(Expression):
    def __init__(self, substr, column, pos):
        super().__init__(column)
        self.substr = substr
        self.column = column
        self.start = pos - 1

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        if self.substr not in value[self.start:]:
            return 0
        return value.index(self.substr, self.start) + 1

    def __str__(self):
        return "locate({0}, {1}{2})".format(
            self.substr,
            self.column,
            ", {0}".format(self.start) if self.start is not None else ""
        )


class StringLPad(Expression):
    def __init__(self, column, length, pad):
        super().__init__(column)
        self.column = column
        self.length = length
        self.pad = pad

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        delta = self.length - len(value)
        padding = (self.pad * delta)[:delta]  # Handle pad with multiple characters
        return "{0}{1}".format(padding, value)

    def __str__(self):
        return "lpad({0}, {1}, {2})".format(
            self.column,
            self.length,
            self.pad
        )


class StringRPad(Expression):
    def __init__(self, column, length, pad):
        super().__init__(column)
        self.column = column
        self.length = length
        self.pad = pad

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        delta = self.length - len(value)
        padding = (self.pad * delta)[:delta]  # Handle pad with multiple characters
        return "{0}{1}".format(value, padding)

    def __str__(self):
        return "rpad({0}, {1}, {2})".format(
            self.column,
            self.length,
            self.pad
        )


class StringRepeat(Expression):
    def __init__(self, column, n):
        super().__init__(column)
        self.column = column
        self.n = n

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        return value * self.n

    def __str__(self):
        return "repeat({0}, {1})".format(
            self.column,
            self.n
        )


class StringTranslate(Expression):
    def __init__(self, column, matching_string, replace_string):
        super().__init__(column)
        self.column = column
        self.matching_string = matching_string
        self.replace_string = replace_string
        self.translation_table = str.maketrans(
            # Python's translate use an opposite importance order as Spark
            # when there are duplicates in matching_string mapped to different chars
            matching_string[::-1],
            replace_string[::-1]
        )

    def eval(self, row, schema):
        return self.column.cast(StringType()).eval(row, schema).translate(self.translation_table)

    def __str__(self):
        return "translate({0}, {1}, {2})".format(
            self.column,
            self.matching_string,
            self.replace_string
        )


class InitCap(Expression):
    def __init__(self, column):
        super().__init__(column)
        self.column = column

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        return " ".join(word.capitalize() for word in value.split())

    def __str__(self):
        return "initcap({0})".format(self.column)


class Levenshtein(Expression):
    def __init__(self, column1, column2):
        super().__init__(column1, column2)
        self.column1 = column1
        self.column2 = column2

    def eval(self, row, schema):
        value_1 = self.column1.cast(StringType()).eval(row, schema)
        value_2 = self.column2.cast(StringType()).eval(row, schema)
        if value_1 is None or value_2 is None:
            return None
        return levenshtein_distance(value_1, value_2)

    def __str__(self):
        return "levenshtein({0}, {1})".format(self.column1, self.column2)


__all__ = [
    "StringTrim", "StringTranslate", "StringRTrim", "StringRepeat", "StringRPad",
    "StringLTrim", "StringLPad", "StringLocate", "Levenshtein", "StringInStr", "InitCap"
]
