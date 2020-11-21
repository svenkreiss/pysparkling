import string

from ...utils import levenshtein_distance
from ..types import StringType
from .expressions import Expression, UnaryExpression


class StringTrim(UnaryExpression):
    pretty_name = "trim"

    def eval(self, row, schema):
        return self.column.eval(row, schema).strip()


class StringLTrim(UnaryExpression):
    pretty_name = "ltrim"

    def eval(self, row, schema):
        return self.column.eval(row, schema).lstrip()


class StringRTrim(UnaryExpression):
    pretty_name = "rtrim"

    def eval(self, row, schema):
        return self.column.eval(row, schema).rstrip()


class StringInStr(Expression):
    pretty_name = "instr"

    def __init__(self, column, substr):
        super().__init__(column)
        self.column = column
        self.substr = substr

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        substr_value = self.substr.cast(StringType()).eval(row, schema)
        try:
            return value.index(substr_value) + 1
        except ValueError:
            return 0

    def args(self):
        return (
            self.column,
            self.substr
        )


class StringLocate(Expression):
    pretty_name = "locate"

    def __init__(self, substr, column, pos):
        super().__init__(column)
        self.substr = substr.get_literal_value()
        self.column = column
        self.start = pos.get_literal_value() - 1

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        if self.substr not in value[self.start:]:
            return 0
        return value.index(self.substr, self.start) + 1

    def args(self):
        if self.start is None:
            return (
                self.substr,
                self.column,
            )
        return (
            self.substr,
            self.column,
            self.start
        )


class StringLPad(Expression):
    pretty_name = "lpad"

    def __init__(self, column, length, pad):
        super().__init__(column)
        self.column = column
        self.length = length.get_literal_value()
        self.pad = pad.get_literal_value()

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        delta = self.length - len(value)
        padding = (self.pad * delta)[:delta]  # Handle pad with multiple characters
        return f"{padding}{value}"

    def args(self):
        return (
            self.column,
            self.length,
            self.pad
        )


class StringRPad(Expression):
    pretty_name = "rpad"

    def __init__(self, column, length, pad):
        super().__init__(column)
        self.column = column
        self.length = length.get_literal_value()
        self.pad = pad.get_literal_value()

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        delta = self.length - len(value)
        padding = (self.pad * delta)[:delta]  # Handle pad with multiple characters
        return f"{value}{padding}"

    def args(self):
        return (
            self.column,
            self.length,
            self.pad
        )


class StringRepeat(Expression):
    pretty_name = "repeat"

    def __init__(self, column, n):
        super().__init__(column)
        self.column = column
        self.n = n.get_literal_value()

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        return value * self.n

    def args(self):
        return (
            self.column,
            self.n
        )


class StringTranslate(Expression):
    pretty_name = "translate"

    def __init__(self, column, matching_string, replace_string):
        super().__init__(column)
        self.column = column
        self.matching_string = matching_string.get_literal_value()
        self.replace_string = replace_string.get_literal_value()
        self.translation_table = str.maketrans(
            # Python's translate use an opposite importance order as Spark
            # when there are duplicates in matching_string mapped to different chars
            self.matching_string[::-1],
            self.replace_string[::-1]
        )

    def eval(self, row, schema):
        return self.column.cast(StringType()).eval(row, schema).translate(self.translation_table)

    def args(self):
        return (
            self.column,
            self.matching_string,
            self.replace_string
        )


class InitCap(UnaryExpression):
    pretty_name = "initcap"

    def eval(self, row, schema):
        value = self.column.cast(StringType()).eval(row, schema)
        return " ".join(word.capitalize() for word in value.split())


class Levenshtein(Expression):
    pretty_name = "levenshtein"

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

    def args(self):
        return (
            self.column1,
            self.column2
        )


class SoundEx(UnaryExpression):
    pretty_name = "soundex"

    _soundex_mapping = {
        letter: int(soundex_code)
        for letter, soundex_code in zip(
            string.ascii_uppercase,
            "01230127022455012623017202"
        )
    }

    def eval(self, row, schema):
        raw_value = self.column.cast(StringType()).eval(row, schema)

        if raw_value is None:
            return None

        if raw_value == "":
            return ""

        value = raw_value.upper()
        initial = value[0]

        last_code = self._encode(initial)
        if last_code is None:
            return raw_value

        res = [initial]
        for letter in value:
            code = self._encode(letter)
            if code is None:
                continue
            if code == 7:
                continue
            if code not in (0, last_code):
                res.append(str(code))
                if len(res) > 3:
                    break
            last_code = code

        return ("".join(res) + "000")[:4]

    def _encode(self, letter):
        """
        Encode a letter using the SoundEx mapping.
        Returns None if the letter is not recognized
        """
        return self._soundex_mapping.get(letter)


__all__ = [
    "StringTrim", "StringTranslate", "StringRTrim", "StringRepeat", "StringRPad",
    "StringLTrim", "StringLPad", "StringLocate", "Levenshtein", "StringInStr", "InitCap",
    "SoundEx"
]
