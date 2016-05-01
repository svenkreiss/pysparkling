
class Tokenizer(object):
    def __init__(self, expression):
        self.expression = expression

    def next(self, separator=None):
        if isinstance(separator, list):
            sep_pos = [self.expression.find(s) for s in separator]
            sep_pos = [s for s in sep_pos if s >= 0]
            if sep_pos:
                sep_pos = min(sep_pos)
            else:
                sep_pos = -1
        elif separator:
            sep_pos = self.expression.find(separator)
        else:
            sep_pos = -1

        if sep_pos < 0:
            value = self.expression
            self.expression = ''
            return value

        value = self.expression[:sep_pos]
        self.expression = self.expression[sep_pos + len(separator):]
        return value
