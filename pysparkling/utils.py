import re
from operator import itemgetter


WILDCARD_START_PATTERN = re.compile(r'(?P<previous_character>^|[^\\])(?P<wildcard_start>[*?[])')


class Tokenizer(object):
    def __init__(self, expression):
        self.expression = expression

    def next(self, separator=None):
        if isinstance(separator, list):
            separator_positions_and_lengths = [
                (self.expression.find(s), s)
                for s in separator if s in self.expression
            ]
            if separator_positions_and_lengths:
                sep_pos, separator = min(separator_positions_and_lengths, key=itemgetter(0))
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


def parse_file_uri(expr):
    t = Tokenizer(expr)
    scheme = t.next('://')
    domain = t.next('/')
    wildcard_match = next(WILDCARD_START_PATTERN.finditer(t.expression), None)
    if wildcard_match is not None:
        first_pattern_position = wildcard_match.start("wildcard_start")
    else:
        first_pattern_position = len(t.expression)

    last_slash_position = t.expression.rfind('/', 0, first_pattern_position)
    folder_path = '/' + t.expression[:last_slash_position+1]
    file_pattern = t.expression[last_slash_position+1:]

    return scheme, domain, folder_path, file_pattern


def format_file_uri(scheme, domain, *local_path_components):
    return '{0}://{1}{2}'.format(scheme, domain, "/".join(local_path_components))
