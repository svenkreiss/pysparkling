import itertools
import random
import re
from operator import itemgetter

from pyspark import Row


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
    last_slash_position = t.expression.rfind('/')
    folder_path = '/' + t.expression[:last_slash_position + 1]
    file_pattern = t.expression[last_slash_position + 1:]

    return scheme, domain, folder_path, file_pattern


def format_file_uri(scheme, domain, *local_path_components):
    return '{0}://{1}{2}'.format(scheme, domain, "/".join(local_path_components))


def reservoir_sample_and_size(iterable, k, seed):
    """
        Returns a sample of k items of iterable and its original size
        If the iterable contains less than k items the sample is a list of those items
        Algorithm used is reservoir sampling.

        :rtype list
    """
    random.seed(seed)

    # Put the first k elements in the reservoir.
    reservoir = list(itertools.islice(iterable, k))

    # If we have consumed all the elements, return them. Otherwise do the replacement.
    l = len(reservoir)
    if l < k:
        return reservoir, l

    # If input size > k, continue the sampling process.
    for l, item in enumerate(iterable, start=k + 1):
        # There are k elements in the reservoir, and the l-th element has been
        # consumed. It should be chosen with probability k/l. The expression
        # below is a random int chosen uniformly from [0, l)
        replacementIndex = random.randint(0, l)
        if replacementIndex < k:
            reservoir[replacementIndex.toInt] = item

    return reservoir, l


def compute_weighted_percentiles(weighted_values, number_of_percentiles, key=lambda x: x):
    """
    Compute weighted percentiles from a list of values and weights.

    number_of_percentiles evenly distributed percentiles values will be returned,
    including the 0th (minimal value) and the 100th (maximal value).

    A custom key function can be supplied to customize the sort order

    :type weighted_values: list of tuple
    :type number_of_percentiles: int
    :type key: function

    Examples with 0th, 50th and 100th percentiles:
    >>> compute_weighted_percentiles([(2, 0.2), (1, 0.1), (3, 0.7)], 3)
    [1, 3, 3]
    >>> compute_weighted_percentiles([(1, 10), (2, 20), (3, 20)], 3)
    [1, 2, 3]
    >>> compute_weighted_percentiles([(i, 1) for i in range(1, 101)], 1)
    Traceback (most recent call last):
     ...
    ValueError: number_of_percentiles must be at least 2
    >>> compute_weighted_percentiles([(i, 1) for i in range(1, 101)], 2)
    [1, 100]
    >>> compute_weighted_percentiles([(i, 1) for i in range(1, 101)], 3)
    [1, 50, 100]
    >>> compute_weighted_percentiles([(i, 1) for i in range(1, 101)], 4)
    [1, 34, 67, 100]
    >>> compute_weighted_percentiles([(i, 1) for i in range(1, 101)], 5)
    [1, 25, 50, 75, 100]
    >>> compute_weighted_percentiles([
    ...   ((1, "b"), 10),
    ...   ((2, "c"), 20),
    ...   ((3, "a"), 20)
    ... ], 3, key=lambda row: row[1])
    [(3, 'a'), (1, 'b'), (2, 'c')]
    """
    if number_of_percentiles == 1:
        raise ValueError("number_of_percentiles must be at least 2")

    ordered_values = sorted(weighted_values, key=lambda weighted_value: key(weighted_value[0]))
    total_weight = sum(weight for value, weight in ordered_values)

    bounds = []
    cumulative_weight = 0
    for value, weight in ordered_values:
        cumulative_weight += weight
        while len(bounds) / (number_of_percentiles - 1) <= cumulative_weight / total_weight:
            bounds.append(value)

    return bounds


def get_keyfunc(cols):
    """
    Return a function that maps a row to a tuple of some of its columns values
    """

    def key(row):
        """
        Returns a tuple of a row column values in the requested column order
        """
        return tuple(row[col] for col in cols)

    return key


def row_from_keyed_values(keyed_values):
    # Preserve Row column order which is modified when calling Row(dict)
    new_row = Row(*[value for key, value in keyed_values])
    new_row.__fields__ = [key for key, value in keyed_values]
    return new_row


FULL_WIDTH_REGEX = fullWidthRegex = re.compile(
    "[" +
    "\u1100-\u115F" +
    "\u2E80-\uA4CF" +
    "\uAC00-\uD7A3" +
    "\uF900-\uFAFF" +
    "\uFE10-\uFE19" +
    "\uFE30-\uFE6F" +
    "\uFF00-\uFF60" +
    "\uFFE0-\uFFE6" +
    "]"
)


def str_half_width(string):
    """
    Compute string length with full width characters counting for 2 normal ones
    """
    if string is None:
        return 0
    if not isinstance(string, str):
        string = str(string)
    return len(string) + len(FULL_WIDTH_REGEX.findall(string))


def pad_cell(cell, truncate, col_width):
    """
    if (truncate > 0) {
    StringUtils.leftPad(cell, colWidths(i) - Utils.stringHalfWidth(cell) + cell.length)
  } else {
    StringUtils.rightPad(cell, colWidths(i) - Utils.stringHalfWidth(cell) + cell.length)
  }
    """
    if cell is None:
        cell = "null"
    if not isinstance(cell, str):
        cell = str(cell)
    cell_width = col_width - str_half_width(cell) + len(cell)
    if truncate > 0:
        return cell.rjust(cell_width)
    else:
        return cell.ljust(cell_width)
