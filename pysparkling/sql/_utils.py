import itertools
import re

from ._row import create_row, Row, row_from_keyed_values
from .internal_utils.joins import (
    CROSS_JOIN, FULL_JOIN, INNER_JOIN, LEFT_ANTI_JOIN, LEFT_JOIN, LEFT_SEMI_JOIN, RIGHT_JOIN
)
from .utils import IllegalArgumentException


def merge_rows_joined_on_values(left, right, left_schema, right_schema, how, on):
    left_names = left_schema.names
    right_names = right_schema.names

    left_on_fields, right_on_fields = get_on_fields(left_schema, right_schema, on)

    on_parts = [
        (on_field, left[on_field] if left is not None else right[on_field])
        for on_field in on
    ]

    if left is None and how in (FULL_JOIN, RIGHT_JOIN):
        left = create_row(left_names, [None for _ in left_names])
    if right is None and how in (LEFT_JOIN, FULL_JOIN):
        right = create_row(right_names, [None for _ in right_names])

    left_parts = (
        (field.name, value)
        for field, value in zip(left_schema.fields, left)
        if field not in left_on_fields
    )

    if how in (INNER_JOIN, CROSS_JOIN, LEFT_JOIN, FULL_JOIN, RIGHT_JOIN):
        right_parts = (
            (field.name, value)
            for field, value in zip(right_schema.fields, right)
            if field not in right_on_fields
        )
    elif how in (LEFT_SEMI_JOIN, LEFT_ANTI_JOIN):
        right_parts = ()
    else:
        raise IllegalArgumentException(f"Argument 'how' cannot be '{how}'")

    return row_from_keyed_values(itertools.chain(on_parts, left_parts, right_parts))


def merge_rows(left, right):
    return create_row(
        itertools.chain(left.__fields__, right.__fields__),
        left + right
    )


FULL_WIDTH_REGEX = re.compile(
    "["
    + r"\u1100-\u115F"
    + r"\u2E80-\uA4CF"
    + r"\uAC00-\uD7A3"
    + r"\uF900-\uFAFF"
    + r"\uFE10-\uFE19"
    + r"\uFE30-\uFE6F"
    + r"\uFF00-\uFF60"
    + r"\uFFE0-\uFFE6"
    + "]"
)


def str_half_width(value):
    """
    Compute string length with full width characters counting for 2 normal ones
    """
    string = format_cell(value)
    if string is None:
        return 0
    if not isinstance(string, str):
        string = str(string)
    return len(string) + len(FULL_WIDTH_REGEX.findall(string))


def pad_cell(cell, truncate, col_width):
    """
    Compute how to pad the value "cell" truncated to truncate so that it fits col width
    :param cell: Any
    :param truncate: int
    :param col_width: int
    :return:
    """
    cell = format_cell(cell)
    cell_width = col_width - str_half_width(cell) + len(cell)
    if truncate > 0:
        return cell.rjust(cell_width)
    return cell.ljust(cell_width)


def format_cell(value):
    """
    Convert a cell value to a string using the logic needed in DataFrame.show()
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, Row):
        return f"[{', '.join(format_cell(sub_value) for sub_value in value)}]"
    if isinstance(value, dict):
        return "[{0}]".format(
            ", ".join(
                f"{format_cell(key)} -> {format_cell(sub_value)}"
                for key, sub_value in value.items()
            )
        )
    return str(value)


def get_on_fields(left_schema, right_schema, on):
    left_on_fields = [next(field for field in left_schema if field.name == c) for c in on]
    right_on_fields = [next(field for field in right_schema if field.name == c) for c in on]
    return left_on_fields, right_on_fields
