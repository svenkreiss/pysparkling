from functools import reduce

from .internal_utils.joins import (
    CROSS_JOIN, FULL_JOIN, INNER_JOIN, LEFT_ANTI_JOIN, LEFT_JOIN, LEFT_SEMI_JOIN, RIGHT_JOIN
)
from .types import _get_null_fields, _has_nulltype, _infer_schema, _merge_type, StructField, StructType
from .utils import IllegalArgumentException


def infer_schema_from_rdd(rdd):
    return infer_schema_from_list(rdd.takeSample(withReplacement=False, num=200))


def infer_schema_from_list(data, names=None):
    """
    Infer schema from list of Row or tuple.

    :param data: list of Row or tuple
    :param names: list of column names
    :return: :class:`pysparkling.sql.types.StructType`
    """
    if not data:
        raise ValueError("can not infer schema from empty dataset")
    first = data[0]
    if isinstance(first, dict):
        raise NotImplementedError(
            "Inferring schema from dict is deprecated in Spark "
            "and not implemented in pysparkling. "
            "Please use .sql.Row instead"
        )
    schema = reduce(_merge_type, (_infer_schema(row, names) for row in data))
    if _has_nulltype(schema):
        raise ValueError(
            "Type(s) of the following field(s) cannot be determined after inferring: '{0}'".format(
                "', '".join(_get_null_fields(schema))
            )
        )
    return schema


def merge_schemas(left_schema, right_schema, how, on=None):
    if on is None:
        on = []

    left_on_fields, right_on_fields = get_on_fields(left_schema, right_schema, on)
    other_left_fields = [field for field in left_schema.fields if field not in left_on_fields]
    other_right_fields = [field for field in right_schema.fields if field not in right_on_fields]

    if how in (INNER_JOIN, CROSS_JOIN, LEFT_JOIN, LEFT_ANTI_JOIN, LEFT_SEMI_JOIN):
        on_fields = left_on_fields
    elif how == RIGHT_JOIN:
        on_fields = right_on_fields
    elif how == FULL_JOIN:
        on_fields = [StructField(field.name, field.dataType, nullable=True)
                     for field in left_on_fields]
    else:
        raise IllegalArgumentException("Invalid how argument in join: {0}".format(how))

    return StructType(fields=on_fields + other_left_fields + other_right_fields)


def get_on_fields(left_schema, right_schema, on):
    left_on_fields = [next(field for field in left_schema if field.name == c) for c in on]
    right_on_fields = [next(field for field in right_schema if field.name == c) for c in on]
    return left_on_fields, right_on_fields


def get_schema_from_cols(cols, current_schema):
    new_schema = StructType(fields=[
        field for col in cols for field in col.find_fields_in_schema(current_schema)
    ])
    return new_schema
