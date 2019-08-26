from functools import reduce
from pysparkling.sql.types import _infer_schema, _has_nulltype, _merge_type, StructType


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
    if type(first) is dict:
        raise NotImplementedError(
            "Inferring schema from dict is deprecated in Spark "
            "and not implemented in pysparkling. "
            "Please use .sql.Row instead"
        )
    schema = reduce(_merge_type, (_infer_schema(row, names) for row in data))
    if _has_nulltype(schema):
        raise ValueError("Some of types cannot be determined after inferring")
    return schema


def merge_schemas(first, second, field_not_to_duplicate=None):
    fields = [field for field in first.fields] + [
        field for field in second.fields if field.name != field_not_to_duplicate
    ]
    return StructType(fields)


def get_schema_from_cols(cols, current_schema):
    new_schema = StructType(fields=[
        field for col in cols for field in col.find_fields_in_schema(current_schema)
    ])
    return new_schema
