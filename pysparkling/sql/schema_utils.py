from functools import reduce

from pysparkling.sql.types import _infer_schema, _has_nulltype, _merge_type, \
    _get_null_fields


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
