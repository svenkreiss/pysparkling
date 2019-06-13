from functools import reduce
from pyspark.sql.types import _infer_schema, _has_nulltype, _merge_type


def infer_schema_from_list(data, names=None):
    """
    Infer schema from list of Row or tuple.

    :param data: list of Row or tuple
    :param names: list of column names
    :return: :class:`pyspark.sql.types.StructType`
    """
    if not data:
        raise ValueError("can not infer schema from empty dataset")
    first = data[0]
    if type(first) is dict:
        raise NotImplementedError(
            "Inferring schema from dict is deprecated in Spark and not implemented in Pyspark. "
            "Please use pyspark.sql.Row instead"
        )
    schema = reduce(_merge_type, (_infer_schema(row, names) for row in data))
    if _has_nulltype(schema):
        raise ValueError("Some of types cannot be determined after inferring")
    return schema
