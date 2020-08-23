from pysparkling.sql.types import StructType


class FieldIdGenerator(object):
    """
    This metaclass adds an unique ID to all instances of its classes.

    This allows to identify that a field was, when created, associated to a DataFrame.

    Such field can be retrieved with the syntax df.name to build an operation.
    The id clarifies if it is still associated to a field and which one
    when the operation is applied.

    While id() allow the same behaviour in most cases, this one:
    - Allows deep copies which are needed for aggregation
    - Support distributed computation, e.g. multiprocessing
    """
    _id = 0

    @classmethod
    def next(cls):
        cls._id += 1
        return cls._id

    @classmethod
    def bind_schema(cls, schema):
        for field in schema.fields:
            if not hasattr(field, "id"):
                field.id = cls.next()
            if isinstance(field, StructType):
                cls.bind_schema(field)
        return schema

    @classmethod
    def unbind_schema(cls, schema):
        for field in schema.fields:
            delattr(field, "id")
            if isinstance(field, StructType):
                cls.unbind_schema(field)
        return schema
