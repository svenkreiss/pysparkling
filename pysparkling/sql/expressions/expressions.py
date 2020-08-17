from pysparkling.sql.types import StructField, DataType


class Expression(object):
    def __init__(self, *children):
        self.children = children
        self.pre_evaluation_schema = None

    def eval(self, row, schema):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError

    def __repr__(self):
        return self.__class__.__name__

    def output_fields(self, schema):
        return [StructField(
            name=str(self),
            dataType=self.data_type,
            nullable=self.is_nullable
        )]

    @property
    def data_type(self):
        # pylint: disable=W0511
        # todo: be more specific
        return DataType()

    @property
    def is_nullable(self):
        return True
