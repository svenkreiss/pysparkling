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

    @property
    def may_output_multiple_cols(self):
        return False

    @property
    def may_output_multiple_rows(self):
        return False

    @property
    def is_an_aggregation(self):
        return False

    def merge(self, row, schema):
        pass

    def recursive_merge(self, row, schema):
        self.merge(row, schema)
        self.children_merge(self.children, row, schema)

    @staticmethod
    def children_merge(children, row, schema):
        for child in children:
            if isinstance(child, Expression):
                child.recursive_merge(row, schema)
            elif hasattr(child, "expr") and isinstance(child.expr, Expression):
                child.expr.recursive_merge(row, schema)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_merge(child, row, schema)

    def mergeStats(self, other, schema):
        pass

    def recursive_merge_stats(self, other, schema):
        # todo: Add logic
        # # Top level import would cause cyclic dependencies
        # # pylint: disable=import-outside-toplevel
        # from pysparkling.sql.expressions.operators import Alias
        # if isinstance(other.expr, Alias):
        #     self.recursive_merge_stats(other.expr.expr, schema)
        # else:
        self.mergeStats(other.expr, schema)
        self.children_merge_stats(self.children, other, schema)

    @staticmethod
    def children_merge_stats(children, other, schema):
        # # Top level import would cause cyclic dependencies
        # # pylint: disable=import-outside-toplevel
        # from pysparkling.sql.column import Column
        for child in children:
            if isinstance(child, Expression):
                child.recursive_merge_stats(other, schema)
            # todo: elif isinstance(child, Column) and isinstance(child.expr, Expression):
            elif isinstance(child.expr, Expression):
                child.expr.recursive_merge_stats(other, schema)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_merge_stats(child, other, schema)
