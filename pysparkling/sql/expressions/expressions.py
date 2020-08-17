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

    def recursive_initialize(self, partition_index):
        """
        This methods adds once data to expressions that require it
        e.g. for non-deterministic expression so that their result is constant
        across several evaluations
        """
        self.initialize(partition_index)
        self.children_initialize(self.children, partition_index)

    @staticmethod
    def children_initialize(children, partition_index):
        # # Top level import would cause cyclic dependencies
        # # pylint: disable=import-outside-toplevel
        # from pysparkling.sql.column import Column
        for child in children:
            if isinstance(child, Expression):
                child.recursive_initialize(partition_index)
            # todo: elif isinstance(child, Column) and isinstance(child.expr, Expression):
            elif isinstance(child.expr, Expression):
                child.expr.recursive_initialize(partition_index)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_initialize(child, partition_index)

    def initialize(self, partition_index):
        pass

    # Adding information about the schema that was defined in the step prior the evaluation
    def with_pre_evaluation_schema(self, schema):
        self.pre_evaluation_schema = schema

    def recursive_pre_evaluation_schema(self, schema):
        self.with_pre_evaluation_schema(schema)
        self.children_pre_evaluation_schema(self.children, schema)

    @staticmethod
    def children_pre_evaluation_schema(children, schema):
        # # Top level import would cause cyclic dependencies
        # # pylint: disable=import-outside-toplevel
        # from pysparkling.sql.column import Column
        for child in children:
            if isinstance(child, Expression):
                child.recursive_pre_evaluation_schema(schema)
            # todo: elif isinstance(child, Column) and isinstance(child.expr, Expression):
            elif isinstance(child.expr, Expression):
                child.expr.recursive_pre_evaluation_schema(schema)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_pre_evaluation_schema(child, schema)
