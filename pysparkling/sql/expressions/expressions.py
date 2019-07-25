from pyspark.sql.types import StructField, DataType


class Expression(object):
    def __init__(self, *children):
        self.children = children

    def eval(self, row, schema):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError

    def output_fields(self, schema):
        return [StructField(
            name=str(self),
            dataType=self.data_type,
            nullable=self.is_nullable
        )]

    @property
    def data_type(self):
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

    def merge(self, row, schema):
        pass

    def recursive_merge(self, row, schema):
        self.merge(row, schema)
        self.children_merge(self.children, row, schema)

    @staticmethod
    def children_merge(children, row, schema):
        from pysparkling.sql.column import Column
        for child in children:
            if isinstance(child, Expression):
                child.recursive_merge(row, schema)
            elif isinstance(child, Column) and isinstance(child.expr, Expression):
                child.expr.recursive_merge(row, schema)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_merge(child, row, schema)

    def mergeStats(self, other, schema):
        pass

    def recursive_merge_stats(self, other, schema):
        self.mergeStats(other, schema)
        self.children_merge_stats(self.children, other, schema)

    @staticmethod
    def children_merge_stats(children, other, schema):
        from pysparkling.sql.column import Column
        for child in children:
            if isinstance(child, Expression):
                child.recursive_merge_stats(other, schema)
            elif isinstance(child, Column) and isinstance(child.expr, Expression):
                child.expr.recursive_merge_stats(other, schema)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_merge_stats(child, other, schema)

    # Initialization for nondeterministic expression (like in scala)
    def recursive_initialize(self, partition_index):
        self.initialize(partition_index)
        self.children_initialize(self.children, partition_index)

    @staticmethod
    def children_initialize(children, partition_index):
        from pysparkling.sql.column import Column
        for child in children:
            if isinstance(child, Expression):
                child.recursive_initialize(partition_index)
            elif isinstance(child, Column) and isinstance(child.expr, Expression):
                child.expr.recursive_initialize(partition_index)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_initialize(child, partition_index)

    def initialize(self, partition_index):
        pass


class UnaryExpression(Expression):
    def __init__(self, column):
        super().__init__(column)
        self.column = column

    def eval(self, row, schema):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError
