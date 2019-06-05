class Expression(object):
    def __init__(self, *children):
        self.children = children

    def eval(self, row):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError

    def output_cols(self, row):
        return [str(self)]

    @property
    def may_output_multiple_cols(self):
        return False

    def merge(self, row):
        pass

    def recursive_merge(self, row):
        self.merge(row)
        self.children_merge(self.children, row)

    @staticmethod
    def children_merge(children, row):
        from pysparkling.sql.column import Column
        for child in children:
            if isinstance(child, Expression):
                child.recursive_merge(row)
            elif isinstance(child, Column) and isinstance(child.expr, Expression):
                child.expr.recursive_merge(row)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_merge(child, row)

    def mergeStats(self, other):
        pass

    def recursive_merge_stats(self, other):
        self.mergeStats(other)
        self.children_merge_stats(self.children, other)

    @staticmethod
    def children_merge_stats(children, other):
        from pysparkling.sql.column import Column
        for child in children:
            if isinstance(child, Expression):
                child.recursive_merge_stats(other)
            elif isinstance(child, Column) and isinstance(child.expr, Expression):
                child.expr.recursive_merge_stats(other)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_merge_stats(child, other)

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
