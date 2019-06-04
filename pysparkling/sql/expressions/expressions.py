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

    def recursive_merge(self, row):
        self.merge(row)
        self.children_merge(self.children, row)

    @staticmethod
    def children_merge(childrens, row):
        from pysparkling.sql.column import Column
        for child in childrens:
            if isinstance(child, Expression):
                child.recursive_merge(row)
            elif isinstance(child, Column):
                child.expr.recursive_merge(row)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_merge(child, row)

    @staticmethod
    def children_merge_stats(childrens, other):
        from pysparkling.sql.column import Column
        for child in childrens:
            if isinstance(child, Expression):
                child.recursive_merge_stats(other)
            elif isinstance(child, Column):
                child.expr.recursive_merge_stats(other)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_merge_stats(child, other)

    def merge(self, row):
        pass

    def recursive_merge_stats(self, row):
        pass

    def mergeStats(self, other):
        self.mergeStats(other)
        self.children_merge_stats(self.children, other)

