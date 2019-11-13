from pysparkling.sql.expressions.expressions import Expression


class Aggregation(Expression):
    @property
    def is_an_aggregation(self):
        return True

    def merge(self, row, schema):
        raise NotImplementedError

    def mergeStats(self, other, schema):
        raise NotImplementedError

    def eval(self, row, schema):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError
