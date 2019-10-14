from dateutil.relativedelta import relativedelta

from pysparkling.sql.expressions.expressions import Expression, UnaryExpression
from pysparkling.sql.types import DateType


class AddMonths(Expression):
    def __init__(self, start_date, num_months):
        super().__init__(start_date)
        self.start_date = start_date
        self.num_months = num_months

    def eval(self, row, schema):
        return self.start_date.cast(DateType()).eval(row, schema) + relativedelta(months=self.num_months)

    def __str__(self):
        return "add_months({0}, {1})".format(self.start_date, self.num_months)


class Year(UnaryExpression):
    def eval(self, row, schema):
        return self.column.cast(DateType()).eval(row, schema).year

    def __str__(self):
        return "year({0})".format(self.column)


class Month(UnaryExpression):
    def eval(self, row, schema):
        return self.column.cast(DateType()).eval(row, schema).month

    def __str__(self):
        return "month({0})".format(self.column)
