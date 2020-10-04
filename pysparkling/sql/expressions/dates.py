import datetime

from dateutil.relativedelta import relativedelta

from pysparkling.sql.expressions.expressions import Expression, UnaryExpression
from pysparkling.sql.types import DateType, TimestampType


class AddMonths(Expression):
    def __init__(self, start_date, num_months):
        super(AddMonths, self).__init__(start_date)
        self.start_date = start_date
        self.num_months = num_months

    def eval(self, row, schema):
        return (self.start_date.cast(DateType()).eval(row, schema)
                + relativedelta(months=self.num_months))

    def __str__(self):
        return "add_months({0}, {1})".format(self.start_date, self.num_months)


class DateAdd(Expression):
    def __init__(self, start_date, num_days):
        super(DateAdd, self).__init__(start_date)
        self.start_date = start_date
        self.num_days = num_days
        self.timedelta = datetime.timedelta(days=num_days)

    def eval(self, row, schema):
        return self.start_date.cast(DateType()).eval(row, schema) + self.timedelta

    def __str__(self):
        return "date_add({0}, {1})".format(self.start_date, self.num_days)


class DateSub(Expression):
    def __init__(self, start_date, num_days):
        super(DateSub, self).__init__(start_date)
        self.start_date = start_date
        self.num_days = num_days
        self.timedelta = datetime.timedelta(days=num_days)

    def eval(self, row, schema):
        return self.start_date.cast(DateType()).eval(row, schema) - self.timedelta

    def __str__(self):
        return "date_sub({0}, {1})".format(self.start_date, self.num_days)


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


class Quarter(UnaryExpression):
    def eval(self, row, schema):
        month = self.column.cast(DateType()).eval(row, schema).month
        return 1 + int((month - 1) / 3)

    def __str__(self):
        return "quarter({0})".format(self.column)


class Hour(UnaryExpression):
    def eval(self, row, schema):
        return self.column.cast(TimestampType()).eval(row, schema).hour

    def __str__(self):
        return "hour({0})".format(self.column)


class Minute(UnaryExpression):
    def eval(self, row, schema):
        return self.column.cast(TimestampType()).eval(row, schema).minute

    def __str__(self):
        return "minute({0})".format(self.column)


class Second(UnaryExpression):
    def eval(self, row, schema):
        return self.column.cast(TimestampType()).eval(row, schema).second

    def __str__(self):
        return "second({0})".format(self.column)


class DayOfMonth(UnaryExpression):
    def eval(self, row, schema):
        return self.column.cast(DateType()).eval(row, schema).day

    def __str__(self):
        return "dayofmonth({0})".format(self.column)


class DayOfYear(UnaryExpression):
    def eval(self, row, schema):
        value = self.column.cast(DateType()).eval(row, schema)
        day_from_the_first = value - datetime.date(value.year, 1, 1)
        return 1 + day_from_the_first.days

    def __str__(self):
        return "dayofyear({0})".format(self.column)


class LastDay(UnaryExpression):
    def eval(self, row, schema):
        value = self.column.cast(DateType()).eval(row, schema)
        first_of_next_month = value + relativedelta(months=1, day=1)
        return first_of_next_month - datetime.timedelta(days=1)

    def __str__(self):
        return "last_day({0})".format(self.column)


class WeekOfYear(UnaryExpression):
    def eval(self, row, schema):
        return self.column.cast(DateType()).eval(row, schema).isocalendar()[1]

    def __str__(self):
        return "weekofyear({0})".format(self.column)


class DayOfWeek(UnaryExpression):
    def eval(self, row, schema):
        date = self.column.cast(DateType()).eval(row, schema)
        return date.isoweekday() + 1 if date.isoweekday() != 7 else 1

    def __str__(self):
        return "dayofweek({0})".format(self.column)

