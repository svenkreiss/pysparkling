import datetime

from dateutil.relativedelta import relativedelta

from pysparkling.sql.casts import get_time_formatter, get_unix_timestamp_parser
from pysparkling.sql.expressions.expressions import Expression, UnaryExpression
from pysparkling.sql.types import DateType, TimestampType, FloatType


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


class FromUnixTime(Expression):
    def __init__(self, column, f):
        super().__init__(column)
        self.column = column
        self.format = f
        self.formatter = get_time_formatter(self.format)

    def eval(self, row, schema):
        timestamp = self.column.cast(FloatType()).eval(row, schema)
        return self.formatter(datetime.datetime.fromtimestamp(timestamp))

    def __str__(self):
        return "from_unixtime({0}, {1})".format(self.column, self.format)


class CurrentTimestamp(Expression):
    def __init__(self):
        super().__init__()
        self.current_timestamp = None

    def eval(self, row, schema):
        return self.current_timestamp

    def initialize(self, partition_index):
        super().initialize(partition_index)
        self.current_timestamp = datetime.datetime.now()

    def __str__(self):
        return "current_timestamp()"


class UnixTimestamp(Expression):
    def __init__(self, column, f):
        super().__init__(column)
        self.column = column
        self.format = f
        self.parser = get_unix_timestamp_parser(self.format)

    def eval(self, row, schema):
        datetime_as_string = self.column.eval(row, schema)
        return self.parser(datetime_as_string)

    def __str__(self):
        return "unix_timestamp({0}, {1})".format(self.column, self.format)


class ParseToTimestamp(Expression):
    def __init__(self, column, f):
        super().__init__(column)
        self.column = column
        self.format = f
        self.parser = get_unix_timestamp_parser(self.format)

    def eval(self, row, schema):
        datetime_as_string = self.column.eval(row, schema)
        return datetime.datetime.fromtimestamp(self.parser(datetime_as_string))

    def __str__(self):
        return "to_timestamp('{0}'{1})".format(
            self.column,
            ", '{0}'".format(self.format) if self.format is not None else ""
        )


class ParseToDate(Expression):
    def __init__(self, column, f):
        super().__init__(column)
        self.column = column
        self.format = f
        self.parser = get_unix_timestamp_parser(self.format)

    def eval(self, row, schema):
        datetime_as_string = self.column.eval(row, schema)
        return datetime.date.fromtimestamp(self.parser(datetime_as_string))

    def __str__(self):
        return "to_date('{0}'{1})".format(
            self.column,
            ", '{0}'".format(self.format) if self.format is not None else ""
        )
