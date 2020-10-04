import datetime

from dateutil.relativedelta import relativedelta

from pysparkling.sql.expressions.expressions import Expression, UnaryExpression
from pysparkling.sql.types import DateType, TimestampType

DAYS_OF_WEEK = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")


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


class NextDay(Expression):
    def __init__(self, column, day_of_week):
        super(NextDay, self).__init__(column)
        self.column = column
        self.day_of_week = day_of_week

    def eval(self, row, schema):
        value = self.column.cast(DateType()).eval(row, schema)

        if self.day_of_week.upper() not in DAYS_OF_WEEK:
            return None

        today = value.isoweekday()
        target = DAYS_OF_WEEK.index(self.day_of_week.upper()) + 1
        delta = target - today
        if delta <= 0:
            delta += 7
        return value + datetime.timedelta(days=delta)

    def __str__(self):
        return "next_day({0}, {1})".format(self.column, self.day_of_week)


class MonthsBetween(Expression):
    def __init__(self, column1, column2, round_off):
        super(MonthsBetween, self).__init__(column1, column2)
        self.column1 = column1
        self.column2 = column2
        self.round_off = round_off

    def eval(self, row, schema):
        value_1 = self.column1.cast(TimestampType()).eval(row, schema)
        value_2 = self.column2.cast(TimestampType()).eval(row, schema)

        if (not isinstance(value_1, datetime.datetime)
                or not isinstance(value_2, datetime.datetime)):
            return None

        one_day = datetime.timedelta(days=1)
        value_1_is_the_last_of_its_month = (value_1.month != (value_1 + one_day).month)
        value_2_is_the_last_of_its_month = (value_2.month != (value_2 + one_day).month)
        if value_1.day == value_2.day or (
                value_1_is_the_last_of_its_month and
                value_2_is_the_last_of_its_month
        ):
            # Special cases where time of day is not consider
            diff = ((value_1.year - value_2.year) * 12 +
                    (value_1.month - value_2.month))
        else:
            day_offset = (value_1.day - value_2.day +
                          (value_1.hour - value_2.hour) / 24 +
                          (value_1.minute - value_2.minute) / 1440 +
                          (value_1.second - value_2.second) / 86400)
            diff = ((value_1.year - value_2.year) * 12 +
                    (value_1.month - value_2.month) * 1 +
                    day_offset / 31)
        if self.round_off:
            return float(round(diff, 8))
        return float(diff)

    def __str__(self):
        return "months_between({0}, {1}, {2})".format(
            self.column1,
            self.column2,
            str(self.round_off).lower()
        )


class DateDiff(Expression):
    def __init__(self, column1, column2):
        super(DateDiff, self).__init__(column1, column2)
        self.column1 = column1
        self.column2 = column2

    def eval(self, row, schema):
        value_1 = self.column1.cast(DateType()).eval(row, schema)
        value_2 = self.column2.cast(DateType()).eval(row, schema)

        if (not isinstance(value_1, datetime.date)
                or not isinstance(value_2, datetime.date)):
            return None

        return (value_1 - value_2).days

    def __str__(self):
        return "datediff({0}, {1})".format(self.column1, self.column2)

