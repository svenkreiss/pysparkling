import datetime

import pytz
from dateutil.relativedelta import relativedelta

from pysparkling.sql.casts import get_time_formatter, get_unix_timestamp_parser
from pysparkling.sql.expressions.expressions import Expression, UnaryExpression
from pysparkling.sql.types import DateType, TimestampType, FloatType
from pysparkling.utils import parse_tz

GMT_TIMEZONE = pytz.timezone("GMT")

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


class FromUnixTime(Expression):
    def __init__(self, column, f):
        super(FromUnixTime, self).__init__(column)
        self.column = column
        self.format = f
        self.formatter = get_time_formatter(self.format)

    def eval(self, row, schema):
        timestamp = self.column.cast(FloatType()).eval(row, schema)
        return self.formatter(datetime.datetime.fromtimestamp(timestamp))

    def __str__(self):
        return "from_unixtime({0}, {1})".format(self.column, self.format)


class DateFormat(Expression):
    def __init__(self, column, f):
        super(DateFormat, self).__init__(column)
        self.column = column
        self.format = f
        self.formatter = get_time_formatter(self.format)

    def eval(self, row, schema):
        timestamp = self.column.cast(TimestampType()).eval(row, schema)
        return self.formatter(timestamp)

    def __str__(self):
        return "date_format({0}, {1})".format(self.column, self.format)


class CurrentTimestamp(Expression):
    def __init__(self):
        super(CurrentTimestamp, self).__init__()
        self.current_timestamp = None

    def eval(self, row, schema):
        return self.current_timestamp

    def initialize(self, partition_index):
        super(CurrentTimestamp, self).initialize(partition_index)
        self.current_timestamp = datetime.datetime.now()

    def __str__(self):
        return "current_timestamp()"


class CurrentDate(Expression):
    def __init__(self):
        super(CurrentDate, self).__init__()
        self.current_timestamp = None

    def eval(self, row, schema):
        return self.current_timestamp.date()

    def initialize(self, partition_index):
        super(CurrentDate, self).initialize(partition_index)
        self.current_timestamp = datetime.datetime.now()

    def __str__(self):
        return "current_date()"


class UnixTimestamp(Expression):
    def __init__(self, column, f):
        super(UnixTimestamp, self).__init__(column)
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
        super(ParseToTimestamp, self).__init__(column)
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
        super(ParseToDate, self).__init__(column)
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


class TruncDate(Expression):
    def __init__(self, column, level):
        super(TruncDate, self).__init__(column)
        self.column = column
        self.level = level

    def eval(self, row, schema):
        value = self.column.cast(DateType()).eval(row, schema)
        if self.level in ('year', 'yyyy', 'yy'):
            return datetime.date(value.year, 1, 1)
        if self.level in ('month', 'mon', 'mm'):
            return datetime.date(value.year, value.month, 1)
        return None

    def __str__(self):
        return "trunc({0}, {1})".format(self.column, self.level)


class TruncTimestamp(Expression):
    def __init__(self, level, column):
        super(TruncTimestamp, self).__init__(column)
        self.column = column
        self.level = level

    def eval(self, row, schema):
        value = self.column.cast(TimestampType()).eval(row, schema)

        day_truncation = self.truncate_to_day(value)
        if day_truncation:
            return day_truncation

        time_truncated = self.truncate_to_time(value)
        if time_truncated:
            return time_truncated

        return None

    def truncate_to_day(self, value):
        if self.level in ('year', 'yyyy', 'yy'):
            return datetime.datetime(value.year, 1, 1)
        if self.level in ('month', 'mon', 'mm'):
            return datetime.datetime(value.year, value.month, 1)
        if self.level in ('day', 'dd'):
            return datetime.datetime(value.year, value.month, value.day)
        if self.level in ('quarter',):
            quarter_start_month = int((value.month - 1) / 3) * 3 + 1
            return datetime.datetime(value.year, quarter_start_month, 1)
        if self.level in ('week',):
            return datetime.datetime(
                value.year, value.month, value.day
            ) - datetime.timedelta(days=value.isoweekday() - 1)
        return None

    def truncate_to_time(self, value):
        if self.level in ('hour',):
            return datetime.datetime(value.year, value.month, value.day, value.hour)
        if self.level in ('minute',):
            return datetime.datetime(value.year, value.month, value.day, value.hour, value.minute)
        if self.level in ('second',):
            return datetime.datetime(
                value.year, value.month, value.day, value.hour, value.minute, value.second
            )
        return None

    def __str__(self):
        return "date_trunc({0}, {1})".format(self.level, self.column)


class FromUTCTimestamp(Expression):
    def __init__(self, column, tz):
        super(FromUTCTimestamp, self).__init__(column)
        self.column = column
        self.tz = tz
        self.pytz = parse_tz(tz)

    def eval(self, row, schema):
        value = self.column.cast(TimestampType()).eval(row, schema)
        if self.pytz is None:
            return value
        gmt_date = GMT_TIMEZONE.localize(value)
        local_date = gmt_date.astimezone(self.pytz)
        return local_date.replace(tzinfo=None)

    def __str__(self):
        return "from_utc_timestamp({0}, {1})".format(self.column, self.tz)
