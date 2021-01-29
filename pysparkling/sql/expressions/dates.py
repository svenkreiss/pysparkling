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
    pretty_name = "add_months"

    def __init__(self, start_date, num_months):
        super().__init__(start_date)
        self.start_date = start_date
        self.num_months = num_months.get_literal_value()
        self.timedelta = datetime.timedelta(days=self.num_months)

    def eval(self, row, schema):
        return self.start_date.cast(DateType()).eval(row, schema) + self.timedelta

    def args(self):
        return (
            self.start_date,
            self.num_months
        )


class DateAdd(Expression):
    pretty_name = "date_add"

    def __init__(self, start_date, num_days):
        super().__init__(start_date)
        self.start_date = start_date
        self.num_days = num_days.get_literal_value()
        self.timedelta = datetime.timedelta(days=self.num_days)

    def eval(self, row, schema):
        return self.start_date.cast(DateType()).eval(row, schema) + self.timedelta

    def args(self):
        return (
            self.start_date,
            self.num_days
        )


class DateSub(Expression):
    pretty_name = "date_sub"

    def __init__(self, start_date, num_days):
        super().__init__(start_date)
        self.start_date = start_date
        self.num_days = num_days.get_literal_value()
        self.timedelta = datetime.timedelta(days=self.num_days)

    def eval(self, row, schema):
        return self.start_date.cast(DateType()).eval(row, schema) - self.timedelta

    def args(self):
        return (
            self.start_date,
            self.num_days
        )


class Year(UnaryExpression):
    pretty_name = "year"

    def eval(self, row, schema):
        return self.column.cast(DateType()).eval(row, schema).year


class Month(UnaryExpression):
    pretty_name = "month"

    def eval(self, row, schema):
        return self.column.cast(DateType()).eval(row, schema).month


class Quarter(UnaryExpression):
    pretty_name = "quarter"

    def eval(self, row, schema):
        month = self.column.cast(DateType()).eval(row, schema).month
        return 1 + int((month - 1) / 3)


class Hour(UnaryExpression):
    pretty_name = "hour"

    def eval(self, row, schema):
        return self.column.cast(TimestampType()).eval(row, schema).hour


class Minute(UnaryExpression):
    pretty_name = "minute"

    def eval(self, row, schema):
        return self.column.cast(TimestampType()).eval(row, schema).minute


class Second(UnaryExpression):
    pretty_name = "second"

    def eval(self, row, schema):
        return self.column.cast(TimestampType()).eval(row, schema).second


class DayOfMonth(UnaryExpression):
    pretty_name = "dayofmonth"

    def eval(self, row, schema):
        return self.column.cast(DateType()).eval(row, schema).day


class DayOfYear(UnaryExpression):
    pretty_name = "dayofyear"

    def eval(self, row, schema):
        value = self.column.cast(DateType()).eval(row, schema)
        day_from_the_first = value - datetime.date(value.year, 1, 1)
        return 1 + day_from_the_first.days


class LastDay(UnaryExpression):
    pretty_name = "last_day"

    def eval(self, row, schema):
        value = self.column.cast(DateType()).eval(row, schema)
        first_of_next_month = value + relativedelta(months=1, day=1)
        return first_of_next_month - datetime.timedelta(days=1)


class WeekOfYear(UnaryExpression):
    pretty_name = "weekofyear"

    def eval(self, row, schema):
        return self.column.cast(DateType()).eval(row, schema).isocalendar()[1]


class DayOfWeek(UnaryExpression):
    pretty_name = "dayofweek"

    def eval(self, row, schema):
        date = self.column.cast(DateType()).eval(row, schema)
        return date.isoweekday() + 1 if date.isoweekday() != 7 else 1


class NextDay(Expression):
    pretty_name = "next_day"

    def __init__(self, column, day_of_week):
        super().__init__(column)
        self.column = column
        self.day_of_week = day_of_week.get_literal_value()

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

    def args(self):
        return (
            self.column,
            self.day_of_week
        )


class MonthsBetween(Expression):
    pretty_name = "months_between"

    def __init__(self, column1, column2, round_off):
        super().__init__(column1, column2)
        self.column1 = column1
        self.column2 = column2
        self.round_off = round_off.get_literal_value()

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
                value_1_is_the_last_of_its_month
                and value_2_is_the_last_of_its_month
        ):
            # Special cases where time of day is not consider
            diff = ((value_1.year - value_2.year) * 12
                    + (value_1.month - value_2.month))
        else:
            day_offset = (value_1.day - value_2.day
                          + (value_1.hour - value_2.hour) / 24
                          + (value_1.minute - value_2.minute) / 1440
                          + (value_1.second - value_2.second) / 86400)
            diff = ((value_1.year - value_2.year) * 12
                    + (value_1.month - value_2.month) * 1
                    + day_offset / 31)
        if self.round_off:
            return float(round(diff, 8))
        return float(diff)

    def args(self):
        return (
            self.column1,
            self.column2,
            str(self.round_off).lower()
        )


class DateDiff(Expression):
    pretty_name = "datediff"

    def __init__(self, column1, column2):
        super().__init__(column1, column2)
        self.column1 = column1
        self.column2 = column2

    def eval(self, row, schema):
        value_1 = self.column1.cast(DateType()).eval(row, schema)
        value_2 = self.column2.cast(DateType()).eval(row, schema)

        if (not isinstance(value_1, datetime.date)
                or not isinstance(value_2, datetime.date)):
            return None

        return (value_1 - value_2).days

    def args(self):
        return (
            self.column1,
            self.column2
        )


class FromUnixTime(Expression):
    pretty_name = "from_unixtime"

    def __init__(self, column, f):
        super().__init__(column)
        self.column = column
        self.format = f.get_literal_value()
        self.formatter = get_time_formatter(self.format)

    def eval(self, row, schema):
        timestamp = self.column.cast(FloatType()).eval(row, schema)
        return self.formatter(datetime.datetime.fromtimestamp(timestamp))

    def args(self):
        return (
            self.column,
            self.format
        )


class DateFormat(Expression):
    pretty_name = "date_format"

    def __init__(self, column, f):
        super().__init__(column)
        self.column = column
        self.format = f.get_literal_value()
        self.formatter = get_time_formatter(self.format)

    def eval(self, row, schema):
        timestamp = self.column.cast(TimestampType()).eval(row, schema)
        return self.formatter(timestamp)

    def args(self):
        return (
            self.column,
            self.format
        )


class CurrentTimestamp(Expression):
    pretty_name = "current_timestamp"

    def __init__(self):
        super().__init__()
        self.current_timestamp = None

    def eval(self, row, schema):
        return self.current_timestamp

    def initialize(self, partition_index):
        super().initialize(partition_index)
        self.current_timestamp = datetime.datetime.now()

    def args(self):
        return ()


class CurrentDate(Expression):
    pretty_name = "current_date"

    def __init__(self):
        super().__init__()
        self.current_timestamp = None

    def eval(self, row, schema):
        return self.current_timestamp.date()

    def initialize(self, partition_index):
        super().initialize(partition_index)
        self.current_timestamp = datetime.datetime.now()

    def args(self):
        return ()


class UnixTimestamp(Expression):
    pretty_name = "unix_timestamp"

    def __init__(self, column, f):
        super().__init__(column)
        self.column = column
        self.format = f.get_literal_value()
        self.parser = get_unix_timestamp_parser(self.format)

    def eval(self, row, schema):
        datetime_as_string = self.column.eval(row, schema)
        return self.parser(datetime_as_string)

    def args(self):
        return (
            self.column,
            self.format
        )


class ParseToTimestamp(Expression):
    pretty_name = "to_timestamp"

    def __init__(self, column, f):
        super().__init__(column)
        self.column = column
        self.format = f.get_literal_value()
        self.parser = get_unix_timestamp_parser(self.format)

    def eval(self, row, schema):
        datetime_as_string = self.column.eval(row, schema)
        return datetime.datetime.fromtimestamp(self.parser(datetime_as_string))

    def args(self):
        if self.format is None:
            return (
                "'{0}'".format(self.column),
            )
        return (
            "'{0}'".format(self.column),
            "'{0}'".format(self.format)
        )


class ParseToDate(Expression):
    pretty_name = "to_date"

    def __init__(self, column, f):
        super().__init__(column)
        self.column = column
        self.format = f.get_literal_value()
        self.parser = get_unix_timestamp_parser(self.format)

    def eval(self, row, schema):
        datetime_as_string = self.column.eval(row, schema)
        return datetime.date.fromtimestamp(self.parser(datetime_as_string))

    def args(self):
        if self.format is None:
            return (
                "'{0}'".format(self.column),
            )
        return (
            "'{0}'".format(self.column),
            "'{0}'".format(self.format)
        )


class TruncDate(Expression):
    pretty_name = "trunc"

    def __init__(self, column, level):
        super().__init__(column)
        self.column = column
        self.level = level.get_literal_value()

    def eval(self, row, schema):
        value = self.column.cast(DateType()).eval(row, schema)
        if self.level in ('year', 'yyyy', 'yy'):
            return datetime.date(value.year, 1, 1)
        if self.level in ('month', 'mon', 'mm'):
            return datetime.date(value.year, value.month, 1)
        return None

    def args(self):
        return (
            self.column,
            self.level
        )


class TruncTimestamp(Expression):
    pretty_name = "date_trunc"

    def __init__(self, level, column):
        super().__init__(column)
        self.level = level.get_literal_value()
        self.column = column

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

    def args(self):
        return (
            self.level,
            self.column
        )


class FromUTCTimestamp(Expression):
    pretty_name = "from_utc_timestamp"

    def __init__(self, column, tz):
        super().__init__(column)
        self.column = column
        self.tz = tz.get_literal_value()
        self.pytz = parse_tz(self.tz)

    def eval(self, row, schema):
        value = self.column.cast(TimestampType()).eval(row, schema)
        if self.pytz is None:
            return value
        gmt_date = GMT_TIMEZONE.localize(value)
        local_date = gmt_date.astimezone(self.pytz)
        return local_date.replace(tzinfo=None)

    def args(self):
        return (
            self.column,
            self.tz
        )


class ToUTCTimestamp(Expression):
    pretty_name = "to_utc_timestamp"

    def __init__(self, column, tz):
        super().__init__(column)
        self.column = column
        self.tz = tz.get_literal_value()
        self.pytz = parse_tz(self.tz)

    def eval(self, row, schema):
        value = self.column.cast(TimestampType()).eval(row, schema)
        if self.pytz is None:
            return value
        local_date = self.pytz.localize(value)
        gmt_date = local_date.astimezone(GMT_TIMEZONE)
        return gmt_date.replace(tzinfo=None)

    def args(self):
        return (
            self.column,
            self.tz
        )


__all__ = [
    "ToUTCTimestamp", "FromUTCTimestamp", "TruncTimestamp", "TruncDate", "ParseToDate",
    "ParseToTimestamp", "UnixTimestamp", "CurrentTimestamp", "FromUnixTime", "WeekOfYear",
    "NextDay", "MonthsBetween", "LastDay", "DayOfYear", "DayOfMonth", "DayOfWeek", "Month",
    "Quarter", "Year", "DateDiff", "DateSub", "DateAdd", "DateFormat", "CurrentDate",
    "AddMonths", "Hour", "Minute", "Second"
]
