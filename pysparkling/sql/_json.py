import collections
import json
import datetime

from .casts import get_time_formatter
from ._row import Row


def get_json_encoder(options):
    """
    Returns a JsonEncoder which convert Rows to json with the same behavior
    and conversion logic as PySpark.

    :return: type
    """
    date_format = options.get("dateformat", "yyyy-MM-dd")
    timestamp_format = options.get("timestampformat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    date_formatter = get_time_formatter(date_format)
    timestamp_formatter = get_time_formatter(timestamp_format)

    class CustomJSONEncoder(json.JSONEncoder):
        def encode(self, o):
            def encode_rows(item):
                if isinstance(item, Row):
                    return collections.OrderedDict(
                        (key, encode_rows(value))
                        for key, value in zip(item.__fields__, item)
                    )
                if isinstance(item, (list, tuple)):
                    return [encode_rows(e) for e in item]
                if isinstance(item, dict):
                    return collections.OrderedDict(
                        (key, encode_rows(value))
                        for key, value in item.items()
                    )
                return item

            return super().encode(encode_rows(o))

        # default can be overridden if passed a parameter during init
        # pylint doesn't like the behavior but it is the expected one
        # pylint: disable=E0202
        def default(self, o):
            if isinstance(o, datetime.datetime):
                return timestamp_formatter(o)
            if isinstance(o, datetime.date):
                return date_formatter(o)
            return super().default(o)

    return CustomJSONEncoder
