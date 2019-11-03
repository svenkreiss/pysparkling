import json

from pysparkling.sql.casts import get_time_formatter
from pysparkling.sql.expressions.expressions import Expression
from pysparkling.utils import get_json_encoder


class StructsToJson(Expression):
    def __init__(self, column, options):
        super().__init__(column)
        self.column = column
        self.options = options
        if options:
            date_format = options.get("dateformat", "yyyy-MM-dd")
            timestamp_format = options.get("timestampformat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        else:
            date_format = "yyyy-MM-dd"
            timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        self.encoder = get_json_encoder(
            date_formatter=get_time_formatter(date_format),
            timestamp_formatter=get_time_formatter(timestamp_format)
        )

    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        return json.dumps(
            value,
            cls=self.encoder,
            separators=(',', ':')
        )

    def __str__(self):
        return "structstojson({0}{1})".format(
            self.column,
            ", {0}".format(self.options) if self.options is not None else ""
        )
