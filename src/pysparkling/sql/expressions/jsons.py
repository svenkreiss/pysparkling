import json

from pysparkling.sql.expressions.expressions import Expression
from pysparkling.sql.internal_utils.options import Options
from pysparkling.utils import get_json_encoder


class StructsToJson(Expression):
    default_options = dict(
        dateFormat="yyyy-MM-dd",
        timestampFormat="yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    )

    def __init__(self, column, options):
        super(StructsToJson, self).__init__(column)
        self.column = column
        # pylint: disable=import-outside-toplevel; circular import
        from pysparkling.sql.internal_utils.readers.jsonreader import JSONReader
        self.input_options = options
        self.options = Options(JSONReader.default_options, options)
        self.encoder = get_json_encoder(self.options)

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
            ", {0}".format(self.input_options) if self.input_options is not None else ""
        )


__all__ = ["StructsToJson"]
