import json

from .._json import get_json_encoder
from ..internal_utils.options import Options
from ..internal_utils.readers.jsonreader import JSONReader
from .expressions import Expression


class StructsToJson(Expression):
    pretty_name = "structstojson"

    default_options = dict(
        dateFormat="yyyy-MM-dd",
        timestampFormat="yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    )

    def __init__(self, column, options):
        super().__init__(column)
        self.column = column
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

    def args(self):
        if self.input_options is None:
            return (self.column, )
        return (
            self.column,
            self.input_options
        )


__all__ = ["StructsToJson"]
