from .._date_time import NO_TIMESTAMP_CONVERSION
from ..internal_utils.options import Options
from ..internal_utils.readers.csvreader import csv_record_to_row, CSVReader
from ..internal_utils.readers.utils import guess_schema_from_strings
from ..utils import AnalysisException
from .expressions import Expression

sql_csv_function_options = dict(
    dateFormat=NO_TIMESTAMP_CONVERSION,
    timestampFormat=NO_TIMESTAMP_CONVERSION,
)


class SchemaOfCsv(Expression):
    pretty_name = "schema_of_csv"

    def __init__(self, column, options):
        super().__init__(column)
        self.column = column
        self.input_options = options
        self.options = Options(CSVReader.default_options, sql_csv_function_options, options)

    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        if not isinstance(value, str) or value == "":
            raise AnalysisException(
                "type mismatch: The input csv should be a string literal and not null; "
                f"however, got {value}."
            )
        record_as_row = csv_record_to_row(value, self.options)
        schema = guess_schema_from_strings(record_as_row.__fields__, [record_as_row], self.options)
        return schema.simpleString()

    def args(self):
        return (self.column,)
