from pysparkling.sql.internal_utils.readers.csv import CSVReader
from pysparkling.sql.internal_utils.readers.json import JSONReader
from pysparkling.sql.internal_utils.readwrite import OptionUtils, to_option_stored_value


class InternalReader(OptionUtils):
    def schema(self, schema):
        self._schema = schema

    def option(self, k, v):
        self._options[k.lower()] = to_option_stored_value(v)

    def __init__(self, spark):
        """

        :type spark: pysparkling.sql.session.SparkSession
        """
        self._spark = spark
        self._options = {}
        self._schema = None

    def csv(self, paths):
        return CSVReader(self._spark, paths, self._schema, self._options).read()

    def json(self, paths):
        return JSONReader(self._spark, paths, self._schema, self._options).read()
