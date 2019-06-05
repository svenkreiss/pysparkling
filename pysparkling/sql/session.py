import sys
from functools import reduce
from threading import RLock

from pyspark.sql.types import _parse_datatype_string, StructType, _make_type_verifier, DataType, \
    _create_converter, _infer_schema, _has_nulltype, _merge_type

import pysparkling
from pysparkling import RDD
from pysparkling.sql.conf import RuntimeConfig
from pysparkling.sql.internals import DataFrameInternal
from pysparkling.sql.dataframe import DataFrame
from pysparkling.sql.readwriter import DataFrameReader

if sys.version >= '3':
    basestring = unicode = str
    xrange = range
else:
    from itertools import izip as zip, imap as map


class SparkSession(object):
    class Builder(object):
        _lock = RLock()

        def getOrCreate(self):
            with self._lock:
                from pysparkling.context import Context
                session = SparkSession._instantiatedSession
                if session is None:
                    session = SparkSession(Context())
                return session

    _instantiatedSession = None
    _activeSession = None

    builder = Builder()

    def __init__(self, sparkContext, jsparkSession=None):
        from pysparkling.sql.context import SQLContext
        self._sc = sparkContext
        self._wrapped = SQLContext(self._sc, self)
        SparkSession._instantiatedSession = self
        SparkSession._activeSession = self

    def newSession(self):
        """
        Returns a new SparkSession as new session, that has separate SQLConf,
        registered temporary views and UDFs, but shared SparkContext and
        table cache.
        """
        return self.__class__(self._sc, self._jsparkSession.newSession())

    @classmethod
    def getActiveSession(cls):
        return SparkSession._activeSession

    @property
    def sparkContext(self):
        """Returns the underlying Context."""
        return self._sc

    @property
    def version(self):
        return pysparkling.__version__

    @property
    def conf(self):
        """Runtime configuration interface for Spark.

        This is the interface through which the user can get and set all Spark and Hadoop
        configurations that are relevant to Spark SQL. When getting the value of a config,
        this defaults to the value set in the underlying :class:`SparkContext`, if any.
        """
        if not hasattr(self, "_conf"):
            # Compatibility with Pyspark behavior
            # noinspection PyAttributeOutsideInit
            self._conf = RuntimeConfig()
        return self._conf

    @property
    def catalog(self):
        """Interface through which the user may create, drop, alter or query underlying
        databases, tables, functions etc.

        :return: :class:`Catalog`
        """
        from pysparkling.sql.catalog import Catalog
        if not hasattr(self, "_catalog"):
            # Compatibility with Pyspark behavior
            # noinspection PyAttributeOutsideInit
            self._catalog = Catalog(self)
        return self._catalog

    @property
    def udf(self):
        # todo: *
        return ...
        # from pysparkling.sql.udf import UDFRegistration
        # return UDFRegistration(self)

    def _inferSchemaFromList(self, data, names=None):
        """
        Infer schema from list of Row or tuple.

        :param data: list of Row or tuple
        :param names: list of column names
        :return: :class:`pyspark.sql.types.StructType`
        """
        if not data:
            raise ValueError("can not infer schema from empty dataset")
        first = data[0]
        if type(first) is dict:
            raise NotImplementedError("Inferring schema from dict is deprecated in pyspark "
                                      "and not implemented in pysparkling. "
                                      "Use .sql.Row instead")
        schema = reduce(_merge_type, (_infer_schema(row, names) for row in data))
        if _has_nulltype(schema):
            raise ValueError("Some of types cannot be determined after inferring")
        return schema

    def _inferSchema(self, rdd, samplingRatio=None, names=None):
        """
        Infer schema from an RDD of Row or tuple.

        :param rdd: an RDD of Row or tuple
        :param samplingRatio: sampling ratio, or no sampling (default)
        :return: :class:`pyspark.sql.types.StructType`
        """
        first = rdd.first()
        if not first:
            raise ValueError("The first row in RDD is empty, "
                             "can not infer schema")
        if type(first) is dict:
            raise NotImplementedError("Using RDD of dict to inferSchema is deprecated in pyspark "
                                      "and not implemented in pysparkling. "
                                      "Use .sql.Row instead")

        if samplingRatio is None:
            schema = _infer_schema(first, names=names)
            if _has_nulltype(schema):
                for row in rdd.take(100)[1:]:
                    schema = _merge_type(schema, _infer_schema(row, names=names))
                    if not _has_nulltype(schema):
                        break
                else:
                    raise ValueError("Some of types cannot be determined by the "
                                     "first 100 rows, please try again with sampling")
        else:
            if samplingRatio < 0.99:
                rdd = rdd.sample(False, float(samplingRatio))
            schema = rdd.map(lambda r: _infer_schema(r, names)).reduce(_merge_type)
        return schema

    def _createFromRDD(self, rdd, schema, samplingRatio):
        """
        Create an RDD for DataFrame from an existing RDD, returns the RDD and schema.
        """
        if schema is None or isinstance(schema, (list, tuple)):
            struct = self._inferSchema(rdd, samplingRatio, names=schema)
            converter = _create_converter(struct)
            rdd = rdd.map(converter)
            if isinstance(schema, (list, tuple)):
                for i, name in enumerate(schema):
                    struct.fields[i].name = name
                    struct.names[i] = name
            schema = struct

        elif not isinstance(schema, StructType):
            raise TypeError("schema should be StructType or list or None, but got: %s" % schema)

        # convert python objects to sql data
        rdd = rdd.map(schema.toInternal)
        return rdd, schema

    def _createFromLocal(self, data, schema):
        """
        Create an RDD for DataFrame from a list or pandas.DataFrame, returns
        the RDD and schema.
        """
        # make sure data could consumed multiple times
        if not isinstance(data, list):
            data = list(data)

        if schema is None or isinstance(schema, (list, tuple)):
            struct = self._inferSchemaFromList(data, names=schema)
            converter = _create_converter(struct)
            data = map(converter, data)
            if isinstance(schema, (list, tuple)):
                for i, name in enumerate(schema):
                    struct.fields[i].name = name
                    struct.names[i] = name
            schema = struct

        elif not isinstance(schema, StructType):
            raise TypeError("schema should be StructType or list or None, but got: %s" % schema)

        # convert python objects to sql data
        data = [schema.toInternal(row) for row in data]
        return self._sc.parallelize(data), schema

    # noinspection PyMethodMayBeStatic
    def _get_numpy_record_dtype(self, rec):
        import numpy as np
        cur_dtypes = rec.dtype
        col_names = cur_dtypes.names
        record_type_list = []
        has_rec_fix = False
        for i in xrange(len(cur_dtypes)):
            curr_type = cur_dtypes[i]
            # If type is a datetime64 timestamp, convert to microseconds
            # NOTE: if dtype is datetime[ns] then np.record.tolist() will output values as longs,
            # conversion from [us] or lower will lead to py datetime objects, see SPARK-22417
            if curr_type == np.dtype('datetime64[ns]'):
                curr_type = 'datetime64[us]'
                has_rec_fix = True
            record_type_list.append((str(col_names[i]), curr_type))
        return np.dtype(record_type_list) if has_rec_fix else None

    def _convert_from_pandas(self, pdf, schema, timezone):
        if timezone is not None:
            raise NotImplementedError("Pandas respect session timezone is not supported")

        # Convert pandas.DataFrame to list of numpy records
        np_records = pdf.to_records(index=False)

        # Check if any columns need to be fixed for Spark to infer properly
        if len(np_records) > 0:
            record_dtype = self._get_numpy_record_dtype(np_records[0])
            if record_dtype is not None:
                return [r.astype(record_dtype).tolist() for r in np_records]

        # Convert list of numpy records to python lists
        return [r.tolist() for r in np_records]

    def createDataFrame(self, data, schema=None, samplingRatio=None, verifySchema=True):
        SparkSession._activeSession = self

        if isinstance(data, DataFrame):
            raise TypeError("data is already a DataFrame")

        if isinstance(schema, basestring):
            schema = _parse_datatype_string(schema)
        elif isinstance(schema, (list, tuple)):
            # Must re-encode any unicode strings to be consistent with StructField names
            schema = [x.encode('utf-8') if not isinstance(x, str) else x for x in schema]

        try:
            import pandas
            has_pandas = True
        except ModuleNotFoundError:
            has_pandas = False

        if has_pandas and isinstance(data, pandas.DataFrame):
            from pyspark.sql.utils import require_minimum_pandas_version
            require_minimum_pandas_version()

            # todo: Add support of pandasRespectSessionTimeZone
            # if self._wrapped._conf.pandasRespectSessionTimeZone():
            #     timezone = self._wrapped._conf.sessionLocalTimeZone()
            # else:
            timezone = None

            # If no schema supplied by user then get the names of columns only
            if schema is None:
                schema = [str(x) if not isinstance(x, basestring) else
                          (x.encode('utf-8') if not isinstance(x, str) else x)
                          for x in data.columns]

            data = self._convert_from_pandas(data, schema, timezone)

        if isinstance(schema, StructType):
            verify_func = _make_type_verifier(schema) if verifySchema else lambda _: True

            def prepare(obj):
                verify_func(obj)
                return obj

        elif isinstance(schema, DataType):
            dataType = schema
            schema = StructType().add("value", schema)

            verify_func = _make_type_verifier(dataType, name="field value") if verifySchema else lambda _: True

            def prepare(obj):
                verify_func(obj)
                return obj,
        else:
            def prepare(obj):
                return obj

        if isinstance(data, RDD):
            rdd, schema = self._createFromRDD(data.map(prepare), schema, samplingRatio)
        else:
            rdd, schema = self._createFromLocal(map(prepare, data), schema)

        cols = [
            col_type.name if hasattr(col_type, "name") else "_" + str(i)
            for i, col_type in enumerate(schema)
        ]
        df = DataFrame(DataFrameInternal(self._sc, rdd, cols, True), self._wrapped)
        df._schema = schema
        return df

    def range(self, start, end=None, step=1, numPartitions=None):
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism

        idf = DataFrameInternal.range(self.sparkContext, start, end, step, numPartitions)
        return DataFrame(idf, self._wrapped)

    @property
    def read(self):
        return DataFrameReader(self)
