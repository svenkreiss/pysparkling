from threading import RLock

from ..__version__ import __version__
from ..context import Context
from ..rdd import RDD
from .conf import RuntimeConfig
from .dataframe import DataFrame
from .internals import DataFrameInternal
from .pandas.utils import require_minimum_pandas_version
from .readwriter import DataFrameReader
from .schema_utils import infer_schema_from_list
from .types import (
    _create_converter, _has_nulltype, _infer_schema, _make_type_verifier, _merge_type, DataType, StructType
)

__all__ = ['SparkSession']


def _monkey_patch_RDD(sparkSession):
    def toDF(self, schema=None, sampleRatio=None):
        """
        Converts current :class:`RDD` into a :class:`DataFrame`

        This is a shorthand for ``spark.createDataFrame(rdd, schema, sampleRatio)``

        Parameters
        ----------
        schema : :class:`pyspark.sql.types.DataType`, str or list, optional
            a :class:`pyspark.sql.types.DataType` or a datatype string or a list of
            column names, default is None.  The data type string format equals to
            :class:`pyspark.sql.types.DataType.simpleString`, except that top level struct type can
            omit the ``struct<>`` and atomic types use ``typeName()`` as their format, e.g. use
            ``byte`` instead of ``tinyint`` for :class:`pyspark.sql.types.ByteType`.
            We can also use ``int`` as a short name for :class:`pyspark.sql.types.IntegerType`.
        sampleRatio : float, optional
            the sample ratio of rows used for inferring

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> rdd.toDF().collect()
        [Row(name='Alice', age=1)]
        """
        return sparkSession.createDataFrame(self, schema, sampleRatio)

    RDD.toDF = toDF


class SparkSession:
    class Builder:
        _lock = RLock()
        _options = {}

        def config(self, key=None, value=None, conf=None):
            """Sets a config option. Options set using this method are automatically propagated to
            both :class:`SparkConf` and :class:`SparkSession`'s own configuration.

            .. versionadded:: 2.0.0

            Parameters
            ----------
            key : str, optional
                a key name string for configuration property
            value : str, optional
                a value for configuration property
            conf : :class:`SparkConf`, optional
                an instance of :class:`SparkConf`

            Examples
            --------
            For an existing SparkConf, use `conf` parameter.

            >>> from pyspark.conf import SparkConf
            >>> SparkSession.builder.config(conf=SparkConf())  # doctest: +ELLIPSIS
            <pysparkling.sql.session...

            For a (key, value) pair, you can omit parameter names.

            >>> SparkSession.builder.config("spark.some.config.option", "some-value")  # doctest: +ELLIPSIS
            <pysparkling.sql.session...

            """
            with self._lock:
                if conf is None:
                    self._options[key] = str(value)
                else:
                    for (k, v) in conf.getAll():
                        self._options[k] = v
                return self

        def master(self, master):
            """Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]"
            to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone
            cluster.

            .. versionadded:: 2.0.0

            Parameters
            ----------
            master : str
                a url for spark master
            """
            return self.config("spark.master", master)

        def appName(self, name):
            """Sets a name for the application, which will be shown in the Spark web UI.

            If no application name is set, a randomly generated name will be used.

            .. versionadded:: 2.0.0

            Parameters
            ----------
            name : str
                an application name
            """
            return self.config("spark.app.name", name)

        def getOrCreate(self):
            with self._lock:
                session = SparkSession._instantiatedSession
                if session is None:
                    session = SparkSession(Context())
                return session

    _instantiatedSession = None
    _activeSession = None

    builder = Builder()

    def __init__(self, sparkContext, jsparkSession=None):
        # pylint: disable=import-outside-toplevel, cyclic-import
        from .context import SQLContext

        self._sc = sparkContext
        self._wrapped = SQLContext(self._sc, self)
        SparkSession._instantiatedSession = self
        SparkSession._activeSession = self

        _monkey_patch_RDD(self)

    def newSession(self):
        """
        Returns a new SparkSession as new session, that has separate SQLConf,
        registered temporary views and UDFs, but shared SparkContext and
        table cache.
        """
        return self.__class__(self._sc)

    @classmethod
    def getActiveSession(cls):
        return SparkSession._activeSession

    @property
    def sparkContext(self):
        """Returns the underlying Context."""
        return self._sc

    @property
    def version(self):
        return __version__

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
            # pylint: disable=W0201
            self._conf = RuntimeConfig()
        return self._conf

    @property
    def catalog(self):
        """Interface through which the user may create, drop, alter or query underlying
        databases, tables, functions etc.

        :return: :class:`Catalog`
        """
        # from .catalog import Catalog
        # if not hasattr(self, "_catalog"):
        #     # Compatibility with Pyspark behavior
        #     # noinspection PyAttributeOutsideInit
        #     self._catalog = Catalog(self)
        # return self._catalog
        raise NotImplementedError()

    @property
    def udf(self):
        # pylint: disable=W0511
        # todo: Add support of udf registration
        raise NotImplementedError("Pysparkling does not support yet catalog")
        # from .udf import UDFRegistration
        # return UDFRegistration(self)

    def _inferSchema(self, rdd, samplingRatio=None, names=None):
        """
        Infer schema from an RDD of Row or tuple.

        :param rdd: an RDD of Row or tuple
        :param samplingRatio: sampling ratio, or no sampling (default)
        :return: :class:`.types.StructType`
        """
        first = rdd.first()
        if not first:
            raise ValueError("The first row in RDD is empty, "
                             "can not infer schema")
        if isinstance(first, dict):
            raise NotImplementedError(
                "Using RDD of dict to inferSchema is deprecated in Spark "
                "and not implemented in pysparkling. "
                "Please use .sql.Row instead"
            )

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
            struct = infer_schema_from_list(data, names=schema)
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
        # numpy is an optional dependency
        # pylint: disable=import-outside-toplevel
        import numpy as np
        cur_dtypes = rec.dtype
        col_names = cur_dtypes.names
        record_type_list = []
        has_rec_fix = False
        for i, field in enumerate(cur_dtypes.fields.values()):
            curr_type = field[0]
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
            raise NotImplementedError("Pandas with session timezone respect is not supported")

        # Convert pandas.DataFrame to list of numpy records
        np_records = pdf.to_records(index=False)

        # Check if any columns need to be fixed for Spark to infer properly
        if np_records.size > 0:
            record_dtype = self._get_numpy_record_dtype(np_records[0])
            if record_dtype is not None:
                return [r.astype(record_dtype).tolist() for r in np_records]

        # Convert list of numpy records to python lists
        return [r.tolist() for r in np_records]

    def createDataFrame(self, data, schema=None, samplingRatio=None, verifySchema=True):
        SparkSession._activeSession = self

        if isinstance(data, DataFrame):
            raise TypeError("data is already a DataFrame")

        if isinstance(schema, str):
            schema = StructType.fromDDL(schema)
        elif isinstance(schema, (list, tuple)):
            # Must re-encode any unicode strings to be consistent with StructField names
            schema = [x.encode('utf-8') if not isinstance(x, str) else x for x in schema]

        try:
            # pandas is an optional dependency
            # pylint: disable=import-outside-toplevel
            import pandas
        except ImportError:
            pass
        else:
            if isinstance(data, pandas.DataFrame):
                data, schema = self.parse_pandas_dataframe(data, schema)

        no_check = lambda _: True
        if isinstance(schema, StructType):
            verify_func = _make_type_verifier(schema) if verifySchema else no_check

            def prepare(obj):
                verify_func(obj)
                return obj

        elif isinstance(schema, DataType):
            dataType = schema
            schema = StructType().add("value", schema)

            verify_func = _make_type_verifier(
                dataType, name="field value"
            ) if verifySchema else no_check

            def prepare(obj):
                verify_func(obj)
                return tuple([obj])
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
        df = DataFrame(DataFrameInternal(self._sc, rdd, cols, True, schema), self._wrapped)
        return df

    def parse_pandas_dataframe(self, data, schema):
        require_minimum_pandas_version()
        # pylint: disable=W0511
        # todo: Add support of pandasRespectSessionTimeZone
        # if self._wrapped._conf.pandasRespectSessionTimeZone():
        #     timezone = self._wrapped._conf.sessionLocalTimeZone()
        # else:
        timezone = None
        # If no schema supplied by user then get the names of columns only
        if schema is None:
            schema = [str(x) for x in data.columns]
        data = self._convert_from_pandas(data, schema, timezone)
        return data, schema

    def range(self, start, end=None, step=1, numPartitions=None):
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism

        idf = DataFrameInternal.range(self.sparkContext, start, end, step, numPartitions)
        return DataFrame(idf, self._wrapped)

    @property
    def read(self):
        return DataFrameReader(self)
