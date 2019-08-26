from collections import namedtuple

from pysparkling.sql.types import StructType

from pysparkling.sql.dataframe import DataFrame

# External structures
Database = namedtuple("Database", ["name", "description", "locationUri"])
Table = namedtuple("Table", ["name", "database", "description", "tableType", "isTemporary"])
Column = namedtuple("Column", ["name", "description", "dataType", "nullable", "isPartition", "isBucket"])
Function = namedtuple("Function", ["name", "description", "className", "isTemporary"])

# Internal structures
CatalogInfo = namedtuple("CatalogInfo", ["databases", "current_database"])
DatabaseInfo = namedtuple("DatabaseInfo", ["tables", "functions"])
TableInfo = namedtuple("TableInfo", ["columns"])


# noinspection PyProtectedMember
class Catalog(object):
    """User-facing catalog API, accessible through `SparkSession.catalog`.

    This is a thin wrapper around its Scala implementation org.apache.spark.sql.catalog.Catalog.
    """

    _catalog = CatalogInfo(databases={}, current_database=None)

    def __init__(self, sparkSession):
        """Create a new Catalog that wraps the underlying JVM object."""
        self._sparkSession = sparkSession

    def currentDatabase(self):
        """Returns the current default database in this session."""
        return self._catalog.current_database

    def setCurrentDatabase(self, dbName):
        """Sets the current default database in this session."""
        self._catalog.current_database = dbName

    def listDatabases(self):
        """Returns a list of databases available across all sessions."""
        return [
            Database(
                name=db.name(),
                description=db.description(),
                locationUri=db.locationUri())
            for db_name, db in self._catalog.databases
        ]

    def listTables(self, dbName=None):
        """Returns a list of tables/views in the specified database.

        If no database is specified, the current database is used.
        This includes all temporary views.
        """
        if dbName is None:
            dbName = self.currentDatabase()

        if dbName in self._catalog.databases:
            database = self._catalog.databases[dbName]
        else:
            raise Exception

        return [
            Table(
                name=table.name(),
                database=table.database(),
                description=table.description(),
                tableType=table.tableType(),
                isTemporary=table.isTemporary())
            for table_name, table in database
        ]

    def listFunctions(self, dbName=None):
        """Returns a list of functions registered in the specified database.

        If no database is specified, the current database is used.
        This includes all temporary functions.
        """
        if dbName is None:
            dbName = self.currentDatabase()
        iter = self._jcatalog.listFunctions(dbName).toLocalIterator()
        functions = []
        while iter.hasNext():
            jfunction = iter.next()
            functions.append(Function(
                name=jfunction.name(),
                description=jfunction.description(),
                className=jfunction.className(),
                isTemporary=jfunction.isTemporary()))
        return functions

    def listColumns(self, tableName, dbName=None):
        """Returns a list of columns for the given table/view in the specified database.

        If no database is specified, the current database is used.

        Note: the order of arguments here is different from that of its JVM counterpart
        because Python does not support method overloading.
        """
        if dbName is None:
            dbName = self.currentDatabase()
        iter = self._jcatalog.listColumns(dbName, tableName).toLocalIterator()
        columns = []
        while iter.hasNext():
            jcolumn = iter.next()
            columns.append(Column(
                name=jcolumn.name(),
                description=jcolumn.description(),
                dataType=jcolumn.dataType(),
                nullable=jcolumn.nullable(),
                isPartition=jcolumn.isPartition(),
                isBucket=jcolumn.isBucket()))
        return columns

    def createExternalTable(self, tableName, path=None, source=None, schema=None, **options):
        """Creates a table based on the dataset in a data source.

        It returns the DataFrame associated with the external table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        Optionally, a schema can be provided as the schema of the returned :class:`DataFrame` and
        created external table.

        :return: :class:`DataFrame`
        """
        raise NotImplementedError(
            "`createExternalTable` is not implemented as deprecated in Spark 2.3.0, use `createTable` instead "
        )

    def createTable(self, tableName, path=None, source=None, schema=None, **options):
        """Creates a table based on the dataset in a data source.

        It returns the DataFrame associated with the table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used. When ``path`` is specified, an external table is
        created from the data at the given path. Otherwise a managed table is created.

        Optionally, a schema can be provided as the schema of the returned :class:`DataFrame` and
        created table.

        :return: :class:`DataFrame`
        """
        if path is not None:
            options["path"] = path
        if source is None:
            source = self._sparkSession._wrapped._conf.defaultDataSourceName()
        if schema is None:
            df = self._jcatalog.createTable(tableName, source, options)
        else:
            if not isinstance(schema, StructType):
                raise TypeError("schema should be StructType")
            scala_datatype = self._jsparkSession.parseDataType(schema.json())
            df = self._jcatalog.createTable(tableName, source, scala_datatype, options)
        return DataFrame(df, self._sparkSession._wrapped)

    def dropTempView(self, viewName):
        """Drops the local temporary view with the given view name in the catalog.
        If the view has been cached before, then it will also be uncached.
        Returns true if this view is dropped successfully, false otherwise.

        Note that, the return type of this method was None in Spark 2.0, but changed to Boolean
        in Spark 2.1.

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> spark.createDataFrame([(1, 1)]).createTempView("my_table")
        >>> spark.table("my_table").collect()
        [Row(_1=1, _2=1)]
        >>> spark.catalog.dropTempView("my_table")
        >>> spark.table("my_table") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        self._jcatalog.dropTempView(viewName)

    def dropGlobalTempView(self, viewName):
        """Drops the global temporary view with the given view name in the catalog.
        If the view has been cached before, then it will also be uncached.
        Returns true if this view is dropped successfully, false otherwise.

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> spark.createDataFrame([(1, 1)]).createGlobalTempView("my_table")
        >>> spark.table("global_temp.my_table").collect()
        [Row(_1=1, _2=1)]
        >>> spark.catalog.dropGlobalTempView("my_table")
        >>> spark.table("global_temp.my_table") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        self._jcatalog.dropGlobalTempView(viewName)

    def registerFunction(self, name, f, returnType=None):
        """An alias for :func:`spark.udf.register`.
        See :meth:`pyspark.sql.UDFRegistration.register`.

        .. note:: Deprecated in 2.3.0. Use :func:`spark.udf.register` instead.
        """
        raise NotImplementedError(
            "`registerFunction` is not implemented as deprecated in Spark 2.3.0, use `spark.udf.register` instead "
        )

    def isCached(self, tableName):
        """Returns true if the table is currently cached in-memory."""
        return self._jcatalog.isCached(tableName)

    def cacheTable(self, tableName):
        """Caches the specified table in-memory."""
        self._jcatalog.cacheTable(tableName)

    def uncacheTable(self, tableName):
        """Removes the specified table from the in-memory cache."""
        self._jcatalog.uncacheTable(tableName)

    def clearCache(self):
        """Removes all cached tables from the in-memory cache."""
        self._jcatalog.clearCache()

    def refreshTable(self, tableName):
        """Invalidates and refreshes all the cached data and metadata of the given table."""
        self._jcatalog.refreshTable(tableName)

    def recoverPartitions(self, tableName):
        """Recovers all the partitions of the given table and update the catalog.

        Only works with a partitioned table, and not a view.
        """
        self._jcatalog.recoverPartitions(tableName)

    def refreshByPath(self, path):
        """Invalidates and refreshes all the cached data (and the associated metadata) for any
        DataFrame that contains the given data source path.
        """
        self._jcatalog.refreshByPath(path)

    def _reset(self):
        """(Internal use only) Drop all existing databases (except "default"), tables,
        partitions and functions, and set the current database to "default".

        This is mainly used for tests.
        """
        self._jsparkSession.sessionState().catalog().reset()
