class CapturedException(Exception):
    pass


class AnalysisException(CapturedException):
    """
    Failed to analyze a SQL query plan.
    """


class ParseException(CapturedException):
    """
    Failed to parse a SQL command.
    """


class IllegalArgumentException(CapturedException):
    """
    Failed to analyze a SQL query plan.
    """


class StreamingQueryException(CapturedException):
    """
    Exception that stopped a :class:`StreamingQuery`.
    """


class QueryExecutionException(CapturedException):
    """
    Failed to execute a query.
    """


class PythonException(CapturedException):
    """
    Exceptions thrown from Python workers.
    """


class UnknownException(CapturedException):
    """
    None of the above exceptions.
    """


def convert_exception(e):
    """In pyspark, this method converts a received string into a Python exception."""
    return


def capture_sql_exception(f):
    """This is a decorator to convert py4j exceptions into nicer Python exceptions."""
    return


def install_exception_handler():
    """Don't implement, but also don't raise."""
    return


def toJArray(gateway, jtype, arr):
    """Convert python list to java type array"""
    return


def require_test_compiled():
    """ Raise Exception if test classes are not compiled"""

    raise RuntimeError("Tests don't exist for pysparkling as they do for Spark.")


class ForeachBatchFunction:
    """
    This is the Python implementation of Java interface 'ForeachBatchFunction'. This wraps
    the user-defined 'foreachBatch' function such that it can be called from the JVM when
    the query is active.
    """
    # ==> Not implemented; do we actually need it?
    # def __init__(self, sql_ctx, func):
    #     self.sql_ctx = sql_ctx
    #     self.func = func
    #
    # def call(self, jdf, batch_id):
    #     from pyspark.sql.dataframe import DataFrame
    #     try:
    #         self.func(DataFrame(jdf, self.sql_ctx), batch_id)
    #     except Exception as e:
    #         self.error = e
    #         raise e
    #
    # class Java:
    #     implements = ['org.apache.spark.sql.execution.streaming.sources.PythonForeachBatchFunction']


def to_str(value):
    """
    A wrapper over str(), but converts bool values to lower case strings.
    If None is given, just returns None, instead of converting it to string "None".
    """
    if isinstance(value, bool):
        return str(value).lower()

    if value is None:
        return value

    return str(value)
