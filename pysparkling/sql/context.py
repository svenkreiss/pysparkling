from pysparkling.sql.session import SparkSession


class SQLContext(object):
    _instantiatedContext = None

    def __init__(self, sparkContext, sparkSession=None, jsqlContext=None):
        self._sc = sparkContext
        if sparkSession is None:
            sparkSession = SparkSession.builder.getOrCreate()
        self.sparkSession = sparkSession
        if SQLContext._instantiatedContext is None:
            SQLContext._instantiatedContext = self
