from .session import SparkSession


class SQLContext:
    _instantiatedContext = None

    def __init__(self, sparkContext, sparkSession=None, jsqlContext=None):
        self._sc = sparkContext
        if sparkSession is None:
            sparkSession = SparkSession.builder.getOrCreate()
        self.sparkSession = sparkSession
        if SQLContext._instantiatedContext is None:
            SQLContext._instantiatedContext = self

    @classmethod
    def getOrCreate(cls, sc):
        """
        Get the existing SQLContext or create a new one with given Context.

        :param sc: Context
        """
        if cls._instantiatedContext is None:
            cls(sc, SparkSession(sc), None)
        return cls._instantiatedContext

    def newSession(self):
        """
        Returns a new SQLContext as new session, that has separate SQLConf,
        registered temporary views and UDFs, but shared Context and
        table cache.
        """
        return self.__class__(self._sc, self.sparkSession.newSession())

    def setConf(self, key, value):
        """Sets the given Spark SQL configuration property.
        """
        self.sparkSession.conf.set(key, value)
