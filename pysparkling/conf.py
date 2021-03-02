__all__ = ['SparkConf']


class SparkConf:

    """
    Configuration for a Spark application. Used to set various Spark
    parameters as key-value pairs.

    Most of the time, you would create a SparkConf object with
    ``SparkConf()``, which will load values from `spark.*` Java system
    properties as well. In this case, any parameters you set directly on
    the :class:`SparkConf` object take priority over system properties.

    For unit tests, you can also call ``SparkConf(false)`` to skip
    loading external settings and get the same configuration no matter
    what the system properties are.

    All setter methods in this class support chaining. For example,
    you can write ``conf.setMaster("local").setAppName("My app")``.

    Parameters
    ----------
    loadDefaults : bool
        whether to load values from Java system properties (True by default)

    Notes
    -----
    Once a SparkConf object is passed to Spark, it is cloned
    and can no longer be modified by the user.

    Examples
    --------
    >>> from pysparkling.conf import SparkConf
    >>> from pysparkling.context import SparkContext
    >>> conf = SparkConf()
    >>> conf.setMaster("local").setAppName("My app")  # doctest: +ELLIPSIS
    <pysparkling.conf.SparkConf object at ...>
    >>> conf.get("spark.master")
    'local'
    >>> conf.get("spark.app.name")
    'My app'
    >>> sc = SparkContext(conf=conf)
    >>> sc.master
    'local'
    >>> sc.appName
    'My app'
    >>> sc.sparkHome is None
    True

    >>> conf = SparkConf(loadDefaults=False)
    >>> conf.setSparkHome("/path")  # doctest: +ELLIPSIS
    <pysparkling.conf.SparkConf object at ...>
    >>> conf.get("spark.home")
    '/path'
    >>> conf.setExecutorEnv("VAR1", "value1")  # doctest: +ELLIPSIS
    <pysparkling.conf.SparkConf object at ...>
    >>> conf.setExecutorEnv(pairs = [("VAR3", "value3"), ("VAR4", "value4")])  # doctest: +ELLIPSIS
    <pysparkling.conf.SparkConf object at ...>
    >>> conf.get("spark.executorEnv.VAR1")
    'value1'
    >>> print(conf.toDebugString())
    spark.home=/path
    spark.executorEnv.VAR1=value1
    spark.executorEnv.VAR3=value3
    spark.executorEnv.VAR4=value4
    >>> for p in sorted(conf.getAll(), key=lambda p: p[0]):
    ...     print(p)
    ('spark.executorEnv.VAR1', 'value1')
    ('spark.executorEnv.VAR3', 'value3')
    ('spark.executorEnv.VAR4', 'value4')
    ('spark.home', '/path')
    >>> print(conf.toDebugString())
    spark.home=/path
    spark.executorEnv.VAR1=value1
    spark.executorEnv.VAR3=value3
    spark.executorEnv.VAR4=value4
    """

    def __init__(self, loadDefaults=True, _jvm=None, _jconf=None):
        """
        Create a new Spark configuration.
        """
        if _jconf:
            self._jconf = _jconf
        else:
            self._jconf = None
            self._conf = {}

    def set(self, key, value):
        """Set a configuration property."""
        # Try to set self._jconf first if JVM is created, set self._conf if JVM is not created yet.
        if self._jconf is not None:
            self._jconf.set(key, str(value))
        else:
            self._conf[key] = str(value)
        return self

    def setIfMissing(self, key, value):
        """Set a configuration property, if not already set."""
        if self.get(key) is None:
            self.set(key, value)
        return self

    def setMaster(self, value):
        """Set master URL to connect to."""
        self.set("spark.master", value)
        return self

    def setAppName(self, value):
        """Set application name."""
        self.set("spark.app.name", value)
        return self

    def setSparkHome(self, value):
        """Set path where Spark is installed on worker nodes."""
        self.set("spark.home", value)
        return self

    def setExecutorEnv(self, key=None, value=None, pairs=None):
        """Set an environment variable to be passed to executors."""
        if (key is not None and pairs is not None) or (key is None and pairs is None):
            raise Exception("Either pass one key-value pair or a list of pairs")

        if key is not None:
            self.set("spark.executorEnv." + key, value)
        elif pairs is not None:
            for (k, v) in pairs:
                self.set("spark.executorEnv." + k, v)

        return self

    def setAll(self, pairs):
        """
        Set multiple parameters, passed as a list of key-value pairs.

        Parameters
        ----------
        pairs : iterable of tuples
            list of key-value pairs to set
        """
        for (k, v) in pairs:
            self.set(k, v)
        return self

    def get(self, key, defaultValue=None):
        """Get the configured value for some key, or return a default otherwise."""
        if defaultValue is None:   # Py4J doesn't call the right get() if we pass None
            if self._jconf is not None:
                if not self._jconf.contains(key):
                    return None
                return self._jconf.get(key)

            if key not in self._conf:
                return None
            return self._conf[key]

        if self._jconf is not None:
            return self._jconf.get(key, defaultValue)

        return self._conf.get(key, defaultValue)

    def getAll(self):
        """Get all values as a list of key-value pairs."""
        if self._jconf is not None:
            return [(elem._1(), elem._2()) for elem in self._jconf.getAll()]

        return self._conf.items()

    def contains(self, key):
        """Does this configuration contain a given key?"""
        if self._jconf is not None:
            return self._jconf.contains(key)

        return key in self._conf

    def toDebugString(self):
        """
        Returns a printable version of the configuration, as a list of
        key=value pairs, one per line.
        """
        if self._jconf is not None:
            return self._jconf.toDebugString()

        return '\n'.join('%s=%s' % (k, v) for k, v in self._conf.items())
