import warnings

_NoValue = object()


class DataFrame(object):
    def __init__(self, jdf, sql_ctx):
        self._jdf = jdf
        self.sql_ctx = sql_ctx

    def collect(self):
        """Returns the number of rows in this :class:`DataFrame`.

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(2)
        >>> df.collect()
        [Row(id=0), Row(id=1)]
        """
        return self._jdf.collect()

    @property
    def rdd(self):
        return self._jdf.rdd()

    @property
    def is_cached(self):
        return self._jdf.is_cached()

    @property
    def na(self):
        """Returns a :class:`DataFrameNaFunctions` for handling missing values.
        """
        return DataFrameNaFunctions(self)

    def dropna(self, how='any', thresh=None, subset=None):
        if how is not None and how not in ['any', 'all']:
            raise ValueError("how ('" + how + "') should be 'any' or 'all'")

        if subset is None:
            subset = self.columns
        elif isinstance(subset, str):
            subset = [subset]
        elif not isinstance(subset, (list, tuple)):
            raise ValueError("subset should be a list or tuple of column names")

        if thresh is None:
            thresh = len(subset) if how == 'any' else 1

        return DataFrame(self._jdf.dropna(thresh, subset), self.sql_ctx)

    def fillna(self, value, subset=None):
        if not isinstance(value, (float, int, str, bool, dict)):
            raise ValueError("value should be a float, int, long, string, bool or dict")

        # Note that bool validates isinstance(int), but we don't want to
        # convert bools to floats

        if not isinstance(value, bool) and isinstance(value, int):
            value = float(value)

        if isinstance(value, dict):
            return DataFrame(self._jdf.fillna(value), self.sql_ctx)
        if subset is None:
            return DataFrame(self._jdf.fillna(value), self.sql_ctx)
        if isinstance(subset, str):
            subset = [subset]
        elif not isinstance(subset, (list, tuple)):
            raise ValueError("subset should be a list or tuple of column names")

        return DataFrame(self._jdf.fillna(value, subset), self.sql_ctx)

    def replace(self, to_replace, value=_NoValue, subset=None):
        # Helper functions
        def all_of(types):
            def all_of_(xs):
                return all(isinstance(x, types) for x in xs)

            return all_of_

        all_of_bool = all_of(bool)
        all_of_str = all_of(str)
        all_of_numeric = all_of((float, int))

        value = self._check_replace_inputs(subset, to_replace, value)

        # Reshape input arguments if necessary
        if isinstance(to_replace, (float, int, str)):
            to_replace = [to_replace]

        if isinstance(to_replace, dict):
            rep_dict = to_replace
            if value is not None:
                warnings.warn("to_replace is a dict and value is not None. value will be ignored.")
        else:
            if isinstance(value, (float, int, str)) or value is None:
                value = [value for _ in range(len(to_replace))]
            rep_dict = dict(zip(to_replace, value))

        if isinstance(subset, str):
            subset = [subset]

        # Verify we were not passed in mixed type generics.
        if not any(all_of_type(rep_dict.keys())
                   and all_of_type(x for x in rep_dict.values() if x is not None)
                   for all_of_type in [all_of_bool, all_of_str, all_of_numeric]):
            raise ValueError("Mixed type replacements are not supported")

        if subset is None:
            return DataFrame(self._jdf.replace('*', rep_dict), self.sql_ctx)
        return DataFrame(self._jdf.replace(subset, rep_dict), self.sql_ctx)

    def _check_replace_inputs(self, subset, to_replace, value):
        if value is _NoValue:
            if isinstance(to_replace, dict):
                value = None
            else:
                raise TypeError("value argument is required when to_replace is not a dictionary.")

        # Validate input types
        valid_types = (bool, float, int, str, list, tuple)
        if not isinstance(to_replace, valid_types) and not isinstance(to_replace, dict):
            raise ValueError(
                "to_replace should be a bool, float, int, long, string, list, tuple, or dict. "
                "Got {0}".format(type(to_replace)))
        if not isinstance(value, valid_types) and value is not None \
                and not isinstance(to_replace, dict):
            raise ValueError("If to_replace is not a dict, value should be "
                             "a bool, float, int, long, string, list, tuple or None. "
                             "Got {0}".format(type(value)))
        if isinstance(to_replace, (list, tuple)) and isinstance(value, (list, tuple)):
            if len(to_replace) != len(value):
                raise ValueError("to_replace and value lists should be of the same length. "
                                 "Got {0} and {1}".format(len(to_replace), len(value)))
        if not (subset is None or isinstance(subset, (list, tuple, str))):
            raise ValueError("subset should be a list or tuple of column names, "
                             "column name or None. Got {0}".format(type(subset)))
        return value

    def approxQuantile(self, col, probabilities, relativeError):
        """
        Approximate a list of quantiles (probabilities) for one or a list of columns (col)
        with an error related to relativeError.

        More information in pysparkling.stat_counter.ColumnStatHelper

        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df.approxQuantile("age", [0.1, 0.5, 0.9], 1/1000)
        [2.0, 2.0, 5.0]
        >>> df.approxQuantile(["age"], [0.1, 0.5, 0.9], 1/1000)
        [[2.0, 2.0, 5.0]]
        """
        if not isinstance(col, (str, list, tuple)):
            raise ValueError("col should be a string, list or tuple, but got %r" % type(col))

        isStr = isinstance(col, str)

        if isinstance(col, tuple):
            col = list(col)
        elif isStr:
            col = [col]

        for c in col:
            if not isinstance(c, str):
                raise ValueError("columns should be strings, but got %r" % type(c))

        if not isinstance(probabilities, (list, tuple)):
            raise ValueError("probabilities should be a list or tuple")
        if isinstance(probabilities, tuple):
            probabilities = list(probabilities)
        for p in probabilities:
            if not isinstance(p, (float, int)) or p < 0 or p > 1:
                raise ValueError("probabilities should be numerical (float, int, long) in [0,1].")

        if not isinstance(relativeError, (float, int)) or relativeError < 0:
            raise ValueError("relativeError should be numerical (float, int, long) >= 0.")
        relativeError = float(relativeError)

        jaq = self._jdf.approxQuantile(col, probabilities, relativeError)
        jaq_list = [list(j) for j in jaq]
        return jaq_list[0] if isStr else jaq_list

    def corr(self, col1, col2, method=None):
        """
        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> spark.range(50).corr('id', 'id')
        1.0
        """
        if not isinstance(col1, str):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, str):
            raise ValueError("col2 should be a string.")
        if not method:
            method = "pearson"
        if method != "pearson":
            raise ValueError("Currently only the calculation of the Pearson Correlation " +
                             "coefficient is supported.")
        return self._jdf.corr(col1, col2, method)

    def cov(self, col1, col2):
        """
        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> spark.range(50).cov('id', 'id')
        212.5
        """
        if not isinstance(col1, str):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, str):
            raise ValueError("col2 should be a string.")
        return self._jdf.cov(col1, col2)

    def crosstab(self, col1, col2):
        if not isinstance(col1, str):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, str):
            raise ValueError("col2 should be a string.")
        return DataFrame(self._jdf.crosstab(self, col1, col2), self.sql_ctx)


class DataFrameNaFunctions(object):
    def __init__(self, df):
        self.df = df

    def drop(self, how='any', thresh=None, subset=None):
        return self.df.dropna(how=how, thresh=thresh, subset=subset)

    drop.__doc__ = DataFrame.dropna.__doc__

    def fill(self, value, subset=None):
        return self.df.fillna(value=value, subset=subset)

    fill.__doc__ = DataFrame.fillna.__doc__

    def replace(self, to_replace, value=_NoValue, subset=None):
        return self.df.replace(to_replace, value, subset)

    replace.__doc__ = DataFrame.replace.__doc__
