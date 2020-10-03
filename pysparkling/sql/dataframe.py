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
