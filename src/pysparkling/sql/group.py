from pysparkling.sql.column import Column
from pysparkling.sql.dataframe import DataFrame
# pylint: disable=W0622
from pysparkling.sql.functions import count, mean, parse, avg, max, min, sum, lit


class GroupedData(object):
    def __init__(self, jgd, df):
        self._jgd = jgd
        self._df = df
        self.sql_ctx = df.sql_ctx

    def agg(self, *exprs):
        """
        # >>> sorted(gdf.agg({"*": "count"}).collect())
        # [Row(name=u'Alice', count(1)=1), Row(name=u'Bob', count(1)=1)]

        >>> from pysparkling.sql import functions as F        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import col, avg
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> gdf = df.groupBy(df.name)
        >>> sorted(gdf.agg(F.min(df.age)).collect())
        [Row(name='Alice', min(age)=2), Row(name='Bob', min(age)=5)]
        >>> df.groupBy("age").agg(avg("age"), col("age")).show()
        +---+--------+---+
        |age|avg(age)|age|
        +---+--------+---+
        |  2|     2.0|  2|
        |  5|     5.0|  5|
        +---+--------+---+

        """
        if not exprs:
            raise ValueError("exprs should not be empty")

        if len(exprs) == 1 and isinstance(exprs[0], dict):
            # pylint: disable=W0511
            # todo implement agg_dict
            jdf = self._jgd.agg_dict(exprs[0])
        else:
            # Columns
            if not all(isinstance(c, Column) for c in exprs):
                raise ValueError("all exprs should be Column")

            # noinspection PyProtectedMember
            jdf = self._jgd.agg([parse(e) for e in exprs])

        return DataFrame(jdf, self.sql_ctx)

    def count(self):
        return self.agg(count(lit(1)).alias("count"))

    # pylint: disable=W0511
    # todo: avg, max, etc should work when cols is left empty
    def mean(self, *cols):
        return self.agg(*(mean(parse(col)) for col in cols))

    def avg(self, *cols):
        return self.agg(*(avg(parse(col)) for col in cols))

    def max(self, *cols):
        return self.agg(*(max(parse(col)) for col in cols))

    def min(self, *cols):
        return self.agg(*(min(parse(col)) for col in cols))

    def sum(self, *cols):
        return self.agg(*(sum(parse(col)) for col in cols))

    def pivot(self, pivot_col, values=None):
        """
        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import col, avg, sum
        >>> sc = Context()
        >>> spark = SparkSession(sc)
        >>> df4 = sc.parallelize([Row(course="dotNET", year=2012, earnings=10000),
        ...                        Row(course="Java",   year=2012, earnings=20000),
        ...                        Row(course="dotNET", year=2012, earnings=5000),
        ...                        Row(course="dotNET", year=2013, earnings=48000),
        ...                        Row(course="Java",   year=2013, earnings=30000)]).toDF()
        >>> df4.groupBy("year").pivot("course", ["dotNET", "Java"]).sum("earnings").collect()
        [Row(year=2012, dotNET=15000, Java=20000), Row(year=2013, dotNET=48000, Java=30000)]
        >>> df4.groupBy("year").pivot("course").sum("earnings").collect()
        [Row(year=2012, Java=20000, dotNET=15000), Row(year=2013, Java=30000, dotNET=48000)]
        >>> df4.groupBy("year").pivot("course", ["dotNET"]).sum("earnings").collect()
        [Row(year=2012, dotNET=15000), Row(year=2013, dotNET=48000)]
        >>> df4.groupBy("year").pivot("course").agg(sum("earnings")).show()
        +----+-----+------+
        |year| Java|dotNET|
        +----+-----+------+
        |2012|20000| 15000|
        |2013|30000| 48000|
        +----+-----+------+
        >>> df4.groupBy("year").pivot("course", ["dotNET", "PHP"]).agg(sum("earnings")).show()
        +----+------+----+
        |year|dotNET| PHP|
        +----+------+----+
        |2012| 15000|null|
        |2013| 48000|null|
        +----+------+----+
        >>> df4.groupBy("year").pivot("course").agg(sum("earnings"), avg("earnings")).show()
        +----+------------------+------------------+--------------------+--------------------+
        |year|Java_sum(earnings)|Java_avg(earnings)|dotNET_sum(earnings)|dotNET_avg(earnings)|
        +----+------------------+------------------+--------------------+--------------------+
        |2012|             20000|           20000.0|               15000|              7500.0|
        |2013|             30000|           30000.0|               48000|             48000.0|
        +----+------------------+------------------+--------------------+--------------------+
        """
        jgd = self._jgd.pivot(parse(pivot_col), values)
        return GroupedData(jgd, self._df)

    # pylint: disable=W0511
    # todo: implement apply()
    # def apply(self, udf):
    #     # Columns are special because hasattr always return True
    #     if isinstance(udf, Column) or not hasattr(udf, 'func') \
    #             or udf.evalType != PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF:
    #         raise ValueError("Invalid udf: the udf argument must be a pandas_udf of type "
    #                          "GROUPED_MAP.")
    #     df = self._df
    #     udf_column = udf(*[df[col] for col in df.columns])
    #     jdf = self._jgd.flatMapGroupsInPandas(udf_column._jc.expr())
    #     return DataFrame(jdf, self.sql_ctx)
