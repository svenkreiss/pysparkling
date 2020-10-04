from pysparkling.sql.column import Column
from pysparkling.sql.dataframe import DataFrame
# pylint: disable=W0622
from pysparkling.sql.functions import count, parse, avg, lit


class GroupedData(object):
    def __init__(self, jgd, df):
        self._jgd = jgd
        self._df = df
        self.sql_ctx = df.sql_ctx

    def agg(self, *exprs):
        """
        # >>> sorted(gdf.agg({"*": "count"}).collect())
        # [Row(name=u'Alice', count(1)=1), Row(name=u'Bob', count(1)=1)]

        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import col, avg
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> gdf = df.groupBy(df.name)
        >>> from pysparkling.sql import functions as F
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
