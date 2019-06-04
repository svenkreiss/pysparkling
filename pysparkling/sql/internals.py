import json
import math
import warnings
from functools import partial

from pyspark import StorageLevel, Row
from pyspark.sql.types import StructField, LongType, StructType, StringType

from pysparkling import RDD
from pysparkling.sql.column import resolve_column
from pysparkling.sql.functions import parse, count
from pysparkling.stat_counter import RowStatHelper, CovarianceCounter
from pysparkling.utils import reservoir_sample_and_size, compute_weighted_percentiles, get_keyfunc, \
    row_from_keyed_values, str_half_width, pad_cell


def to_row(cols, record):
    return Row(**dict(zip(cols, record)))


class DataFrameInternal(object):
    def __init__(self, sc, rdd, cols=None, convert_to_row=False):
        """
        :type rdd: RDD
        """
        self._sc = sc
        if convert_to_row:
            rdd = rdd.map(partial(to_row, cols))
        self._rdd = rdd

    def _with_rdd(self, rdd):
        return DataFrameInternal(self._sc, rdd)

    def rdd(self):
        return self._rdd

    @staticmethod
    def range(sc, start, end=None, step=1, numPartitions=None):
        if end is None:
            start, end = 0, start

        rdd = sc.parallelize(
            ([i] for i in range(start, end, step)),
            numSlices=numPartitions
        )
        return DataFrameInternal(sc, rdd, ["id"], True)

    def count(self):
        return self._rdd.count()

    def collect(self):
        return self._rdd.collect()

    def toLocalIterator(self):
        return self._rdd.toLocalIterator()

    def limit(self, n):
        jdf = self._sc.parallelize(self._rdd.take(n))
        return self._with_rdd(jdf)

    def take(self, n):
        return self._rdd.take(n)

    def foreach(self, f):
        self._rdd.foreach(f)

    def foreachPartition(self, f):
        self._rdd.foreachPartition(f)

    def cache(self):
        return self._with_rdd(self._rdd.cache())

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY):
        return self._with_rdd(self._rdd.persist(storageLevel))

    def unpersist(self, blocking=False):
        return self._with_rdd(self._rdd.unpersist(blocking))

    def coalesce(self, numPartitions):
        return self._with_rdd(self._rdd.coalesce(numPartitions))

    def repartition(self, numPartitions):
        return self._with_rdd(self._rdd.repartition(numPartitions))

    def distinct(self):
        return self._with_rdd(self._rdd.distinct())

    def sample(self, withReplacement=None, fraction=None, seed=None):
        return self._with_rdd(
            self._rdd.sample(
                withReplacement=withReplacement,
                fraction=fraction,
                seed=seed
            )
        )

    def randomSplit(self, weights, seed):
        return self._with_rdd(
            self._rdd.randomSplit(weights=weights, seed=seed)
        )

    @property
    def storageLevel(self):
        return getattr(self._rdd, "storageLevel", StorageLevel(False, False, False, False))

    def is_cached(self):
        return hasattr(self._rdd, "storageLevel")

    def partitionValues(self, numPartitions, partitioner):
        return self._with_rdd(
            self._rdd.map(lambda x: (x, x)).partitionBy(numPartitions, partitioner).values()
        )

    def repartitionByRange(self, numPartitions, *cols):
        key = get_keyfunc(cols)
        bounds = self._get_range_bounds(self._rdd, numPartitions, key=key)

        def get_range_id(value):
            return sum(1 for bound in bounds if key(bound) < key(value))

        return self.partitionValues(numPartitions, partitioner=get_range_id)

    @staticmethod
    def _get_range_bounds(rdd, numPartitions, key):
        if numPartitions == 0:
            return []

        # todo: check if sample_size is set in SQLConf.get.rangeExchangeSampleSizePerPartition
        # sample_size = min(SQLConf.get.rangeExchangeSampleSizePerPartition * rdd.getNumPartitions(), 1e6)
        sample_size = 1e6
        sample_size_per_partition = math.ceil(3 * sample_size / numPartitions)
        sketched_rdd = DataFrameInternal.sketch_rdd(rdd, sample_size_per_partition)
        rdd_size = sum(partition_size for partition_size, sample in sketched_rdd.values())

        if rdd_size == 0:
            return []

        fraction = sample_size / rdd_size

        candidates = []
        imbalanced_partitions = set()
        for idx, (partition_size, sample) in sketched_rdd.items():
            # Partition is bigger than (3 times) average and more than sample_size_per_partition
            # is needed to get accurate information on its distribution
            if fraction * partition_size > sample_size_per_partition:
                imbalanced_partitions.add(idx)
            else:
                # The weight is 1 over the sampling probability.
                weight = partition_size / len(sample)
                candidates += [(key, weight) for key in sample]

        if len(imbalanced_partitions) > 0:
            # Re-sample imbalanced partitions with the desired sampling probability.
            def keep_imbalanced_partitions(partition_id, x):
                return x if partition_id in imbalanced_partitions else []

            resampled = (rdd.mapPartitionsWithIndex(keep_imbalanced_partitions)
                         .sample(withReplacement=False, fraction=fraction, seed=rdd.id())
                         .collect())
            weight = (1.0 / fraction).toFloat
            candidates += [(x, weight) for x in resampled]

        bounds = compute_weighted_percentiles(
            candidates,
            min(numPartitions, len(candidates)) + 1,
            key=key
        )[1:-1]
        return bounds

    @staticmethod
    def sketch_rdd(rdd, sample_size_per_partition):
        """
        Get a subset per partition of an RDD

        Sampling algorithm is reservoir sampling.

        :param rdd:
        :param sample_size_per_partition:
        :return:
        """

        def sketch_partition(idx, x):
            sample, original_size = reservoir_sample_and_size(x, sample_size_per_partition, seed=rdd.id() + idx)
            return [(idx, (original_size, sample))]

        sketched_rdd_content = rdd.mapPartitionsWithIndex(sketch_partition).collect()

        return dict(sketched_rdd_content)

    def sampleBy(self, col, fractions, seed):
        import random

        def sample_row(row):
            key = row[col]
            if key in fractions and random.random() < fractions[key]:
                return True

        random.seed(seed)
        return self._with_rdd(self._rdd.filter(sample_row))

    def toJSON(self):
        """

        :rtype: RDD
        """
        return self._rdd.map(lambda row: json.dumps(row.asDict(True)))

    def sortWithinPartitions(self, cols, ascending):
        key = get_keyfunc(cols)

        def partition_sort(data):
            return sorted(data, key=key, reverse=not ascending)

        return self._with_rdd(self._rdd.mapPartitions(partition_sort))

    def sort(self, cols, ascending):
        key = get_keyfunc(cols)
        return self._with_rdd(self._rdd.sortBy(key, ascending=ascending))

    def groupBy(self, *cols):
        key = get_keyfunc(cols)
        return self._with_rdd(self._rdd.groupBy(key))

    def select(self, *cols):
        def mapper(row):
            keyed_values = [
                (key, value)
                for col in cols
                for key, value in resolve_column(col, row)
            ]
            return row_from_keyed_values(keyed_values)

        return self._with_rdd(self._rdd.map(mapper))

    def selectExpr(self, *cols):
        # todo: implement
        raise NotImplementedError

    def unionByName(self, other):
        return self._with_rdd(self._rdd.union(other.rdd()))

    def withColumn(self, colName, col):
        return self.selectExpr("*", parse(col).alias(colName))

    def withColumnRenamed(self, existing, new):
        def mapper(row):
            keyed_values = [
                (new, row[col]) if col == existing else (col, row[col])
                for col in row.__fields__
            ]
            return row_from_keyed_values(keyed_values)

        return self._with_rdd(self._rdd.map(mapper))

    def toDF(self, cols):
        def mapper(row):
            keyed_values = [
                (new, row[old])
                for new, old in zip(cols, row.__fields__)
            ]
            return row_from_keyed_values(keyed_values)

        return self._with_rdd(self._rdd.map(mapper))

    def schema(self):
        schema = {}
        for row in self._rdd.takeSample(200):
            for col in row:
                new_type = type(row[col])
                if col not in schema or schema[col] == type(None):
                    schema[col] = new_type
                elif row[col] is not None:
                    old_type = schema[col]
                    if old_type != new_type:
                        raise Exception("Unable to parse col {0}, found types {1} and {2}".format(
                            col, old_type, new_type
                        ))
        return schema

    def describe(self, cols):
        stat_helper = self._get_stat_helper(cols=cols)
        return DataFrameInternal(self._sc, self._sc.parallelize(stat_helper.get_as_rows()))

    def summary(self, statistics):
        stat_helper = self._get_stat_helper(cols=["*"])
        if not statistics:
            statistics = ("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")
        return DataFrameInternal(self._sc, self._sc.parallelize(stat_helper.get_as_rows(statistics)))

    def _get_stat_helper(self, cols, relative_error=1 / 10000):
        """
        :rtype: RowStatHelper
        """
        return self._rdd.aggregate(
            RowStatHelper(relative_error),
            lambda counter, row: counter.merge(cols, row),
            lambda counter1, counter2: counter1.mergeStats(counter2)
        )

    def showString(self, n, truncate=20, vertical=False):
        n = max(0, n)
        if truncate:
            sample = self.take(n + 1)
            rows = sample[:n]
            contains_more = len(sample) == n + 1
        else:
            rows = self.collect()
            contains_more = False

        min_col_width = 3

        cols = []
        for row in rows:
            for field in row.__fields__:
                if field not in cols:
                    cols.append(field)
        output = ""
        if not vertical:
            col_widths = [max(min_col_width, str_half_width(col)) for col in cols]
            for row in rows:
                col_widths = [
                    max(cur_width, str_half_width(cell))
                    for cur_width, cell in zip(col_widths, row)
                ]

            padded_header = (pad_cell(col, truncate, col_width) for col, col_width in zip(cols, col_widths))
            padded_rows = (
                [pad_cell(cell, truncate, col_width) for cell, col_width in zip(row, col_widths)]
                for row in rows
            )

            sep = "+" + "+".join("-" * col_width for col_width in col_widths) + "+\n"
            output += sep
            output += "|{0}|\n".format("|".join(padded_header))
            output += sep
            output += "\n".join(
                "|{0}|".format("|".join(padded_row)) for padded_row in padded_rows
            ) + "\n"
            output += sep
            if contains_more:
                output += "only showing top {0} row{1}\n".format(n, "s" if len(rows) > 1 else "")
        else:
            pass
            # todo: *
            # """
            #       {
            #           // Extended display mode enabled
            #           val fieldNames = rows.head
            #           val dataRows = rows.tail
            #
            #           // Compute the width of field name and data columns
            #           val fieldNameColWidth = fieldNames.foldLeft(minimumColWidth) { case (curMax, fieldName) =>
            #             math.max(curMax, Utils.stringHalfWidth(fieldName))
            #           }
            #           val dataColWidth = dataRows.foldLeft(minimumColWidth) { case (curMax, row) =>
            #             math.max(curMax, row.map(cell => Utils.stringHalfWidth(cell)).max)
            #           }
            #
            #           dataRows.zipWithIndex.foreach { case (row, i) =>
            #             // "+ 5" in size means a character length except for padded names and data
            #             val rowHeader = StringUtils.rightPad(
            #               s"-RECORD $i", fieldNameColWidth + dataColWidth + 5, "-")
            #             sb.append(rowHeader).append("\n")
            #             row.zipWithIndex.map { case (cell, j) =>
            #               val fieldName = StringUtils.rightPad(fieldNames(j),
            #                 fieldNameColWidth - Utils.stringHalfWidth(fieldNames(j)) + fieldNames(j).length)
            #               val data = StringUtils.rightPad(cell,
            #                 dataColWidth - Utils.stringHalfWidth(cell) + cell.length)
            #               s" $fieldName | $data "
            #             }.addString(sb, "", "\n", "\n")
            #           }
            #         }
            #
            #         // Print a footer
            #         if (vertical && rows.tail.isEmpty) {
            #           // In a vertical mode, print an empty row set explicitly
            #           sb.append("(0 rows)\n")
            #         } else if (hasMoreData) {
            #           // For Data that has more than "numRows" records
            #           val rowsString = if (numRows == 1) "row" else "rows"
            #           sb.append(s"only showing top $numRows $rowsString\n")
            #         }
            #       """
            # todo: above

        # Last \n will be added by print()
        return output[:-1]

    def approxQuantile(self, cols, quantiles, relative_error):
        stat_helper = self._get_stat_helper(cols=cols, relative_error=relative_error)
        return [
            [
                float(stat_helper.column_stat_helpers[col].get_quantile(quantile))
                for quantile in quantiles
            ] for col in cols
        ]

    def corr(self, col1, col2, method):
        covariance_helper = self._get_covariance_helper(method, col1, col2)
        return covariance_helper.pearson_correlation

    def cov(self, col1, col2):
        covariance_helper = self._get_covariance_helper("pearson", col1, col2)
        return covariance_helper.cov

    def _get_covariance_helper(self, method, col1, col2):
        """
        :rtype: CovarianceCounter
        """
        covariance_helper = self._rdd.treeAggregate(
            CovarianceCounter(method),
            seqOp=lambda counter, row: counter.add(row[col1], row[col2]),
            combOp=lambda baseCounter, other: baseCounter.merge(other)
        )
        return covariance_helper

    def crosstab(self, df, col1, col2):
        table_name = "_".join((col1, col2))
        counts = df.groupBy(col1, col2).agg(count("*")).take(1e6)
        if len(counts) == 1e6:
            warnings.warn("The maximum limit of 1e6 pairs have been collected, "
                          "which may not be all of the pairs. Please try reducing "
                          "the amount of distinct items in your columns.")

        def clean_element(element):
            return str(element) if element is not None else "null"

        distinct_col2 = (counts
                         .map(lambda row: clean_element(row[col2]))
                         .distinct()
                         .sorted()
                         .zipWithIndex()
                         .toMap())
        column_size = len(distinct_col2)
        if column_size < 1e4:
            raise ValueError(
                "The number of distinct values for {0} can't exceed 1e4. "
                "Currently {1}".format(col2, column_size)
            )

        def create_counts_row(col1Item, rows):
            counts_row = [None] * (column_size + 1)

            def parse_row(row):
                column_index = distinct_col2[clean_element(row[1])]
                counts_row[int(column_index + 1)] = int(row[2])

            rows.foreach(parse_row)
            # the value of col1 is the first value, the rest are the counts
            counts_row[0] = clean_element(col1Item)
            return Row(counts_row)

        table = counts.groupBy(lambda r: r[col1]).map(create_counts_row).toSeq

        # Back ticks can't exist in DataFrame column names, therefore drop them. To be able to accept
        # special keywords and `.`, wrap the column names in ``.
        def clean_column_name(name):
            return name.replace("`", "")

        # In the map, the column names (._1) are not ordered by the index (._2).
        # We need to explicitly sort by the column index and assign the column names.
        header_names = distinct_col2.toSeq.sortBy(lambda r: r[2]).map(lambda r: StructField(
            clean_column_name(str(r[1])), LongType
        ))
        schema = StructType([StructField(table_name, StringType)] + header_names)

        return schema, table
