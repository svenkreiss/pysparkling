import json
import math
import random
import sys
import warnings
from collections import Counter
from copy import deepcopy
from functools import partial

from pyspark import StorageLevel, Row
from pyspark.sql.types import StructField, LongType, StructType, StringType, DataType

from pysparkling import RDD
from pysparkling.sql.column import resolve_column
from pysparkling.sql.functions import parse, count
from pysparkling.sql.schema_utils import infer_schema_from_list, merge_schemas, get_schema_from_cols
from pysparkling.stat_counter import RowStatHelper, CovarianceCounter
from pysparkling.utils import reservoir_sample_and_size, compute_weighted_percentiles, get_keyfunc, \
    row_from_keyed_values, str_half_width, pad_cell, merge_rows, format_cell

if sys.version >= '3':
    basestring = str


def to_row(cols, record):
    return row_from_keyed_values(zip(cols, record))


class DataFrameInternal(object):
    def __init__(self, sc, rdd, cols=None, convert_to_row=False, schema=None):
        """
        :type rdd: RDD
        """
        if convert_to_row:
            if cols is None:
                cols = ["_c{0}".format(i) for i in range(200)]
            rdd = rdd.map(partial(to_row, cols))

        self._sc = sc
        self._rdd = rdd
        if schema is None and convert_to_row is False:
            raise NotImplementedError(
                "Schema cannot be None when creating DataFrameInternal from another. "
                "As a user you should not see this error, feel free to report a bug at "
                "https://github.com/svenkreiss/pysparkling/issues"
            )
        if schema is not None:
            self.schema = schema
        else:
            self.schema = infer_schema_from_list(self._rdd.takeSample(withReplacement=False, num=200))

    def _with_rdd(self, rdd, schema):
        return DataFrameInternal(
            self._sc,
            rdd,
            schema=schema
        )

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
        return self._with_rdd(jdf, self.schema)

    def take(self, n):
        return self._rdd.take(n)

    def foreach(self, f):
        self._rdd.foreach(f)

    def foreachPartition(self, f):
        self._rdd.foreachPartition(f)

    def cache(self):
        return self._with_rdd(self._rdd.cache(), self.schema)

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY):
        return self._with_rdd(self._rdd.persist(storageLevel), self.schema)

    def unpersist(self, blocking=False):
        return self._with_rdd(self._rdd.unpersist(blocking), self.schema)

    def coalesce(self, numPartitions):
        return self._with_rdd(self._rdd.coalesce(numPartitions), self.schema)

    def repartition(self, numPartitions):
        return self._with_rdd(self._rdd.repartition(numPartitions), self.schema)

    def distinct(self):
        return self._with_rdd(self._rdd.distinct(), self.schema)

    def sample(self, withReplacement=None, fraction=None, seed=None):
        return self._with_rdd(
            self._rdd.sample(
                withReplacement=withReplacement,
                fraction=fraction,
                seed=seed
            ),
            self.schema
        )

    def randomSplit(self, weights, seed):
        return self._with_rdd(
            self._rdd.randomSplit(weights=weights, seed=seed),
            self.schema
        )

    @property
    def storageLevel(self):
        return getattr(self._rdd, "storageLevel", StorageLevel(False, False, False, False))

    def is_cached(self):
        return hasattr(self._rdd, "storageLevel")

    def partitionValues(self, numPartitions, partitioner):
        return self._with_rdd(
            self._rdd.map(lambda x: (x, x)).partitionBy(numPartitions, partitioner).values(),
            self.schema
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
        if seed is None:
            seed = random.random()

        random.seed(seed)

        def sample_row(row):
            key = row[col]
            if key in fractions and random.random() < fractions[key]:
                return True

        return self._with_rdd(
            self._rdd.filter(sample_row),
            self.schema
        )

    def toJSON(self):
        """

        :rtype: RDD
        """
        return self._rdd.map(lambda row: json.dumps(row.asDict(True)))

    def sortWithinPartitions(self, cols, ascending):
        key = get_keyfunc(cols)

        def partition_sort(data):
            return sorted(data, key=key, reverse=not ascending)

        return self._with_rdd(
            self._rdd.mapPartitions(partition_sort),
            self.schema
        )

    def sort(self, cols, ascending):
        key = get_keyfunc(cols)
        return self._with_rdd(
            self._rdd.sortBy(key, ascending=ascending),
            self.schema
        )

    def select(self, *exprs):
        cols = [parse(e) for e in exprs]
        new_schema = get_schema_from_cols(cols, self.schema)

        def mapper(partition_index, partition):
            # Initialize non deterministic functions make them reproducible
            initialized_cols = [col.initialize(partition_index) for col in cols]
            generators = [col for col in initialized_cols if col.may_output_multiple_rows]
            non_generators = [col for col in initialized_cols if not col.may_output_multiple_rows]
            number_of_generators = len(generators)
            if number_of_generators > 1:
                raise Exception(
                    "Only one generator allowed per select clause but found {0}: {1}".format(
                        number_of_generators,
                        ", ".join(generators)
                    )
                )

            output_field_lists = []
            for row in partition:
                base_row = []
                for col in non_generators:
                    output_cols, output_values = resolve_column(col, row, schema=self.schema)
                    base_row += zip(output_cols, output_values[0])

                if number_of_generators == 1:
                    generator = generators[0]
                    generator_position = initialized_cols.index(generator)
                    generated_cols, generated_sub_rows = resolve_column(generator, row, schema=self.schema)
                    for generated_sub_row in generated_sub_rows:
                        sub_row = list(zip(generated_cols, generated_sub_row))
                        output_field_lists.append(
                            base_row[:generator_position]+sub_row+base_row[generator_position:]
                        )
                else:
                    output_field_lists.append(base_row)

            return list(
                row_from_keyed_values(output_row_fields)
                for output_row_fields in output_field_lists
            )

        return self._with_rdd(self._rdd.mapPartitionsWithIndex(mapper), schema=new_schema)

    def selectExpr(self, *cols):
        raise NotImplementedError("Pysparkling does not currently support DF.selectExpr")

    def filter(self, condition):
        condition = parse(condition)

        def mapper(partition_index, partition):
            initialized_condition = condition.initialize(partition_index)
            return (row for row in partition if initialized_condition.eval(row, self.schema))

        return self._with_rdd(
            self._rdd.mapPartitionsWithIndex(mapper),
            self.schema
        )

    def union(self, other):
        self_field_names = [field.name for field in self.schema.fields]
        other_field_names = [field.name for field in other.schema.fields]
        if len(self_field_names) != len(other_field_names):
            raise Exception(
                "Union can only be performed on tables with the same number "
                "of columns, but the first table has {0} columns and the "
                "second table has {1} columns".format(
                    len(self_field_names),
                    len(other_field_names)
                )
            )

        def change_col_names(row):
            return row_from_keyed_values([
                (field.name, value) for field, value in zip(self.schema.fields, row)
            ])

        # This behavior (keeping the column of self) is the same as in PySpark
        return self._with_rdd(
            self._rdd.union(other.rdd().map(change_col_names)),
            self.schema
        )

    def unionByName(self, other):
        # todo: add 2 tests with
        # ... df.unionByName(df2).select(df2.age).show()
        # and df.unionByName(df2).select(df.age).show()

        self_field_names = [field.name for field in self.schema.fields]
        other_field_names = [field.name for field in other.schema.fields]
        if len(self_field_names) != len(set(self_field_names)):
            raise Exception(
                "Found duplicate column(s) in the left attributes: {0}".format(
                    name for name, count in Counter(self_field_names).items() if count > 1
                )
            )

        if len(other_field_names) != len(set(other_field_names)):
            raise Exception(
                "Found duplicate column(s) in the right attributes: {0}".format(
                    name for name, count in Counter(other_field_names).items() if count > 1
                )
            )

        if len(self_field_names) != len(other_field_names):
            raise Exception(
                "Union can only be performed on tables with the same number "
                "of columns, but the first table has {0} columns and the "
                "second table has {1} columns".format(
                    len(self_field_names),
                    len(other_field_names)
                )
            )

        def change_col_order(row):
            return row_from_keyed_values([
                (field.name, row[field.name]) for field in self.schema.fields
            ])

        # This behavior (keeping the column of self) is the same as in PySpark
        return self._with_rdd(
            self._rdd.union(other.rdd().map(change_col_order)),
            self.schema
        )

    def withColumn(self, colName, col):
        return self.select(parse("*"), parse(col).alias(colName))

    def withColumnRenamed(self, existing, new):
        def mapper(row):
            keyed_values = [
                (new, row[col]) if col == existing else (col, row[col])
                for col in row.__fields__
            ]
            return row_from_keyed_values(keyed_values)

        return self._with_rdd(self._rdd.map(mapper))

    def toDF(self, new_names):
        def mapper(row):
            keyed_values = [
                (new_name, row[old])
                for new_name, old in zip(new_names, row.__fields__)
            ]
            return row_from_keyed_values(keyed_values)

        new_schema = StructType([
            StructField(
                new_name,
                field.dataType,
                field.nullable
            ) for new_name, field in zip(new_names, self.schema.fields)
        ])

        return self._with_rdd(self._rdd.map(mapper), schema=new_schema)

    def describe(self, cols):
        stat_helper = self.get_stat_helper(cols)
        exprs = [parse(col) for col in cols]

        return DataFrameInternal(
            self._sc,
            self._sc.parallelize(stat_helper.get_as_rows()),
            schema=self.get_summary_schema(exprs)
        )

    def summary(self, statistics):
        stat_helper = self.get_stat_helper(["*"])
        if not statistics:
            statistics = ("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")
        return DataFrameInternal(
            self._sc,
            self._sc.parallelize(stat_helper.get_as_rows(statistics)),
            schema=self.get_summary_schema([parse("*")])
        )

    def get_summary_schema(self, exprs):
        return StructType(
            [
                StructField("summary", StringType(), True)
            ] + [
                StructField(field.name, StringType(), True)
                for field in get_schema_from_cols(exprs, self.schema).fields
            ]
        )

    def get_stat_helper(self, exprs, percentiles_relative_error=1 / 10000):
        """
        :rtype: RowStatHelper
        """
        return self.aggregate(
            RowStatHelper(exprs, percentiles_relative_error),
            lambda counter, row: counter.merge(row, self.schema),
            lambda counter1, counter2: counter1.mergeStats(counter2)
        )

    def aggregate(self, zeroValue, seqOp, combOp):
        return self._rdd.aggregate(zeroValue, seqOp, combOp)

    def showString(self, n, truncate=20, vertical=False):
        n = max(0, n)
        if n:
            sample = self.take(n + 1)
            rows = sample[:n]
            contains_more = len(sample) == n + 1
        else:
            rows = self.collect()
            contains_more = False

        min_col_width = 3

        cols = [field.name for field in self.schema.fields]
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
            body = "\n".join("|{0}|".format("|".join(padded_row)) for padded_row in padded_rows)
            if body:
                output += body + "\n"
            output += sep
        else:
            field_names = [field.name for field in self.schema.fields]

            field_names_col_width = max(
                min_col_width,
                *(str_half_width(field_name) for field_name in field_names)
            )
            data_col_width = max(
                min_col_width,
                *(str_half_width(cell) for data_row in rows for cell in data_row)
            )

            for i, row in enumerate(rows):
                row_header = "-RECORD {0}".format(i).ljust(field_names_col_width + data_col_width + 5, "-")
                output += row_header + "\n"
                for j, cell in enumerate(row):
                    field_name = field_names[j]
                    formatted_field_name = field_name.ljust(
                        field_names_col_width-str_half_width(field_name) + len(field_name)
                    )
                    data = format_cell(cell).ljust(data_col_width - str_half_width(cell))
                    output += " {0} | {1} \n".format(formatted_field_name, data)

        if len(rows[1:]) == 0 and vertical:
            output += "(0 rows)\n"
        elif contains_more:
            output += "only showing top {0} row{1}\n".format(n, "s" if len(rows) > 1 else "")

        # Last \n will be added by print()
        return output[:-1]

    def approxQuantile(self, exprs, quantiles, relative_error):
        stat_helper = self.get_stat_helper(exprs, percentiles_relative_error=relative_error)
        return [
            [
                stat_helper.get_col_quantile(col, quantile)
                for quantile in quantiles
            ] for col in stat_helper.col_names
        ]

    def corr(self, col1, col2, method):
        covariance_helper = self._get_covariance_helper(method, col1, col2)
        return covariance_helper.pearson_correlation

    def cov(self, col1, col2):
        covariance_helper = self._get_covariance_helper("pearson", col1, col2)
        return covariance_helper.covar_samp

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

    def join(self, other, on, how):
        if isinstance(on, basestring):
            new_schema = merge_schemas(self.schema, other.schema, field_not_to_duplicate=on)
            output_rdd = self.join_on_values(other, on, new_schema)
        else:
            new_schema = merge_schemas(self.schema, other.schema)
            output_rdd = self.join_on_condition(other, on, new_schema)

        return self._with_rdd(output_rdd, schema=new_schema)

    def join_on_condition(self, other, on, new_schema):
        """

        :type other: DataFrameInternal
        """
        def condition(couple):
            left, right = couple
            merged_rows = merge_rows(left, right)
            return on.eval(merged_rows, schema=new_schema)

        joined_rdd = self.rdd().cartesian(other.rdd()).filter(condition)

        def format_output(entry):
            left, right = entry

            return merge_rows(left, right)

        output_rdd = joined_rdd.map(format_output)
        return output_rdd

    def join_on_values(self, other, on, new_schema):
        on = parse(on)

        def add_key(row):
            return on.eval(row, new_schema), row

        keyed_self = self.rdd().map(add_key)
        keyed_other = other.rdd().map(add_key)
        joined_rdd = keyed_self.join(keyed_other)

        def format_output(entry):
            key, (left, right) = entry

            return merge_rows(left, right, key)

        output_rdd = joined_rdd.map(format_output)
        return output_rdd


class InternalGroupedDataFrame(object):
    GROUP_BY_TYPE = 0
    ROLLUP_TYPE = 1
    CUBE_TYPE = 2

    def __init__(self, df, grouping_exprs, group_type):
        """
        :type df: pysparkling.sql.dataframe.DataFrame
        """
        self.df = df
        self.grouping_cols = [parse(e) for e in grouping_exprs]
        self.group_type = group_type

    def agg(self, stats):
        init = GroupedStats(self.grouping_cols, stats)
        aggregated_stats = self.df._jdf.aggregate(
            init,
            lambda grouped_stats, row: grouped_stats.merge(row, self.df.schema),
            lambda grouped_stats_1, grouped_stats_2: grouped_stats_1.mergeStats(grouped_stats_2)
        )
        data = []
        for group_key in aggregated_stats.group_keys:
            key = [(str(key), value) for key, value in zip(self.grouping_cols, group_key)]
            key_as_row = row_from_keyed_values(key)
            data.append(row_from_keyed_values(
                key + [
                    (str(stat), stat.eval(key_as_row, self.df.schema)) for stat in aggregated_stats.groups[group_key]
                ]
            ))
        new_schema = StructType(
            [
                field for col in self.grouping_cols for field in
                col.find_fields_in_schema(self.df.schema)
            ] + [
                StructField(
                    str(stat for stat in stats),
                    DataType(),
                    True
                )
            ]
        )
        # noinspection PyProtectedMember
        return self.df._jdf._with_rdd(self.df._sc.parallelize(data), schema=new_schema)


class GroupedStats(object):
    def __init__(self, grouping_cols, stats):
        self.grouping_cols = grouping_cols
        self.stats = [stat for stat in stats]
        self.groups = {}
        # As python < 3.6 does not guarantee dict ordering
        # we need to keep track of in which order the columns were
        self.group_keys = []

    def merge(self, row, schema):
        group_key = tuple(col.eval(row, schema) for col in self.grouping_cols)
        if group_key not in self.groups:
            group_stats = [deepcopy(stat) for stat in self.stats]
            self.groups[group_key] = group_stats
            self.group_keys.append(group_key)
        else:
            group_stats = self.groups[group_key]

        for i, stat in enumerate(group_stats):
            group_stats[i] = stat.merge(row, schema)

        return self

    def mergeStats(self, other):
        for group_key in other.group_keys:
            if group_key not in self.group_keys:
                self.groups[group_key] = other.groups[group_key]
                self.group_keys.append(group_key)
            else:
                group_stats = self.groups[group_key]
                other_stats = other.groups[group_key]
                for i, (stat, other_stat) in enumerate(zip(group_stats, other_stats)):
                    group_stats[i] = stat.mergeStats(other_stat)

        return self
