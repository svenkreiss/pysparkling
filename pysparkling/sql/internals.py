import json
import math
from collections import Counter
from copy import deepcopy
from functools import partial

from pysparkling import StorageLevel
from pysparkling.sql.internal_utils.column import resolve_column
from pysparkling.sql.internal_utils.joins import CROSS_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN, INNER_JOIN, \
    LEFT_ANTI_JOIN, LEFT_SEMI_JOIN
from pysparkling.sql.schema_utils import infer_schema_from_rdd, get_schema_from_cols, merge_schemas
from pysparkling.sql.types import StructType, create_row, row_from_keyed_values, StructField, StringType
from pysparkling.sql.column import parse
from pysparkling.sql.utils import IllegalArgumentException
from pysparkling.stat_counter import RowStatHelper, CovarianceCounter
from pysparkling.utils import get_keyfunc, compute_weighted_percentiles, reservoir_sample_and_size, pad_cell, \
    str_half_width, format_cell, merge_rows, merge_rows_joined_on_values, portable_hash


class FieldIdGenerator(object):
    """
    This metaclass adds an unique ID to all instances of its classes.

    This allows to identify that a field was, when created, associated to a DataFrame.

    Such field can be retrieved with the syntax df.name to build an operation.
    The id clarifies if it is still associated to a field and which one
    when the operation is applied.

    While id() allow the same behaviour in most cases, this one:
    - Allows deep copies which are needed for aggregation
    - Support distributed computation, e.g. multiprocessing
    """
    _id = 0

    @classmethod
    def next(cls):
        cls._id += 1
        return cls._id

    @classmethod
    def bind_schema(cls, schema):
        for field in schema.fields:
            if not hasattr(field, "id"):
                field.id = cls.next()
            if isinstance(field, StructType):
                cls.bind_schema(field)
        return schema

    @classmethod
    def unbind_schema(cls, schema):
        for field in schema.fields:
            delattr(field, "id")
            if isinstance(field, StructType):
                cls.unbind_schema(field)
        return schema


class DataFrameInternal(object):
    def __init__(self, sc, rdd, cols=None, convert_to_row=False, schema=None):
        """
        :type rdd: RDD
        """
        if convert_to_row:
            if cols is None:
                cols = ["_c{0}".format(i) for i in range(200)]
            rdd = rdd.map(partial(create_row, cols))

        self._sc = sc
        self._rdd = rdd
        if schema is None and convert_to_row is False:
            raise NotImplementedError(
                "Schema cannot be None when creating DataFrameInternal from another. "
                "As a user you should not see this error, feel free to report a bug at "
                "https://github.com/svenkreiss/pysparkling/issues"
            )
        if schema is not None:
            self._set_schema(schema)
        else:
            self._set_schema(infer_schema_from_rdd(self._rdd))

    def _set_schema(self, schema):
        bound_schema = FieldIdGenerator.bind_schema(deepcopy(schema))
        self.bound_schema = bound_schema

    @property
    def unbound_schema(self):
        schema = deepcopy(self.bound_schema)
        return FieldIdGenerator.unbind_schema(schema)

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
        return self._with_rdd(jdf, self.bound_schema)

    def take(self, n):
        return self._rdd.take(n)

    def foreach(self, f):
        self._rdd.foreach(f)

    def foreachPartition(self, f):
        self._rdd.foreachPartition(f)

    def cache(self):
        return self._with_rdd(self._rdd.cache(), self.bound_schema)

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY):
        return self._with_rdd(self._rdd.persist(storageLevel), self.bound_schema)

    def unpersist(self, blocking=False):
        return self._with_rdd(self._rdd.unpersist(blocking), self.bound_schema)

    def coalesce(self, numPartitions):
        return self._with_rdd(self._rdd.coalesce(numPartitions), self.bound_schema)

    def distinct(self):
        return self._with_rdd(self._rdd.distinct(), self.bound_schema)

    def sample(self, withReplacement=None, fraction=None, seed=None):
        return self._with_rdd(
            self._rdd.sample(
                withReplacement=withReplacement,
                fraction=fraction,
                seed=seed
            ),
            self.bound_schema
        )

    def randomSplit(self, weights, seed):
        return self._with_rdd(
            self._rdd.randomSplit(weights=weights, seed=seed),
            self.bound_schema
        )

    @property
    def storageLevel(self):
        return getattr(self._rdd, "storageLevel", StorageLevel(False, False, False, False))

    def is_cached(self):
        return hasattr(self._rdd, "storageLevel")

    def simple_repartition(self, numPartitions):
        return self._with_rdd(self._rdd.repartition(numPartitions), self.bound_schema)

    def repartitionByValues(self, numPartitions, partitioner=None):
        return self._with_rdd(
            self._rdd.map(lambda x: (x, x)).partitionBy(numPartitions, partitioner).values(),
            self.bound_schema
        )

    def repartition(self, numPartitions, cols):
        def partitioner(row):
            return sum(hash(c.eval(row, self.bound_schema)) for c in cols)

        return self.repartitionByValues(numPartitions, partitioner)

    def repartitionByRange(self, numPartitions, *cols):
        key = get_keyfunc(cols, self.bound_schema)
        bounds = self._get_range_bounds(self._rdd, numPartitions, key=key)

        def get_range_id(value):
            return sum(1 for bound in bounds if key(bound) < key(value))

        return self.repartitionByValues(numPartitions, partitioner=get_range_id)

    @staticmethod
    def _get_range_bounds(rdd, numPartitions, key):
        if numPartitions == 0:
            return []

        # pylint: disable=W0511
        # todo: check if sample_size is set in SQLConf.get.rangeExchangeSampleSizePerPartition
        # sample_size = min(
        #   SQLConf.get.rangeExchangeSampleSizePerPartition * rdd.getNumPartitions(),
        #   1e6
        # )
        sample_size = 1e6
        sample_size_per_partition = math.ceil(3 * sample_size / numPartitions)
        sketched_rdd = DataFrameInternal.sketch_rdd(rdd, sample_size_per_partition)
        rdd_size = sum(partition_size for partition_size, sample in sketched_rdd.values())

        if rdd_size == 0:
            return []

        fraction = sample_size / rdd_size

        candidates, imbalanced_partitions = DataFrameInternal._get_initial_candidates(
            sketched_rdd,
            sample_size_per_partition,
            fraction
        )

        additional_candidates = DataFrameInternal._get_additional_candidates(
            rdd,
            imbalanced_partitions,
            fraction
        )

        candidates += additional_candidates
        bounds = compute_weighted_percentiles(
            candidates,
            min(numPartitions, len(candidates)) + 1,
            key=key
        )[1:-1]
        return bounds

    @staticmethod
    def _get_initial_candidates(sketched_rdd, sample_size_per_partition, fraction):
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
        return candidates, imbalanced_partitions

    @staticmethod
    def _get_additional_candidates(rdd, imbalanced_partitions, fraction):
        additional_candidates = []
        if imbalanced_partitions:
            # Re-sample imbalanced partitions with the desired sampling probability.
            def keep_imbalanced_partitions(partition_id, x):
                return x if partition_id in imbalanced_partitions else []

            resampled = (rdd.mapPartitionsWithIndex(keep_imbalanced_partitions)
                         .sample(withReplacement=False, fraction=fraction, seed=rdd.id())
                         .collect())
            weight = (1.0 / fraction).toFloat
            additional_candidates += [(x, weight) for x in resampled]
        return additional_candidates

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
            sample, original_size = reservoir_sample_and_size(
                x,
                sample_size_per_partition,
                seed=rdd.id() + idx
            )
            return [(idx, (original_size, sample))]

        sketched_rdd_content = rdd.mapPartitionsWithIndex(sketch_partition).collect()

        return dict(sketched_rdd_content)

    def toJSON(self, use_unicode):
        """

        :rtype: RDD
        """
        return self._rdd.map(lambda row: json.dumps(row.asDict(True), ensure_ascii=not use_unicode))

    def sortWithinPartitions(self, cols, ascending):
        key = get_keyfunc([parse(c) for c in cols], self.bound_schema)

        def partition_sort(data):
            return sorted(data, key=key, reverse=not ascending)

        return self._with_rdd(
            self._rdd.mapPartitions(partition_sort),
            self.bound_schema
        )

    def sort(self, cols):
        # Pysparkling implementation of RDD.sortBy is an in-order sort,
        # calling it multiple times allow sorting
        # based on multiple criteria and ascending orders
        # pylint: disable=W0511
        # Todo: this could be optimized as it's possible to sort
        #  together columns that are in the same ascending order
        sorted_rdd = self._rdd
        for col in cols[::-1]:
            ascending = col.sort_order in ["ASC NULLS FIRST", "ASC NULLS LAST"]
            nulls_are_smaller = col.sort_order in ["DESC NULLS LAST", "ASC NULLS FIRST"]
            key = get_keyfunc([col], self.bound_schema, nulls_are_smaller=nulls_are_smaller)
            sorted_rdd = sorted_rdd.sortBy(key, ascending=ascending)
        return self._with_rdd(sorted_rdd, self.bound_schema)

    def select(self, *exprs):
        cols = [parse(e) for e in exprs]

        if any(col.is_an_aggregation for col in cols):
            # todo: add support
            raise NotImplementedError
            # df_as_group = InternalGroupedDataFrame(self, [])
            # return df_as_group.agg(exprs)

        def select_mapper(partition_index, partition):
            # Initialize non deterministic functions so that they are reproducible
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

            return self.get_select_output_field_lists(
                partition,
                non_generators,
                initialized_cols,
                generators[0] if generators else None
            )

        new_schema = get_schema_from_cols(cols, self.bound_schema)
        return self._with_rdd(
            self._rdd.mapPartitionsWithIndex(select_mapper),
            schema=new_schema
        )

    def get_select_output_field_lists(self, partition, non_generators, initialized_cols, generator):
        output_rows = []
        for row in partition:
            base_row_fields = []
            for col in non_generators:
                output_cols, output_values = resolve_column(col, row, schema=self.bound_schema)
                base_row_fields += zip(output_cols, output_values[0])

            if generator is not None:
                generated_row_fields = self.get_generated_row_fields(
                    generator, row, initialized_cols, base_row_fields
                )
                for generated_row in generated_row_fields:
                    output_rows.append(
                        row_from_keyed_values(generated_row, metadata=row.get_metadata())
                    )
            else:
                output_rows.append(
                    row_from_keyed_values(base_row_fields, metadata=row.get_metadata())
                )
        return output_rows

    def get_generated_row_fields(self, generator, row, initialized_cols, base_row):
        additional_fields = []
        generator_position = initialized_cols.index(generator)
        generated_cols, generated_sub_rows = resolve_column(
            generator, row, schema=self.bound_schema
        )
        for generated_sub_row in generated_sub_rows:
            sub_row = list(zip(generated_cols, generated_sub_row))
            additional_fields.append(
                base_row[:generator_position] + sub_row + base_row[generator_position:]
            )
        return additional_fields

    def selectExpr(self, *cols):
        raise NotImplementedError("Pysparkling does not currently support DF.selectExpr")

    def filter(self, condition):
        condition = parse(condition)

        def mapper(partition_index, partition):
            initialized_condition = condition.initialize(partition_index)
            return (row for row in partition if initialized_condition.eval(row, self.bound_schema))

        return self._with_rdd(
            self._rdd.mapPartitionsWithIndex(mapper),
            self.bound_schema
        )

    def union(self, other):
        self_field_names = [field.name for field in self.bound_schema.fields]
        other_field_names = [field.name for field in other.bound_schema.fields]
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
                (field.name, value) for field, value in zip(self.bound_schema.fields, row)
            ])

        # This behavior (keeping the columns of self) is the same as in PySpark
        return self._with_rdd(
            self._rdd.union(other.rdd().map(change_col_names)),
            self.bound_schema
        )

    def unionByName(self, other):
        self_field_names = [field.name for field in self.bound_schema.fields]
        other_field_names = [field.name for field in other.bound_schema.fields]
        if len(self_field_names) != len(set(self_field_names)):
            raise Exception(
                "Found duplicate column(s) in the left attributes: {0}".format(
                    name for name, cnt in Counter(self_field_names).items() if cnt > 1
                )
            )

        if len(other_field_names) != len(set(other_field_names)):
            raise Exception(
                "Found duplicate column(s) in the right attributes: {0}".format(
                    name for name, cnt in Counter(other_field_names).items() if cnt > 1
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
                (field.name, row[field.name]) for field in self.bound_schema.fields
            ])

        # This behavior (keeping the columns of self) is the same as in PySpark
        return self._with_rdd(
            self._rdd.union(other.rdd().map(change_col_order)),
            self.bound_schema
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

        new_schema = StructType([
            field if field.name != existing else StructField(
                new,
                field.dataType,
                field.nullable
            ) for field in self.bound_schema.fields
        ])

        return self._with_rdd(self._rdd.map(mapper), schema=new_schema)

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
            ) for new_name, field in zip(new_names, self.bound_schema.fields)
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

    def get_summary_schema(self, exprs):
        return StructType(
            [
                StructField("summary", StringType(), True)
            ] + [
                StructField(field.name, StringType(), True)
                for field in get_schema_from_cols(exprs, self.bound_schema).fields
            ]
        )

    def get_stat_helper(self, exprs, percentiles_relative_error=1 / 10000):
        """
        :rtype: RowStatHelper
        """
        return self.aggregate(
            RowStatHelper(exprs, percentiles_relative_error),
            lambda counter, row: counter.merge(row, self.bound_schema),
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

        cols = [field.name for field in self.bound_schema.fields]
        if not vertical:
            output = self.horizontal_show(rows, cols, truncate, min_col_width)
        else:
            output = self.vertical_show(rows, min_col_width)

        if not rows[1:] and vertical:
            output += "(0 rows)\n"
        elif contains_more:
            output += "only showing top {0} row{1}\n".format(n, "s" if len(rows) > 1 else "")

        # Last \n will be added by print()
        return output[:-1]

    def vertical_show(self, rows, min_col_width):
        output = ""
        field_names = [field.name for field in self.bound_schema.fields]
        field_names_col_width = max(
            min_col_width,
            *(str_half_width(field_name) for field_name in field_names)
        )
        data_col_width = max(
            min_col_width,
            *(str_half_width(cell) for data_row in rows for cell in data_row)
        )
        for i, row in enumerate(rows):
            row_header = "-RECORD {0}".format(i).ljust(
                field_names_col_width + data_col_width + 5,
                "-"
            )
            output += row_header + "\n"
            for field_name, cell in zip(field_names, row):
                formatted_field_name = field_name.ljust(
                    field_names_col_width - str_half_width(field_name) + len(field_name)
                )
                data = format_cell(cell).ljust(data_col_width - str_half_width(cell))
                output += " {0} | {1} \n".format(formatted_field_name, data)
        return output

    @staticmethod
    def horizontal_show(rows, cols, truncate, min_col_width):
        output = ""
        col_widths = [max(min_col_width, str_half_width(col)) for col in cols]
        for row in rows:
            col_widths = [
                max(cur_width, str_half_width(cell))
                for cur_width, cell in zip(col_widths, row)
            ]
        padded_header = (pad_cell(col, truncate, col_width)
                         for col, col_width in zip(cols, col_widths))
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
        return output

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

    def join(self, other, on, how):
        if on is None and how == "cross":
            merged_schema = merge_schemas(self.bound_schema, other.bound_schema, how)
            output_rdd = self.cross_join(other)
        elif isinstance(on, list) and all(isinstance(col, str) for col in on):
            merged_schema = merge_schemas(
                self.bound_schema,
                other.bound_schema,
                how,
                on=on
            )
            output_rdd = self.join_on_values(other, on, how)
        elif not isinstance(on, list):
            merged_schema = merge_schemas(self.bound_schema, other.bound_schema, how)
            output_rdd = self.join_on_condition(other, on, how, merged_schema)
        else:
            raise NotImplementedError(
                "Pysparkling only supports str, Column and list of str for on"
            )

        return self._with_rdd(output_rdd, schema=merged_schema)

    def join_on_condition(self, other, on, how, new_schema):
        """

        :type other: DataFrameInternal
        """

        def condition(couple):
            left, right = couple
            merged_rows = merge_rows(left, right)
            condition_value = on.eval(merged_rows, schema=new_schema)
            return condition_value

        joined_rdd = self.rdd().cartesian(other.rdd()).filter(condition)

        def format_output(entry):
            left, right = entry

            return merge_rows(left, right)  # , self.bound_schema, other.bound_schema, how)

        output_rdd = joined_rdd.map(format_output)
        return output_rdd

    def cross_join(self, other):
        """

        :type other: DataFrameInternal
        """
        joined_rdd = self.rdd().cartesian(other.rdd())

        def format_output(entry):
            left, right = entry

            return merge_rows(left, right)  # , self.bound_schema, other.bound_schema, how)

        output_rdd = joined_rdd.map(format_output)
        return output_rdd

    def join_on_values(self, other, on, how):
        if how != CROSS_JOIN:
            def add_key(row):
                # When joining on value, no check on schema (and lack of duplicated col) is done
                return tuple(row[on_column] for on_column in on), row
        else:
            def add_key(row):
                return True, row

        keyed_self = self.rdd().map(add_key)
        keyed_other = other.rdd().map(add_key)
        if how == LEFT_JOIN:
            joined_rdd = keyed_self.leftOuterJoin(keyed_other)
        elif how == RIGHT_JOIN:
            joined_rdd = keyed_self.rightOuterJoin(keyed_other)
        elif how == FULL_JOIN:
            joined_rdd = keyed_self.fullOuterJoin(keyed_other)
        elif how in (INNER_JOIN, CROSS_JOIN):
            joined_rdd = keyed_self.join(keyed_other)
        elif how == LEFT_ANTI_JOIN:
            joined_rdd = keyed_self._leftAntiJoin(keyed_other)
        elif how == LEFT_SEMI_JOIN:
            joined_rdd = keyed_self._leftSemiJoin(keyed_other)
        else:
            raise IllegalArgumentException("Invalid how argument in join: {0}".format(how))

        def format_output(entry):
            _, (left, right) = entry

            return merge_rows_joined_on_values(
                left,
                right,
                self.bound_schema,
                other.bound_schema,
                how,
                on
            )

        output_rdd = joined_rdd.map(format_output)
        return output_rdd

    def crossJoin(self, other):
        return self.join(other, on=None, how="cross")

    def exceptAll(self, other):
        def except_all_within_partition(self_partition, other_partition):
            min_other = next(other_partition, None)
            for item in self_partition:
                if min_other is None or min_other > item:
                    yield item
                elif min_other < item:
                    while min_other < item or min_other is None:
                        min_other = next(other_partition, None)
                else:
                    min_other = next(other_partition, None)

        return self.applyFunctionOnHashPartitionedRdds(other, except_all_within_partition)

    def intersectAll(self, other):
        def intersect_all_within_partition(self_partition, other_partition):
            min_other = next(other_partition, None)
            for item in self_partition:
                if min_other is None:
                    return
                if min_other > item:
                    continue
                if min_other < item:
                    while min_other < item or min_other is None:
                        min_other = next(other_partition, None)
                else:
                    yield item
                    min_other = next(other_partition, None)

        return self.applyFunctionOnHashPartitionedRdds(other, intersect_all_within_partition)

    def applyFunctionOnHashPartitionedRdds(self, other, func):
        self_prepared_rdd, other_prepared_rdd = self.hash_partition_and_sort(other)

        def filter_partition(partition_id, self_partition):
            other_partition = other_prepared_rdd.partitions()[partition_id].x()
            return func(iter(self_partition), iter(other_partition))

        filtered_rdd = self_prepared_rdd.mapPartitionsWithIndex(filter_partition)
        return self._with_rdd(filtered_rdd, self.bound_schema)

    def hash_partition_and_sort(self, other):
        num_partitions = max(self.rdd().getNumPartitions(), 200)

        def prepare_rdd(rdd):
            return rdd.partitionBy(num_partitions, portable_hash).mapPartitions(sorted)

        self_prepared_rdd = prepare_rdd(self.rdd())
        other_prepared_rdd = prepare_rdd(other.rdd())
        return self_prepared_rdd, other_prepared_rdd
