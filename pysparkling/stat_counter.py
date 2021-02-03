#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This file is based on the PySpark version which in turn
# was ported from spark/util/StatCounter.scala
from collections import namedtuple
import copy
import math
import numbers

from pysparkling.sql.functions import parse
from pysparkling.sql.types import row_from_keyed_values

try:
    from numpy import maximum, minimum, sqrt
except ImportError:
    maximum = max
    minimum = min
    sqrt = math.sqrt


class StatCounter(object):

    def __init__(self, values=None):
        self.n = 0  # Running count of our values
        self.mu = 0.0  # Running mean of our values
        self.m2 = 0.0  # Running variance numerator (sum of (x - mean)^2)
        self.maxValue = float("-inf")
        self.minValue = float("inf")

        if values:
            for v in values:
                self.merge(v)

    # Add a value into this StatCounter, updating the internal statistics.
    def merge(self, value):
        delta = value - self.mu
        self.n += 1
        self.mu += delta / self.n
        self.m2 += delta * (value - self.mu)
        self.maxValue = maximum(self.maxValue, value)
        self.minValue = minimum(self.minValue, value)

        return self

    # Merge another StatCounter into this one, adding up the
    # internal statistics.
    def mergeStats(self, other):
        if other is self:  # reference equality holds
            # Avoid overwriting fields in a weird order
            return self.mergeStats(other.copy())

        if self.n == 0:
            self.mu = other.mu
            self.m2 = other.m2
            self.n = other.n
            self.maxValue = other.maxValue
            self.minValue = other.minValue

        elif other.n != 0:
            delta = other.mu - self.mu
            if other.n * 10 < self.n:
                self.mu = self.mu + (delta * other.n) / (self.n + other.n)
            elif self.n * 10 < other.n:
                self.mu = other.mu - (delta * self.n) / (self.n + other.n)
            else:
                self.mu = ((self.mu * self.n + other.mu * other.n)
                           / (self.n + other.n))

            self.maxValue = maximum(self.maxValue, other.maxValue)
            self.minValue = minimum(self.minValue, other.minValue)

            self.m2 += other.m2 + ((delta * delta * self.n * other.n)
                                   / (self.n + other.n))
            self.n += other.n

        return self

    # Clone this StatCounter
    def copy(self):
        return copy.deepcopy(self)

    def count(self):
        return int(self.n)

    def mean(self):
        return self.mu

    def sum(self):
        return self.n * self.mu

    def min(self):
        return self.minValue

    def max(self):
        return self.maxValue

    # Return the variance of the values.
    def variance(self):
        if self.n == 0:
            return float('nan')

        return self.m2 / self.n

    #
    # Return the sample variance, which corrects for bias in estimating
    # the variance by dividing by N-1 instead of N.
    #
    def sampleVariance(self):
        if self.n <= 1:
            return float('nan')

        return self.m2 / (self.n - 1)

    # Return the standard deviation of the values.
    def stdev(self):
        return sqrt(self.variance())

    #
    # Return the sample standard deviation of the values, which corrects for
    # bias in estimating the variance by dividing by N-1 instead of N.
    #
    def sampleStdev(self):
        return sqrt(self.sampleVariance())

    def __repr__(self):
        return ("(count: %s, mean: %s, stdev: %s, max: %s, min: %s)" %
                (self.count(), self.mean(), self.stdev(), self.max(), self.min()))


PercentileStats = namedtuple("PercentileStats", ["value", "g", "delta"])


class ColumnStatHelper(object):
    """
    This class is used to compute statistics on a column

    It computes basic statistics such as count, min, max, average, sample variance and
    sample standard deviation

    It also approximate percentiles with an implementation based, like Spark, on the algorithm
    proposed in the paper "Space-efficient Online Computation of Quantile Summaries"
    by Greenwald, Michael and Khanna, Sanjeev. (https://doi.org/10.1145/375663.375670)

    The result of this algorithm has the following deterministic bound:
    If the DataFrame has N elements and if we request the quantile at
    probability `p` up to error `err`, then the algorithm will return
    a sample `x` from the DataFrame so that the *exact* rank of `x` is
    close to (p * N).

    More precisely:
        floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).

    Its implementation for simple metrics differs from pysparkling.stat_counter.StatCounter
    in order to match Spark behaviour when computing stats on a dataset, among the discrepancies:
        - handle min and max for strings
        - return min and max as int when computed on int
        - name "stddev" the *sample* standard deviation metric
    """

    def __init__(self, column, percentiles_relative_error=1 / 10000):
        self.column = column

        self.count = 0
        self.sum_of_values = 0
        self.m2 = 0
        self.m3 = 0
        self.m4 = 0
        self.min_value = None
        self.max_value = None

        # Percentiles-related fields
        self.sampled = []  # List of PercentileStats
        self.head_sampled = []  # List acting as a buffer of the last added values
        self.head_max_size = 50000  # Buffer size after which buffer is added to sampled
        self.compression_threshold = 10000  # When to compress sampled
        self.percentiles_relative_error = percentiles_relative_error
        self.compressed = True

    def merge(self, row, schema):
        value = self.column.eval(row, schema)
        if value is not None:
            self.update_counters(value)
            if isinstance(value, numbers.Number):
                self.update_sample(value)
        return self

    def update_counters(self, value):
        if self.count != 0:
            self.min_value = min(self.min_value, value)
            self.max_value = max(self.max_value, value)
        else:
            self.min_value = value
            self.max_value = value

        try:
            self.update_moments(value)
            self.sum_of_values += value
        except TypeError:
            self.sum_of_values = None
            self.m2 = None
            self.m3 = None
            self.m4 = None

        self.count += 1

    def update_moments(self, value):
        delta = value - self.mean if self.count > 0 else 0
        deltaN = delta / (self.count + 1)
        self.m2 = self.m2 + delta * (delta - deltaN)
        delta2 = delta * delta
        deltaN2 = deltaN * deltaN
        self.m3 = self.m3 - 3 * deltaN * self.m2 + delta * (delta2 - deltaN2)
        self.m4 = (self.m4
                   - 4 * deltaN * self.m3
                   - 6 * deltaN2 * self.m2
                   + delta * (delta * delta2 - deltaN * deltaN2))

    def update_sample(self, value):
        self.head_sampled.append(value)
        self.compressed = False
        if len(self.head_sampled) >= self.head_max_size:
            self.add_head_to_sample()
            if len(self.sampled) >= self.compression_threshold:
                self.compress()

    def add_head_to_sample(self):
        if not self.head_sampled:
            return

        count_without_head = self.count - len(self.head_sampled)
        sorted_head = sorted(self.head_sampled)
        new_samples = []

        sample_idx = 0
        for ops_idx, current_sample in enumerate(sorted_head):
            for s in self.sampled[sample_idx:]:
                if s.value <= current_sample:
                    new_samples.append(s)
                    sample_idx += 1
                else:
                    break

            count_without_head += 1
            if not new_samples or (
                    sample_idx == len(self.sampled) and ops_idx == len(sorted_head) - 1
            ):
                delta = 0
            else:
                delta = math.floor(2 * self.percentiles_relative_error * count_without_head)

            new_samples.append(PercentileStats(value=current_sample, g=1, delta=delta))

        new_samples += self.sampled[sample_idx:]
        self.sampled = new_samples
        self.head_sampled = []
        self.compressed = False

    def compress(self):
        merge_threshold = self.merge_threshold()

        reverse_compressed_sample = []
        head = self.sampled[-1]
        for sample1 in self.sampled[-2:0:-1]:
            if sample1.g + head.g + head.delta < merge_threshold:
                head = PercentileStats(value=head.value, g=head.g + sample1.g, delta=head.delta)
            else:
                reverse_compressed_sample.append(head)
                head = sample1
        reverse_compressed_sample.append(head)
        current_head = self.sampled[0]
        if current_head.value <= head.value and len(self.sampled) > 1:
            reverse_compressed_sample.append(current_head)

        self.sampled = reverse_compressed_sample[::-1]
        self.compressed = True

    def merge_threshold(self):
        return 2 * self.percentiles_relative_error * self.count

    def finalize(self):
        if self.head_sampled:
            self.add_head_to_sample()
        if not self.compressed:
            self.compress()

    def mergeStats(self, other):
        """

        :type other: ColumnStatHelper
        """
        self.finalize()
        other.finalize()

        self.sampled = sorted(self.sampled + other.sampled)
        self.compress()

        if self.count == 0:
            self.max_value = other.max_value
            self.min_value = other.min_value
        elif other.count != 0:
            self.max_value = max(self.max_value, other.max_value)
            self.min_value = min(self.min_value, other.min_value)

        try:
            self.merge_moments(other)
            self.sum_of_values += other.sum_of_values
        except TypeError:
            self.sum_of_values = None
            self.m2 = None
            self.m3 = None
            self.m4 = None

        self.count += other.count

        return self

    def merge_moments(self, other):
        n1 = self.count
        n2 = other.count
        new_count = n1 + n2
        delta = other.mean - self.mean
        deltaN = delta / new_count if new_count != 0 else 0

        new_m2 = self.m2 + other.m2 + delta * deltaN * n1 * n2
        new_m3 = (self.m3 + other.m3
                  + deltaN * deltaN * delta * n1 * n2 * (n1 - n2)
                  + 3 * deltaN * (n1 * other.m2 - n2 * self.m2))
        self.m4 = (self.m4 + other.m4
                   + deltaN * deltaN * deltaN * delta * n1 * n2 * (n1 * n1 - n1 * n2 + n2 * n2)
                   + 6 * deltaN * deltaN * (n1 * n1 * other.m2 + n2 * n2 * self.m2)
                   + 4 * deltaN * (n1 * other.m3 - n2 * self.m3))
        self.m2 = new_m2
        self.m3 = new_m3

    def get_quantile(self, quantile):
        self.finalize()
        if not 0 <= quantile <= 1:
            raise ValueError("quantile must be between 0 and 1")
        if not self.sampled:
            return None
        if quantile <= self.percentiles_relative_error:
            return self.sampled[0].value
        if quantile >= 1 - self.percentiles_relative_error:
            return self.sampled[-1].value

        rank = math.ceil(quantile * self.count)
        target_error = self.percentiles_relative_error * self.count
        min_rank = 0
        for cur_sample in self.sampled:
            min_rank += cur_sample.g
            max_rank = min_rank + cur_sample.delta
            if max_rank - target_error <= rank <= min_rank + target_error:
                return cur_sample.value
        return self.sampled[-1].value

    @property
    def mean(self):
        if self.count == 0 or self.sum_of_values is None:
            return None
        return self.sum_of_values / self.count

    @property
    def variance_pop(self):
        if self.count == 0 or self.sum_of_values is None:
            return None
        return self.m2 / self.count

    @property
    def variance_samp(self):
        if self.count <= 1 or self.sum_of_values is None:
            return None
        return self.m2 / (self.count - 1)

    @property
    def variance(self):
        if self.count == 0:
            return None
        return self.variance_samp

    @property
    def stddev_pop(self):
        if self.count == 0 or self.sum_of_values is None:
            return None

        return math.sqrt(self.variance_pop)

    @property
    def stddev_samp(self):
        if self.count <= 1 or self.sum_of_values is None:
            return None
        return math.sqrt(self.variance_samp)

    @property
    def stddev(self):
        if self.count == 0:
            return None
        return self.stddev_samp

    @property
    def min(self):
        if self.count == 0:
            return None
        return self.min_value

    @property
    def max(self):
        if self.count == 0:
            return None
        return self.max_value

    @property
    def sum(self):
        if self.count == 0:
            return None
        return self.sum_of_values

    @property
    def skewness(self):
        if self.count == 0:
            return None
        if self.m2 == 0:
            return float("nan")
        return math.sqrt(self.count) * self.m3 / math.sqrt(self.m2 * self.m2 * self.m2)

    @property
    def kurtosis(self):
        if self.count == 0:
            return None
        if self.m2 == 0:
            return float("nan")
        return self.count * self.m4 / (self.m2 * self.m2) - 3


class RowStatHelper(object):
    """
    Class use to maintain one ColumnStatHelper for each Column found when aggregating a list of Rows
    """

    def __init__(self, exprs, percentiles_relative_error=1 / 10000):
        self.percentiles_relative_error = percentiles_relative_error
        self.column_stat_helpers = {}
        self.cols = [parse(e) for e in exprs] if exprs else [parse("*")]
        # As python < 3.6 does not guarantee dict ordering
        # we need to keep track of in which order the columns were
        self.col_names = []

    def merge(self, row, schema):
        for col in self.cols:
            for field in col.output_fields(schema):
                col_name = field.name
                if col_name not in self.column_stat_helpers:
                    self.column_stat_helpers[col_name] = ColumnStatHelper(
                        parse(col_name),
                        self.percentiles_relative_error
                    )
                    self.col_names.append(col_name)
                self.column_stat_helpers[col_name].merge(row, schema)

        return self

    def mergeStats(self, other):
        """

        :type other: RowStatHelper
        """
        for col_name in other.col_names:
            counter = other.column_stat_helpers[col_name]
            if col_name in self.column_stat_helpers:
                self.column_stat_helpers[col_name] = (
                    self.column_stat_helpers[col_name].mergeStats(counter)
                )
            else:
                self.column_stat_helpers[col_name] = counter
                self.col_names.append(col_name)

        return self

    def get_as_rows(self, stats=("count", "mean", "stddev", "min", "max")):
        """
        Provide a list of Row with the same format as the one in the
        Dataset returned by Dataset.stats()
        """
        return [
            row_from_keyed_values(
                [
                    ("summary", stat)
                ] + [
                    (col_name, self.get_stat(self.column_stat_helpers[col_name], stat))
                    for col_name in self.col_names
                ]
            )
            for stat in stats
        ]

    @staticmethod
    def get_stat(stats_counter, stat):
        if stat in ("count", "mean", "stddev", "min", "max"):
            value = getattr(stats_counter, stat)
        elif stat.endswith("%"):
            try:
                percentile = float(stat[:-1]) / 100
            except ValueError as e:
                raise ValueError("Unable to parse {0} as a percentile".format(stat)) from e
            value = stats_counter.get_quantile(percentile)
        else:
            raise ValueError("{0} is not a recognised statistic".format(stat))
        return RowStatHelper.format_stat(value)

    def get_col_quantile(self, col, quantile):
        if str(col) not in self.column_stat_helpers:
            raise Exception("Unable to get quantile for {0}".format(quantile))
        quantile = self.column_stat_helpers[str(col)].get_quantile(quantile)
        return float(quantile) if quantile is not None else None

    @staticmethod
    def format_stat(stat):
        return str(stat) if stat is not None else None


class CovarianceCounter(object):
    def __init__(self, method):
        if method != "pearson":
            raise ValueError(
                "Currently only the calculation of the Pearson Correlation "
                "coefficient is supported."
            )
        self.xAvg = 0.0  # the mean of all examples seen so far in col1
        self.yAvg = 0.0  # the mean of all examples seen so far in col2
        self.Ck = 0.0  # the co-moment after k examples
        self.MkX = 0.0  # sum of squares of differences from the (current) mean for col1
        self.MkY = 0.0  # sum of squares of differences from the (current) mean for col2
        self.count = 0  # count of observed examples

    def add(self, x, y):
        deltaX = x - self.xAvg
        deltaY = y - self.yAvg
        self.count += 1
        self.xAvg += deltaX / self.count
        self.yAvg += deltaY / self.count
        self.Ck += deltaX * (y - self.yAvg)
        self.MkX += deltaX * (x - self.xAvg)
        self.MkY += deltaY * (y - self.yAvg)
        return self

    def merge(self, other):
        """
         Merge counters from other partitions. Formula can be found at:
         http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
        """
        if other.count > 0:
            totalCount = self.count + other.count
            deltaX = self.xAvg - other.xAvg
            deltaY = self.yAvg - other.yAvg
            self.Ck += other.Ck + deltaX * deltaY * self.count / totalCount * other.count
            self.xAvg = (self.xAvg * self.count + other.xAvg * other.count) / totalCount
            self.yAvg = (self.yAvg * self.count + other.yAvg * other.count) / totalCount
            self.MkX += other.MkX + deltaX * deltaX * self.count / totalCount * other.count
            self.MkY += other.MkY + deltaY * deltaY * self.count / totalCount * other.count
            self.count = totalCount
        return self

    @property
    def covar_samp(self):
        """
        Return the sample covariance for the observed examples
        """
        if self.count <= 1:
            return None
        return self.Ck / (self.count - 1)

    @property
    def covar_pop(self):
        """
        Return the sample covariance for the observed examples
        """
        if self.count == 0:
            return None
        return self.Ck / self.count

    @property
    def pearson_correlation(self):
        return self.Ck / math.sqrt(self.MkX * self.MkY)
