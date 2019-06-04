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

from __future__ import division

import copy
import math
import numbers
from collections import namedtuple, defaultdict

from pysparkling.utils import row_from_keyed_values

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
                self.mu = ((self.mu * self.n + other.mu * other.n) /
                           (self.n + other.n))

            self.maxValue = maximum(self.maxValue, other.maxValue)
            self.minValue = minimum(self.minValue, other.minValue)

            self.m2 += other.m2 + ((delta * delta * self.n * other.n) /
                                   (self.n + other.n))
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
        return (
                "(count: %s, mean: %s, stdev: %s, max: %s, min: %s)" %
                (self.count(), self.mean(), self.stdev(), self.max(), self.min())
        )


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

    def __init__(self, relative_error):
        self.count = 0
        self.sum_of_values = 0
        self.sum_of_squares = 0
        self.min_value = None
        self.max_value = None

        # Percentiles-related fields
        self.sampled = []  # List of PercentileStats
        self.head_sampled = []  # List acting as a buffer of the last added values
        self.head_max_size = 50000  # Buffer size after which buffer is added to sampled
        self.compression_threshold = 10000  # When to compress sampled
        self.relative_error = relative_error
        self.compressed = True

    def merge(self, value):
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
        self.count += 1
        try:
            self.sum_of_values += value
            self.sum_of_squares += value * value
        except TypeError:
            self.sum_of_values = None
            self.sum_of_squares = None

    def update_sample(self, value):
        self.head_sampled.append(value)
        self.compressed = False
        if len(self.head_sampled) >= self.head_max_size:
            self.add_head_to_sample()
            if len(self.sampled) >= self.compression_threshold:
                self.compress()

    def add_head_to_sample(self):
        if len(self.head_sampled) == 0:
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
            if len(new_samples) == 0 or (sample_idx == len(self.sampled) and ops_idx == len(sorted_head) - 1):
                delta = 0
            else:
                delta = math.floor(2 * self.relative_error * count_without_head)

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
        return 2 * self.relative_error * self.count

    def finalize(self):
        if len(self.head_sampled) > 0:
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

        self.sum_of_values += other.sum_of_values
        self.sum_of_squares += other.sum_of_squares
        self.count += other.count

        return self

    def get_quantile(self, quantile):
        self.finalize()
        if not 0 <= quantile <= 1:
            raise ValueError("quantile must be between 0 and 1")
        if len(self.sampled) == 0:
            return None
        if quantile <= self.relative_error:
            return self.sampled[0].value
        if quantile >= 1 - self.relative_error:
            return self.sampled[-1].value

        rank = math.ceil(quantile * self.count)
        target_error = self.relative_error * self.count
        min_rank = 0
        for i, cur_sample in enumerate(self.sampled):
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
    def variance(self):
        if self.count == 0 or self.sum_of_values is None:
            return None
        return (self.sum_of_squares - ((self.sum_of_values * self.sum_of_values) / self.count)) / (self.count - 1)

    @property
    def stddev(self):
        if self.count == 0 or self.sum_of_values is None:
            return None
        return math.sqrt(self.variance)

    @property
    def min(self):
        return self.min_value

    @property
    def max(self):
        return self.max_value


class RowStatHelper(object):
    """
    Class use to maintain one ColumnStatHelper for each Column found when aggregating a list of Rows
    """

    def __init__(self, relative_error):
        self.column_stat_helpers = defaultdict(lambda: ColumnStatHelper(relative_error))
        # As pythom < 3.6 does not guarantee dict ordering
        # we need to keep track of in which order the columns were
        self.cols = []

    def merge(self, cols, row):
        cols = cols if cols and "*" not in cols else row.__fields__
        self.cols += [col for col in cols if col not in self.cols]
        self.column_stat_helpers.update({
            col: self.column_stat_helpers[col].merge(row[col]) for col in cols
        })
        return self

    def mergeStats(self, other):
        """

        :type other: RowStatHelper
        """
        for col in other.cols:
            counter = other.column_stat_helpers[col]
            if col in self.column_stat_helpers:
                self.column_stat_helpers[col] = self.column_stat_helpers[col].mergeStats(counter)
            else:
                self.column_stat_helpers[col] = counter
                self.cols.append(col)

        return self

    def get_as_rows(self, stats=("count", "mean", "stddev", "min", "max")):
        """
        Provide a list of Row with the same format as the one in the Dataset returned by Dataset.stats()
        """
        return [
            row_from_keyed_values(
                [
                    ("summary", stat)
                ] + [
                    (col, self.get_stat(self.column_stat_helpers[col], stat))
                    for col in self.cols
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
            except ValueError:
                raise ValueError("Unable to parse {0} as a percentile".format(stat))
            value = stats_counter.get_quantile(percentile)
        else:
            raise ValueError("{0} is not a recognised statistic".format(stat))
        return RowStatHelper.format_stat(value)

    @staticmethod
    def format_stat(stat):
        return str(stat) if stat is not None else None


class CovarianceCounter(object):
    def __init__(self, method):
        if method != "pearson":
            raise ValueError(
                "Currently only the calculation of the Pearson Correlation coefficient is supported."
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
    def cov(self):
        """
        Return the sample covariance for the observed examples
        """
        return self.Ck / (self.count - 1)

    @property
    def pearson_correlation(self):
        return self.Ck / math.sqrt(self.MkX * self.MkY)
