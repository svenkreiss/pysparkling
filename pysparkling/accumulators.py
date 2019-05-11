# A large part of this module is extracted from its PySpark counterpart at
# https://spark.apache.org/docs/1.5.0/api/python/_modules/pyspark/accumulators.html
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

"""
>>> from pysparkling import Context
>>> sc = Context()
>>> a = sc.accumulator(1)
>>> a.value
1
>>> a.value = 2
>>> a.value
2
>>> a += 5
>>> a.value
7

>>> sc.accumulator(1.0).value
1.0

>>> sc.accumulator(1j).value
1j

>>> rdd = sc.parallelize([1,2,3])
>>> def f(x):
...     global a
...     a += x
>>> rdd.foreach(f)
>>> a.value
13

>>> b = sc.accumulator(0)
>>> def g(x):
...     b.add(x)
>>> rdd.foreach(g)
>>> b.value
6

>>> from pysparkling import AccumulatorParam
>>> class VectorAccumulatorParam(AccumulatorParam):
...     def zero(self, value):
...         return [0.0] * len(value)
...     def addInPlace(self, val1, val2):
...         for i in range(len(val1)):
...              val1[i] += val2[i]
...         return val1
>>> va = sc.accumulator([1.0, 2.0, 3.0], VectorAccumulatorParam())
>>> va.value
[1.0, 2.0, 3.0]
>>> def g(x):
...     global va
...     va += [x] * 3
>>> rdd.foreach(g)
>>> va.value
[7.0, 8.0, 9.0]

>>> sc.accumulator([1.0, 2.0, 3.0]) # doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
...
TypeError: No default accumulator param for type <type 'list'>
"""


__all__ = ['Accumulator', 'AccumulatorParam']



class Accumulator(object):
    """
    A shared variable that can be accumulated, i.e., has a commutative and associative "add"
    operation. Tasks can add values to an Accumulator with the ``+=`` operator

    The API supports accumulators for primitive data types like ``int`` and
    ``float``, users can also define accumulators for custom types by providing a custom
    ``AccumulatorParam`` object. Refer to the doctest of this module for an example.
    """

    def __init__(self, value, accum_param):
        """Create a new Accumulator with a given initial value and AccumulatorParam object"""
        self.accum_param = accum_param
        self._value = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def add(self, term):
        """Adds a term to this accumulator's value"""
        self._value = self.accum_param.addInPlace(self._value, term)

    def __iadd__(self, term):
        """The += operator; adds a term to this accumulator's value"""
        self.add(term)
        return self

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return "Accumulator<value={0}>".format(self._value)


class AccumulatorParam(object):
    """
    Helper object that defines how to accumulate values of a given type.
    """
    def zero(self, value):
        """
        Provide a "zero value" for the type, compatible in dimensions with the
        provided ``value`` (e.g., a zero vector)
        """
        raise NotImplementedError

    def addInPlace(self, value1, value2):
        """
        Add two values of the accumulator's data type, returning a new value;
        for efficiency, can also update ``value1`` in place and return it.
        """
        raise NotImplementedError


class AddingAccumulatorParam(AccumulatorParam):
    """
    An AccumulatorParam that uses the + operators to add values. Designed for simple types
    such as integers, floats, and lists. Requires the zero value for the underlying type
    as a parameter.
    """
    def __init__(self, zero_value):
        self.zero_value = zero_value

    def zero(self, value):
        return self.zero_value

    def addInPlace(self, value1, value2):
        value1 += value2
        return value1


# Singleton accumulator params for some standard types
INT_ACCUMULATOR_PARAM = AddingAccumulatorParam(0)
FLOAT_ACCUMULATOR_PARAM = AddingAccumulatorParam(0.0)
COMPLEX_ACCUMULATOR_PARAM = AddingAccumulatorParam(0.0j)


if __name__ == "__main__":
    """
    Execute doctests with

    $ python -m pysparkling.accumulators -v
    """
    import doctest
    failure_count, _ = doctest.testmod()
    if failure_count:
        exit(-1)
