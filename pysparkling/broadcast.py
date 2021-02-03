# A large part of this module is extracted from its PySpark counterpart at
# https://spark.apache.org/docs/1.5.0/api/python/_modules/pyspark/broadcast.html
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

__all__ = ['Broadcast']


class Broadcast:
    """
    A broadcast variable created with ``b = sc.broadcast(0)``.
    Access its value through ``b.value``.

    Examples:

    >>> from pysparkling import Context
    >>> sc = Context()
    >>> b = sc.broadcast([1, 2, 3, 4, 5])
    >>> b.value
    [1, 2, 3, 4, 5]
    >>> sc.parallelize([0, 0]).flatMap(lambda x: b.value).collect()
    [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
    >>> b.value += [1]
    Traceback (most recent call last):
    ...
    AttributeError: can't set attribute
    """
    def __init__(self, sc=None, value=None):
        self._value = value

    @property
    def value(self):
        """Returs the broadcasted value."""
        return self._value


if __name__ == "__main__":
    """
    Execute doctests with

    $ python -m pysparkling.broadcast -v
    """
    import doctest
    import sys

    failure_count, _ = doctest.testmod()
    if failure_count:
        sys.exit(-1)
