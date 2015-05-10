pytld
=====

    A native Python implementation of Spark's RDD interface, but instead of
    being resilient and distributed it is just transient and local,
    so Transient-Local-Datastore TLD, but fast. It is a drop in replacement
    for PySpark's SparkContext and RDD. This is useful when a set of
    transformations for training a machine learning tool are used for training
    in full Spark, but also need to be used when deployed on a server. On the
    server, all transformations can be done in a single thread, but a
    dependency on Spark or the JVM is not possible.

.. image:: https://travis-ci.org/svenkreiss/pytld.png?branch=master
    :target: https://travis-ci.org/svenkreiss/pytld
.. image:: https://coveralls.io/repos/svenkreiss/pytld/badge.png
    :target: https://coveralls.io/r/svenkreiss/pytld
.. image:: https://pypip.in/v/pytld/badge.svg
    :target: https://pypi.python.org/pypi/pytld/
.. image:: https://pypip.in/d/pytld/badge.svg
    :target: https://pypi.python.org/pypi/pytld/


API
===

Context
-------

* ``__init__(pool=None)``: takes a pool object (e.g. a multiprocessing.Pool)
  to parallelize all ``map()`` and ``foreach()`` methods.

* __TextFile__: load every line of a text file into a TLD.


TLD
---

* ``map(func)``: apply func to every element and return a new TLD
* ``foreach(func)``: apply func to every element in place
* ``collect()``: return the underlying list
* ``take(n)``: get the first n elements
* ``takeSample(n)``: get n random samples


Changelog
---------

* `master <https://github.com/svenkreiss/pytld/compare/v0.1.0...master>`_
* `0.1.0 <https://github.com/svenkreiss/pytld/compare/v0.1.0...v0.1.0>`_ (2015-05-09)
