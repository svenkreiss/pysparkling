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
.. image:: https://pypip.in/v/pytld/badge.svg
    :target: https://pypi.python.org/pypi/pytld/
.. image:: https://pypip.in/d/pytld/badge.svg
    :target: https://pypi.python.org/pypi/pytld/


API
===

Context
-------

* ``__init__(pool=None)``: takes a pool object (an object that has a ``map()``
  method, e.g. a multiprocessing.Pool) to parallelize all ``map()`` and
  ``foreach()`` methods.

* ``textFile(filename)``: load every line of a text file into a TLD.


TLD
---

* ``cache()``: do nothing
* ``coalesce()``: do nothing
* ``collect()``: return the underlying list
* ``count()``: get length of internal list
* ``countApprox()``: same as ``count()``
* ``countByKey``: input is list of pairs, returns a dictionary
* ``countByValue``: input is a list, returns a dictionary
* ``context()``: return the context
* ``distinct(numPartitions=None)``: returns a new TLD containing the distinct elements
* ``filter(func)``: return new TLD filtered with func
* ``first()``: return first element
* ``flatMap(func, preservesPartitioning=False)``: return a new TLD of a flattened map (``preservesPartitioning`` has no effect)
* ``flatMapValues(func)``: return new TLD
* ``foreach(func)``: apply func to every element in place
* ``map(func)``: apply func to every element and return a new TLD
* ``mapValues(func)``: apply func to value in (key, value) pairs and return a new TLD
* ``take(n)``: get the first n elements
* ``takeSample(n)``: get n random samples


Changelog
---------

* `master <https://github.com/svenkreiss/pytld/compare/v0.1.0...master>`_
* `0.1.0 <https://github.com/svenkreiss/pytld/compare/v0.1.0...v0.1.0>`_ (2015-05-09)
