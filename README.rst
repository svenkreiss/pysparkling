pysparkling
===========

    A native Python implementation of Spark's RDD interface, but instead of
    being resilient and distributed it is just transient and local; but
    fast (mucher lower latency than PySpark). It is a drop in replacement
    for PySpark's SparkContext and RDD. This is useful when a set of
    transformations for training a machine learning tool are used for training
    in full Spark, but also need to be used when deployed on a server. On the
    server, all transformations are fast enough to be done in a single thread,
    but a dependency on Spark or the JVM is not possible.

.. image:: https://travis-ci.org/svenkreiss/pysparkling.png?branch=master
    :target: https://travis-ci.org/svenkreiss/pysparkling
.. image:: https://pypip.in/v/pysparkling/badge.svg
    :target: https://pypi.python.org/pypi/pysparkling/
.. image:: https://pypip.in/d/pysparkling/badge.svg
    :target: https://pypi.python.org/pypi/pysparkling/


Features
========

* Parallelization via ``multiprocessing.Pool`` or any other Pool-like
  objects that have a ``map(func, iterable)`` method.
* AWS S3 is supported. Use file path of the form
  ``s3n://bucket_name/filename.txt`` with ``Context.textFile()``.
  Specify multiple files separated by comma.
  Use environment variables ``AWS_SECRET_ACCESS_KEY`` and
  ``AWS_ACCESS_KEY_ID`` for auth. Mixed local and S3 files are supported.
  Glob expressions (use ``*`` and ``?``) are resolved.
* Lazy execution is in development.
* only dependency: ``boto`` for AWS S3 access


Examples
========

Count the lines in the ``*.py`` files in the ``tests`` directory:

.. code-block:: python

    import pysparkling

    context = pysparkling.Context()
    print(context.textFile('tests/*.py').count())


API
===

Context
-------

* ``__init__(pool=None)``: takes a pool object (an object that has a ``map()``
  method, e.g. a multiprocessing.Pool) to parallelize all ``map()`` and
  ``foreach()`` methods.

* ``textFile(filename)``: load every line of a text file into a RDD.


RDD
---

* ``cache()``: do nothing
* ``coalesce()``: do nothing
* ``collect()``: return the underlying list
* ``count()``: get length of internal list
* ``countApprox()``: same as ``count()``
* ``countByKey``: input is list of pairs, returns a dictionary
* ``countByValue``: input is a list, returns a dictionary
* ``context()``: return the context
* ``distinct(numPartitions=None)``: returns a new RDD containing the distinct elements
* ``filter(func)``: return new RDD filtered with func
* ``first()``: return first element
* ``flatMap(func)``: return a new RDD of a flattened map
* ``flatMapValues(func)``: return new RDD
* ``fold(zeroValue, op)``: aggregate elements
* ``foldByKey(zeroValue, op)``: aggregate elements by key
* ``foreach(func)``: apply func to every element in place
* ``foreachPartition(func)``: same as ``foreach()``
* ``groupBy(func)``: group by the output of func
* **TODO**: continue going through the list
* ``map(func)``: apply func to every element and return a new RDD
* ``mapValues(func)``: apply func to value in (key, value) pairs and return a new RDD
* ``take(n)``: get the first n elements
* ``takeSample(n)``: get n random samples


Changelog
---------

* `master <https://github.com/svenkreiss/pysparkling/compare/v0.1.0...master>`_
* `0.1.0 <https://github.com/svenkreiss/pysparkling/compare/v0.1.0...v0.1.0>`_ (2015-05-09)
