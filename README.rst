.. image:: https://raw.githubusercontent.com/svenkreiss/pysparkling/master/logo/logo-w100.png
    :target: https://github.com/svenkreiss/pysparkling


pysparkling
===========

  A native Python implementation of Spark's RDD interface. The primary objective
  is not to have RDDs that are resilient and distributed, but to remove the dependency
  on the JVM and Hadoop. The focus is on having a lightweight and fast
  implementation for small datasets. It is a drop-in replacement
  for PySpark's SparkContext and RDD.

  Use case: you have a pipeline that processes 100k input documents
  and converts them to normalized features. They are used to train a local
  scikit-learn classifier. The preprocessing is perfect for a full Spark
  task. Now, you want to use this trained classifier in an API
  endpoint. You need the same pre-processing pipeline for a single
  document per API call. This does not have to be done in parallel, but there
  should be only a small overhead in initialization and preferably no
  dependency on the JVM. This is what ``pysparkling`` is for.

.. image:: https://badge.fury.io/py/pysparkling.svg
    :target: https://pypi.python.org/pypi/pysparkling/
.. image:: https://img.shields.io/pypi/dm/pysparkling.svg
    :target: https://pypi.python.org/pypi/pysparkling/
.. image:: https://badges.gitter.im/Join%20Chat.svg
   :alt: Join the chat at https://gitter.im/svenkreiss/pysparkling
   :target: https://gitter.im/svenkreiss/pysparkling?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge


Install
=======

.. code-block:: bash

  pip install pysparkling


Features
========

* Supports multiple URI scheme: ``s3://``, ``http://`` and ``file://``.
  Specify multiple files separated by comma.
  Resolves ``*`` and ``?`` wildcards.
* Handles ``.gz`` and ``.bz2`` compressed files.
* Parallelization via ``multiprocessing.Pool``,
  ``concurrent.futures.ThreadPoolExecutor`` or any other Pool-like
  objects that have a ``map(func, iterable)`` method.
* only dependencies: ``boto`` for AWS S3 and ``requests`` for http

The change log is in `HISTORY.rst <https://github.com/svenkreiss/pysparkling/blob/master/HISTORY.rst>`_.


Examples
========

**Word Count**

.. code-block:: python

    from pysparkling import Context

    counts = Context().textFile(
        'README.rst'
    ).map(
        lambda line: ''.join(ch if ch.isalnum() else ' ' for ch in line)
    ).flatMap(
        lambda line: line.split(' ')
    ).map(
        lambda word: (word, 1)
    ).reduceByKey(
        lambda a, b: a + b
    )
    print(counts.collect())

which prints a long list of pairs of words and their counts.
This and more advanced examples are demoed in
`docs/demo.ipynb <https://github.com/svenkreiss/pysparkling/blob/master/docs/demo.ipynb>`_.


API
===

A usual ``pysparkling`` session starts with either parallelizing a ``list`` or
by reading data from a file using the methods ``Context.parallelize(my_list)``
or ``Context.textFile("path/to/textfile.txt")``. These two methods return an
``RDD`` which can then be processed with the methods below.


RDD
---

*API doc*: http://pysparkling.trivial.io/v0.2/api.html#pysparkling.RDD


Context
-------

A ``Context`` describes the setup. Instantiating a Context with the default
arguments using ``Context()`` is the most lightweight setup. All data is just
in the local thread and is never serialized or deserialized.

If you want to process the data in parallel, you can use the ``multiprocessing``
module. Given the limitations of the default ``pickle`` serializer, you can
specify to serialize all methods with ``cloudpickle`` instead. For example,
a common instantiation with ``multiprocessing`` looks like this:

.. code-block:: python

  c = Context(
      multiprocessing.Pool(4),
      serializer=cloudpickle.dumps,
      deserializer=pickle.loads,
  )

This assumes that your data is serializable with ``pickle`` which is generally
faster. You can also specify a custom serializer/deserializer for data.

*API doc*: http://pysparkling.trivial.io/v0.2/api.html#pysparkling.Context


fileio
------

The functionality provided by this module is used in ``Context.textFile()``
for reading and in ``RDD.saveAsTextFile()`` for writing. You can use this
submodule for writing files directly with ``File(filename).dump(some_data)``,
``File(filename).load()`` and ``File.exists(path)`` to read, write and check
for existance of a file. All methods transparently handle ``http://``, ``s3://``
and ``file://`` locations and compression/decompression of ``.gz`` and
``.bz2`` files.

Use environment variables ``AWS_SECRET_ACCESS_KEY`` and ``AWS_ACCESS_KEY_ID``
for auth and use file paths of the form ``s3://bucket_name/filename.txt``.

*API doc*: http://pysparkling.trivial.io/v0.2/api.html#pysparkling.fileio.File
