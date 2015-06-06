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
This and a few more advanced examples are demoed in
`docs/demo.ipynb <https://github.com/svenkreiss/pysparkling/blob/master/docs/demo.ipynb>`_.


API
===

A usual ``pysparkling`` session starts with either parallelizing a ``list`` or
by reading data from a file using the methods ``Context.parallelize(my_list)``
or ``Context.textFile("path/to/textfile.txt")``. These two methods return an
``RDD`` which can then be processed with the methods below.


RDD
---

* ``aggregate(zeroValue, seqOp, combOp)``: aggregate value in partition with
  seqOp and combine with combOp
* ``aggregateByKey(zeroValue, seqFunc, combFunc)``: aggregate by key
* ``cache()``: synonym for ``persist()``
* ``cartesian(other)``: cartesian product
* ``coalesce()``: do nothing
* ``collect()``: return the underlying list
* ``count()``: get length of internal list
* ``countApprox()``: same as ``count()``
* ``countByKey``: input is list of pairs, returns a dictionary
* ``countByValue``: input is a list, returns a dictionary
* ``context()``: return the context
* ``distinct()``: returns a new RDD containing the distinct elements
* ``filter(func)``: return new RDD filtered with func
* ``first()``: return first element
* ``flatMap(func)``: return a new RDD of a flattened map
* ``flatMapValues(func)``: return new RDD
* ``fold(zeroValue, op)``: aggregate elements
* ``foldByKey(zeroValue, op)``: aggregate elements by key
* ``foreach(func)``: apply func to every element
* ``foreachPartition(func)``: apply func to every partition
* ``getNumPartitions()``: number of partitions
* ``getPartitions()``: returns an iterator over the partitions
* ``groupBy(func)``: group by the output of func
* ``groupByKey()``: group by key where the RDD is of type [(key, value), ...]
* ``histogram(buckets)``: buckets can be a list or an int
* ``id()``: currently just returns None
* ``intersection(other)``: return a new RDD with the intersection
* ``isCheckpointed()``: returns False
* ``join(other)``: join
* ``keyBy(func)``: creates tuple in new RDD
* ``keys()``: returns the keys of tuples in new RDD
* ``leftOuterJoin(other)``: left outer join
* ``lookup(key)``: return list of values for this key
* ``map(func)``: apply func to every element and return a new RDD
* ``mapPartitions(func)``: apply f to entire partitions
* ``mapValues(func)``: apply func to value in (key, value) pairs and return a new RDD
* ``max()``: get the maximum element
* ``mean()``: mean
* ``min()``: get the minimum element
* ``name()``: RDD's name
* ``persist()``: caches outputs of previous operations (previous steps are still executed lazily)
* ``pipe(command)``: pipe the elements through an external command line tool
* ``reduce()``: reduce
* ``reduceByKey()``: reduce by key and return the new RDD
* ``repartition(numPartitions)``: repartition
* ``rightOuterJoin(other)``: right outer join
* ``sample(withReplacement, fraction, seed=None)``: sample from the RDD
* ``sampleStdev()``: sample standard deviation
* ``sampleVariance()``: sample variance
* ``saveAsTextFile(path)``: save RDD as text file
* ``stats()``: return a StatCounter
* ``stdev()``: standard deviation
* ``subtract(other)``: return a new RDD without the elements in other
* ``sum()``: sum
* ``take(n)``: get the first n elements
* ``takeSample(n)``: get n random samples
* ``toLocalIterator()``: get a local iterator
* ``union(other)``: form union
* ``variance()``: variance
* ``zip(other)``: other has to have the same length
* ``zipWithUniqueId()``: pairs each element with a unique index


Context
-------

A ``Context`` describes the setup. Instantiating a Context with the default
arguments using ``Context()`` is the most lightweight setup. All data is just
in the local thread and is never serialized or deserialized.

If you want to process the data in parallel, you can use the ``multiprocessing``
module. Given the limitations of the default ``pickle`` serializer, you can
specify to serialize all methods with ``dill`` instead. For example, a common
instantiation with ``multiprocessing`` looks like this:

.. code-block:: python

  c = Context(
      multiprocessing.Pool(4),
      serializer=dill.dumps,
      deserializer=dill.loads,
  )

This assumes that your data is serializable with ``pickle`` which is generally
faster than ``dill``. You can also specify a custom serializer/deserializer
for data.

* ``__init__(pool=None, serializer=None, deserializer=None, data_serializer=None, data_deserializer=None)``:
  pool is any instance with a ``map(func, iterator)`` method
* ``broadcast(var)``: returns an instance of  ``Broadcast()``. Access its value
  with ``value``.
* ``newRddId()``: incrementing number [internal use]
* ``parallelize(list_or_iterator, numPartitions)``: returns a new RDD
* ``textFile(filename)``: load every line of a text file into an RDD
  ``filename`` can contain a comma separated list of many files, ``?`` and
  ``*`` wildcards, file paths on S3 (``s3://bucket_name/filename.txt``) and
  local file paths (``relative/path/my_text.txt``, ``/absolut/path/my_text.txt``
  or ``file:///absolute/file/path.txt``). If the filename points to a folder
  containing ``part*`` files, those are resolved.
* ``version``: the version of pysparkling


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

* ``File``:
    * ``__init__(filename)``: filename is a URI of a file (can include
      ``http://``, ``s3://`` and ``file://`` schemes)
    * ``dump(stream)``: write the stream to the file
    * ``[static] exists(path)``: check for existance of path
    * ``load()``: return the contents as BytesIO
    * ``make_public(recursive=False)``: only for files on S3
    * ``[static] resolve_filenames(expr)``: given an expression with ``*``
      and ``?`` wildcard characters, get a list of all matching filenames.
      Multiple expressions separated by ``,`` can also be specified.
      Spark-style partitioned datasets (folders containing ``part-*`` files)
      are resolved as well to a list of the individual files.
