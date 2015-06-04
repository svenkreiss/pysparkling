.. image:: https://raw.githubusercontent.com/svenkreiss/pysparkling/master/logo/logo-w100.png
    :target: https://github.com/svenkreiss/pysparkling


pysparkling
===========

  A native Python implementation of Spark's RDD interface, but instead of
  being resilient and distributed it is just transient and local; but
  fast (lower latency than PySpark). It is a drop in replacement
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

* Supports multiple URI schemes like ``s3n://``, ``http://`` and ``file://``.
  Specify multiple files separated by comma.
  Resolves ``*`` and ``?`` wildcards.
* Handles ``.gz`` and ``.bz2`` compressed files.
* Parallelization via ``multiprocessing.Pool``,
  ``concurrent.futures.ThreadPoolExecutor`` or any other Pool-like
  objects that have a ``map(func, iterable)`` method.
* only dependencies: ``boto`` for AWS S3 and ``requests`` for http

The change log is in `HISTORY.rst <HISTORY.rst>`_.


Examples
========

The example source codes are included in ``tests/readme_example*.py``.

**Word count**: Count the words in a file.

.. code-block:: python

    from pysparkling import Context

    counts = Context().textFile(
        'README.rst'
    ).flatMap(
        lambda line: line.split(' ')
    ).map(
        lambda word: (word, 1)
    ).reduceByKey(
        lambda a, b: a + b
    )
    print(counts.collect())

which prints a long list of pairs of words and their counts.


**Common Crawl**: More info on the dataset is in this `blog post <http://blog.commoncrawl.org/2015/05/march-2015-crawl-archive-available/>`_.

.. code-block:: python

  from pysparkling import Context

  # read all the paths of warc and wat files of the latest Common Crawl
  paths_rdd = Context().textFile(
      's3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2015-11/warc.paths.*,'
      's3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2015-11/wat.paths.gz'
  )

  print(paths_rdd.collect())

which prints a long list of paths extracted from two gzip compressed files.


**Human Microbiome Project**: Get a random line without loading the entire
dataset.

.. code-block:: python

  from pysparkling import Context

  by_subject_rdd = Context().textFile(
      's3n://human-microbiome-project/DEMO/HM16STR/46333/by_subject/*'
  )
  print(by_subject_rdd.takeSample(1))

which prints out a line like ``[u'CAACGCCGCGTGAGGGATGACGGCCTTCGGGTTGTAAACCTCTTTCAGTATCGACGAAGC']``.


API
===

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

* ``__init__(pool=None, serializer=None, deserializer=None, data_serializer=None, data_deserializer=None)``:
  takes a pool object
  (an object that has a ``map()`` method, e.g. a multiprocessing.Pool) to
  parallelize methods. To support functions and lambda functions, specify custom
  serializers and deserializers,
  e.g. ``serializer=dill.dumps, deserializer=dill.loads``.
* ``broadcast(var)``: returns an instance of  ``Broadcast()`` and it's values
  are accessed with ``value``.
* ``newRddId()``: incrementing number
* ``textFile(filename)``: load every line of a text file into a RDD.
  ``filename`` can contain a comma separated list of many files, ``?`` and
  ``*`` wildcards, file paths on S3 (``s3n://bucket_name/filename.txt``) and
  local file paths (``relative/path/my_text.txt``, ``/absolut/path/my_text.txt``
  or ``file:///absolute/file/path.txt``). If the filename points to a folder
  containing ``part*`` files, those are resolved.
* ``version``: the version of pysparkling


Broadcast
---------

* ``value``: access the value it stores


fileio
------

The functionality provided by this module is used in ``Context.textFile()``
for reading and in ``RDD.saveAsTextFile()`` for writing. Normally, you should
not have to use this submodule directly.

Use environment variables ``AWS_SECRET_ACCESS_KEY`` and ``AWS_ACCESS_KEY_ID``
for auth and Use file paths of the form ``s3n://bucket_name/filename.txt``.

Infers ``.gz`` and ``.bz2`` compressions from the file name.

* ``File(file_name)``: file_name is either local, http, on S3 or ...
    * ``[static] exists(path)``: check for existance of path
    * ``[static] resolve_filenames(expr)``: given a glob-like expression with ``*``
      and ``?``, get a list of all matching filenames (either locally or on S3).
    * ``load()``: return the contents as BytesIO
    * ``dump(stream)``: write the stream to the file
    * ``make_public(recursive=False)``: only for files on S3
