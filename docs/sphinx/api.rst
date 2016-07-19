.. _api:

API
===

A usual ``pysparkling`` session starts with either parallelizing a ``list`` or
by reading data from a file using the methods ``Context.parallelize(my_list)``
or ``Context.textFile("path/to/textfile.txt")``. These two methods return an
``RDD`` which can then be processed with the methods below.


RDD
---

.. autoclass:: pysparkling.RDD
   :members:

.. autoclass:: pysparkling.StatCounter
   :members:


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

.. autoclass:: pysparkling.Context
   :members:


fileio
------

The functionality provided by this module is used in ``Context.textFile()``
for reading and in ``RDD.saveAsTextFile()`` for writing. You can use this
submodule for writing files directly with ``File(filename).dump(some_data)``,
``File(filename).load()`` and ``File.exists(path)`` to read, write and check
for existance of a file. All methods transparently handle various schemas
(for example ``http://``, ``s3://`` and ``file://``) and
compression/decompression of ``.gz`` and ``.bz2`` files (among others).

Use environment variables ``AWS_SECRET_ACCESS_KEY`` and ``AWS_ACCESS_KEY_ID``
for auth and use file paths of the form ``s3://bucket_name/filename.txt``.

.. autoclass:: pysparkling.fileio.File
    :members:

.. autoclass:: pysparkling.fileio.TextFile
    :members:
