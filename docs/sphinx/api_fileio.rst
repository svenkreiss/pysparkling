.. _api_fileio:


fileio
------

.. currentmodule:: pysparkling

The functionality provided by this module is used in :func:`Context.textFile`
for reading and in :func:`RDD.saveAsTextFile` for writing.

.. currentmodule:: pysparkling.fileio

You can use this submodule for writing files directly with :func:`File.dump`,
:func:`File.load` and :func:`File.exists` to read, write and check
for existance of a file. All methods transparently handle various schemas
(for example ``http://``, ``s3://`` and ``file://``) and
compression/decompression of ``.gz`` and ``.bz2`` files (among others).


.. autoclass:: pysparkling.fileio.File
    :members:

.. autoclass:: pysparkling.fileio.TextFile
    :members:

.. autoclass:: pysparkling.fileio.fs.FileSystem
    :members:

.. autoclass:: pysparkling.fileio.fs.S3
    :members:

.. autoclass:: pysparkling.fileio.fs.GS
    :members:

.. autoclass:: pysparkling.fileio.fs.Hdfs
    :members:

.. autoclass:: pysparkling.fileio.fs.Http
    :members:

.. autoclass:: pysparkling.fileio.fs.Local
    :members:
