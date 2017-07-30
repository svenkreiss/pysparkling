.. _api:

API
===

.. currentmodule:: pysparkling

A usual ``pysparkling`` session starts with either parallelizing a `list`
with :func:`Context.parallelize` or by reading data from a file using
:func:`Context.textFile`. These two methods return :class:`RDD` instances that
can then be processed.


.. toctree::
   :maxdepth: 2

   api_rdd
   api_context
   api_streaming
   api_fileio
