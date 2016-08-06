.. _api:

API
===

A usual ``pysparkling`` session starts with either parallelizing a ``list`` or
by reading data from a file using the methods ``Context.parallelize(my_list)``
or ``Context.textFile("path/to/textfile.txt")``. These two methods return an
``RDD`` which can then be processed with the methods below.


.. toctree::
   :maxdepth: 2

   api_rdd
   api_context
   api_fileio
