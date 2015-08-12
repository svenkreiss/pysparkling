.. pysparkling documentation master file, created by
   sphinx-quickstart on Sun Jun  7 12:37:20 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: https://raw.githubusercontent.com/svenkreiss/pysparkling/master/logo/logo-w100.png
    :target: https://github.com/svenkreiss/pysparkling


Welcome to pysparkling's documentation!
=======================================

Please read the README at https://github.com/svenkreiss/pysparkling and
checkout the examples in this notebook:
https://github.com/svenkreiss/pysparkling/blob/master/docs/demo.ipynb

``pysparkling`` is native Python implementation of Spark's RDD interface. The primary objective to remove the dependency on the JVM and Hadoop. The focus is on having a lightweight and fast implementation for small datasets at the expense of some data resilience features and some parallel processing features. It is a drop-in replacement for PySpark's SparkContext and RDD.

Use case: you have a pipeline that processes 100k input documents and converts them to normalized features. They are used to train a local scikit-learn classifier. The preprocessing is perfect for a full Spark task. Now, you want to use this trained classifier in an API endpoint. Assume you need the same pre-processing pipeline for a single document per API call. This does not have to be done in parallel, but there should be only a small overhead in initialization and preferably no dependency on the JVM. This is what pysparkling is for.


Contents:

.. toctree::
   :maxdepth: 2

   api
   parallel



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

