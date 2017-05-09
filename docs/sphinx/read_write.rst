.. _read_write:

.. currentmodule:: pysparkling


Reading and Writing
===================

This is a collection of best practices or templates for reading and writing
various input and output formats.


Batch
-----

Python List
~~~~~~~~~~~

The most direct input and output is from and to a Python list.

.. code-block:: python

    import pysparkling

    sc = pysparkling.Context()

    # reading
    rdd = sc.parallelize(['hello', 'world'])

    # back to Python list
    print(rdd.collect())

    # back to an iterator
    rdd.toLocalIterator()


ND-JSON
~~~~~~~

Newline delimited JSON is a text file where every line is its own JSON string.


.. code-block:: python

    import json
    import pysparkling

    sc = pysparkling.Context()

    # reading
    rdd = (
        sc
        .textFile('input.json')
        .map(json.loads)
    )

    # writing
    (
        rdd
        .map(json.dumps)
        .saveAsTextFile('output.json')
    )


CSV
~~~

.. code-block:: python

    import csv
    import io
    import pysparkling

    sc = pysparkling.Context()

    # reading
    rdd = (
        sc
        .textFile('input.csv')
        .mapPartitions(csv.reader)
    )

    # writing
    def csv_row(data):
        s = io.StringIO()
        csv.writer(s).writerow(data)
        return s.getvalue()

    (
        rdd
        .map(csv_row)
        .saveAsTextFile('output.csv')
    )


Streaming
---------

Python List
~~~~~~~~~~~

.. code-block:: python

    import pysparkling

    sc = pysparkling.Context()
    ssc = pysparkling.streaming.StreamingContext(sc, 1.0)

    (
        ssc
        .queueStream([[4], [2], [7]])
        .foreachRDD(lambda rdd: print(rdd.collect()))
    )

    ssc.start()
    ssc.awaitTermination(3.5)

    # output:
    # [4]
    # [2]
    # [7]
