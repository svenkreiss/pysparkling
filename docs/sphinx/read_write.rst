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


TensorFlow Records
~~~~~~~~~~~~~~~~~~

This example preprocesses example data into a TensorFlow Records file. The
second part is a cross check and prints the contents of the `tfrecords` file.

.. code-block:: python

    import pysparkling
    import tensorflow as tf

    def to_tfrecord(self, xy):
        X, y = xy
        example = tf.train.Example(features=tf.train.Features(feature={
            'X': tf.train.Feature(float_list=tf.train.FloatList(value=X)),
            'y': tf.train.Feature(int64_list=tf.train.Int64List(value=y)),
        }))
        return example.SerializeToString()

    # example
    X = [1.2, 3.1, 8.7]
    y = [2, 5]

    # writing
    sc = pysparkling.Context()
    rdd = (
        sc
        .parallelize([(X, y)])
        .map(to_tfrecord)
    )
    with tf.python_io.TFRecordWriter('out.tfrecords') as writer:
        for example in rdd.toLocalIterator():
            writer.write(example)

    # debugging a tf records file
    for serialized_example in tf.python_io.tf_record_iterator('out.tfrecords'):
        example = tf.train.Example()
        example.ParseFromString(serialized_example)
        X = example.features.feature['X'].float_list.value
        y = example.features.feature['y'].int64_list.value
        print(X, y)


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
