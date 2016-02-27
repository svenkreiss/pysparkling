from __future__ import print_function

import pyspark
import pyspark.streaming

sc = pyspark.SparkContext()
ssc = pyspark.streaming.StreamingContext(sc, 1)


def simple_queue():
    ssc.queueStream([range(20), ['a', 'b'], ['c']]).pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    simple_queue()
