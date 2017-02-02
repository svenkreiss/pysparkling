from __future__ import print_function

import pyspark
import pyspark.streaming
import time


def simple_queue():
    ssc.queueStream([range(5), ['a', 'b'], ['c']], oneAtATime=False).pprint()


def simple_queue_count():
    (ssc
     .queueStream([range(5), ['a', 'b'], ['c']], oneAtATime=False)
     .count()
     .foreachRDD(lambda t, r: print('>>>>>>>>>>>>>>', t, r.collect())))


def simple_queue_one_at_a_time():
    ssc.queueStream([range(5), ['a', 'b'], ['c']], oneAtATime=True).pprint()


if __name__ == '__main__':
    sc = pyspark.SparkContext()
    ssc = pyspark.streaming.StreamingContext(sc, 1)

    # simple_queue()
    simple_queue_count()
    # simple_queue_one_at_a_time()

    ssc.start()
    time.sleep(3.0)
    ssc.stop(stopGraceFully=True)
