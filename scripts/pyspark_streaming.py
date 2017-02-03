from __future__ import print_function

import pyspark
import pyspark.streaming
import time


def simple_queue(ssc):
    ssc.queueStream([range(5), ['a', 'b'], ['c']], oneAtATime=False).pprint()


def simple_queue_count(ssc):
    (ssc
     .queueStream([range(5), ['a', 'b'], ['c']], oneAtATime=False)
     .count()
     .foreachRDD(lambda t, r: print('>>>>>>>>>>>>>>', t, r.collect())))


def simple_queue_one_at_a_time(ssc):
    ssc.queueStream([range(5), ['a', 'b'], ['c']], oneAtATime=True).pprint()


def save_text(ssc):
    (ssc
     .queueStream([range(5), ['a', 'b'], ['c']], oneAtATime=True)
     .saveAsTextFiles('scripts/textout/'))


if __name__ == '__main__':
    sc = pyspark.SparkContext()
    ssc = pyspark.streaming.StreamingContext(sc, 1)

    # simple_queue(ssc)
    # simple_queue_count(ssc)
    # simple_queue_one_at_a_time(ssc)
    save_text(ssc)

    ssc.start()
    time.sleep(3.0)
    ssc.stop(stopGraceFully=True)
