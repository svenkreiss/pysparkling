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


def window(ssc):
    (ssc
     .queueStream([[1], [2], [3], [4], [5], [6]])
     .window(3)
     .foreachRDD(lambda rdd: print('>>>>>>>>>', rdd.collect())))


def union(ssc):
    ds1 = ssc.queueStream([[1], [2], [3], [4], [5], [6]])
    ds2 = ssc.queueStream([[1], [2], [3], [4], [5], [6]])
    ssc.union(ds1, ds2).pprint()


def updateStateByKey(ssc):
    ssc.checkpoint('checkpoints/')
    (ssc
     .queueStream([[('a', 1), ('b', 3)], [('a', 2), ('c', 4)]])
     .updateStateByKey(lambda input_stream, state:
                       state if not input_stream else input_stream[-1])
     .pprint()
     )


if __name__ == '__main__':
    sc = pyspark.SparkContext()
    ssc = pyspark.streaming.StreamingContext(sc, 1)

    # simple_queue(ssc)
    # simple_queue_count(ssc)
    # simple_queue_one_at_a_time(ssc)
    # save_text(ssc)
    # window(ssc)
    # union(ssc)
    updateStateByKey(ssc)

    ssc.start()
    time.sleep(3.0)
    ssc.stop(stopGraceFully=True)
