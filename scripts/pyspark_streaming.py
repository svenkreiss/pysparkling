"""Explore PySpark API.

Run with `spark-submit scripts/pyspark_streaming.py`.
"""
import time

import pyspark.streaming


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


def updateStateByKey(ssc):
    def processStateUpdateByKey(input_stream, state):
        print('i', input_stream)
        print('s', state)
        return state if not input_stream else input_stream[-1]

    ssc.checkpoint('checkpoints/')
    (ssc
     .queueStream([[('a', 1), ('b', 3)], [('a', 2), ('a', 5), ('c', 4)]])
     .updateStateByKey(processStateUpdateByKey)
     .pprint()
     )


def stream_log(ssc):
    ssc.textFileStream('/var/log/system.log*').pprint()


def stream_queue_default(ssc):
    (ssc
     .queueStream([[4], [2]], default=['placeholder'])
     .foreachRDD(lambda rdd: print(rdd.collect())))


def join_with_repeated_keys(ssc):
    s1 = ssc.queueStream([[('a', 4), ('a', 2)], [('c', 7)]])
    s2 = ssc.queueStream([[('b', 1), ('b', 3)], [('c', 8)]])
    (
        s1.fullOuterJoin(s2)
        .foreachRDD(lambda rdd: print(sorted(rdd.collect())))
    )


def union(ssc):
    odd = ssc.queueStream([[1], [3], [5]])
    even = ssc.queueStream([[2], [4], [6]])
    (
        odd.union(even)
        .foreachRDD(lambda rdd: print(rdd.collect()))
    )


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


if __name__ == '__main__':
    sc = pyspark.SparkContext()
    quiet_logs(sc)
    ssc = pyspark.streaming.StreamingContext(sc, 1)

    # simple_queue(ssc)
    # simple_queue_count(ssc)
    # simple_queue_one_at_a_time(ssc)
    # save_text(ssc)
    # window(ssc)
    # updateStateByKey(ssc)
    # stream_log(ssc)
    # stream_queue_default(ssc)
    # join_with_repeated_keys(ssc)
    union(ssc)

    ssc.start()
    time.sleep(3.0)
    ssc.stop(stopGraceFully=True)
