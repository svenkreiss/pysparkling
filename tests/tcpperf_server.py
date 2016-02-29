from __future__ import print_function

import os
import pysparkling


def main():
    c = pysparkling.Context()
    stream_c = pysparkling.streaming.StreamingContext(c, 1.0)

    t = stream_c.socketTextStream('localhost', 8123)
    t.count().foreachRDD(lambda _, rdd: print(rdd.collect()))

    stream_c.start()
    stream_c.awaitTermination(timeout=10.0)


if __name__ == '__main__':
    os.system('python tests/tcpperf_client.py -n 2000 &')
    os.system('python tests/tcpperf_client.py -n 2000 &')
    main()
