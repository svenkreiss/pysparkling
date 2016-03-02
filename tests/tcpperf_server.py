from __future__ import print_function

import os
import time
import logging
import pysparkling


def client(n=2000, port=8123, format_='hello', processes=2):
    for _ in range(processes):
        os.system('python tests/tcpperf_client.py '
                  '-n {} --port {} --format {} &'
                  ''.format(int(n/processes), port, format_))


def hello(n=2000, port=8123):
    c = pysparkling.Context()
    stream_c = pysparkling.streaming.StreamingContext(c, 1.0)

    data = []
    t = stream_c.socketTextStream('localhost', port)
    t.count().foreachRDD(lambda _, rdd: data.append(rdd.collect()[0]))

    client(n, port)

    stream_c.start()
    stream_c.awaitTermination(timeout=5.0)

    result = max(data) if data else 0
    print('hello: n = {}, data = {}, result = {}'.format(n, data, result))
    time.sleep(60)
    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
    hello_data = [
        (n, hello(n, p))
        for n, p in reversed((
            (100, 8123), (1000, 8124), (2000, 8125), (3000, 8126),
            (4000, 8127), (5000, 8128), (6000, 8129), (7000, 8130),
            (8000, 8131)
        ))
    ]
    print(hello_data)
