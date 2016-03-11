from __future__ import print_function, division

import os
import json
import math
import time
import struct
import logging
import pysparkling
from collections import defaultdict


MEASUREMENT_POINTS = (
    (100, 8123), (1000, 8124), (2000, 8125), (3000, 8126),
    (4000, 8127), (5000, 8128), (6000, 8129), (7000, 8130),
    (8000, 8131)
)


def client(n=2000, port=8123, format_='hello', processes=2):
    for _ in range(processes):
        os.system('python tests/tcpperf_client.py '
                  '-n {} --port {} --format {} &'
                  ''.format(int(n/processes), port, format_))


def run(n=2000, port=8123, to_kv=None, format_='hello'):
    c = pysparkling.Context()
    stream_c = pysparkling.streaming.StreamingContext(c, 1.0)

    counts = []
    sensor_sums = defaultdict(float)
    sensor_squares = defaultdict(float)
    sensor_counts = defaultdict(int)
    if format_ not in ('bello', 'struct'):
        t = stream_c.socketTextStream('localhost', port)
    else:
        l = {'bello': 5, 'struct': 8}[format_]
        t = stream_c.socketBinaryStream_('localhost', port, l)
    t.count().foreachRDD(lambda _, rdd: counts.append(rdd.collect()[0]))
    if to_kv is not None:
        def update(rdd):
            for k, v in rdd.collect():
                sensor_sums[k] += sum(v)
                sensor_squares[k] += sum(vv**2 for vv in v)
                sensor_counts[k] += len(v)

        t.map(to_kv).groupByKey().foreachRDD(lambda _, rdd: update(rdd))

    client(n, port, format_=format_)

    stream_c.start()
    stream_c.awaitTermination(timeout=5.0)

    result = max(counts) if counts else 0
    sensor_expections = {
        k: (sensor_sums[k]/v, sensor_squares[k]/v)  # expectation of X and X^2
        for k, v in sensor_counts.items()
    }
    sensors = {
        k: (ex_ex2[0], math.sqrt(ex_ex2[1]-ex_ex2[0]**2))
        for k, ex_ex2 in sensor_expections.items()
    }
    print('run: n = {}, counts = {}, result = {}'.format(n, counts, result))
    print('sensors = {}'.format(sensors))
    time.sleep(60)
    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)

    def kv_from_text(text):
        k, _, v = text.partition('|')
        return (k, float(v))

    def kv_from_json(text):
        j = json.loads(text)
        return list(j.items())[0]

    def kv_from_struct(b):
        s, v = struct.unpack('If', b)
        return ('sensor{}'.format(s), v)

    with open('tests/tcpperf_connections.csv', 'w') as f:
        f.write('# messages, hello, text, json, bello, struct\n')
        for n, p in MEASUREMENT_POINTS:
            data = (
                n,
                run(n, p),
                run(n, p, None, 'bello'),
                run(n, p, kv_from_text, 'text'),
                run(n, p, kv_from_json, 'json'),
                run(n, p, kv_from_struct, 'struct'),
            )
            f.write(', '.join('{}'.format(d) for d in data)+'\n')
