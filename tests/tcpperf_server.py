from __future__ import print_function, division

from collections import defaultdict
import json
import logging
import math
import os
import pysparkling
import struct
import time


MEASUREMENT_POINTS = (100, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000)


class Server(object):
    def __init__(self, start_port=8123):
        self.port = start_port

    def client(self, n=2000, format_='hello', processes=2):
        for _ in range(processes):
            os.system('python tests/tcpperf_client.py '
                      '-n {} --port {} --format {} &'
                      ''.format(int(n / processes), self.port, format_))

    def run(self, n=2000, to_kv=None, format_='hello'):
        c = pysparkling.Context()
        stream_c = pysparkling.streaming.StreamingContext(c, 1.0)

        counts = []
        sensor_sums = defaultdict(float)
        sensor_squares = defaultdict(float)
        sensor_counts = defaultdict(int)
        if format_ not in ('bello', 'struct'):
            t = stream_c.socketTextStream('localhost', self.port)
        else:
            length = {'bello': 5, 'struct': 8}[format_]
            t = stream_c.socketBinaryStream('localhost', self.port, length)
        t.count().foreachRDD(lambda _, rdd: counts.append(rdd.collect()[0]))
        if to_kv is not None:
            def update(rdd):
                for k, v in rdd.collect():
                    sensor_sums[k] += sum(v)
                    sensor_squares[k] += sum(vv ** 2 for vv in v)
                    sensor_counts[k] += len(v)

            t.map(to_kv).groupByKey().foreachRDD(lambda _, rdd: update(rdd))

        self.client(n, format_=format_)

        stream_c.start()
        stream_c.awaitTermination(timeout=5.0)

        result = max(counts) if counts else 0
        sensor_expections = {
            # expectation of X and X^2
            k: (sensor_sums[k] / v, sensor_squares[k] / v)
            for k, v in sensor_counts.items()
        }
        sensors = {
            k: (ex_ex2[0], math.sqrt(ex_ex2[1] - ex_ex2[0] ** 2))
            for k, ex_ex2 in sensor_expections.items()
        }
        print('run: n = {}, counts = {}, result = {}'.format(n, counts, result))
        print('sensors = {}'.format(sensors))
        time.sleep(60)
        self.port += 1
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
        server = Server()
        for n in reversed(MEASUREMENT_POINTS):
            data = (
                n,
                server.run(n),
                server.run(n, None, 'bello'),
                server.run(n, kv_from_text, 'text'),
                server.run(n, kv_from_json, 'json'),
                server.run(n, kv_from_struct, 'struct'),
            )
            f.write(', '.join('{}'.format(d) for d in data) + '\n')
