import json

from collections import defaultdict
import logging
import math
import os
import struct
import time

import pysparkling

N_CONNECTIONS = (100, 1000, 2000, 3000, 3500, 4000, 4500, 5000,
                 6000, 7000, 8000)
N_CONNECTIONS_1K = (10, 20, 30, 40, 45, 50, 60, 70, 80, 90, 100)


class Server(object):
    def __init__(self, pause=60, values=1, start_port=8123, processes=2):
        self.pause = pause
        self.values = values
        self.port = start_port
        self.processes = processes

    def client(self, n=2000, format_='hello'):
        for _ in range(self.processes):
            os.system('python tests/tcpperf_client.py '
                      '-n {} --port {} --format {} --values {} &'
                      ''.format(int(n / self.processes), self.port, format_,
                                self.values))

    def _run_process(self, n, to_kv, format_):
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

        return (
            counts,
            sensor_sums,
            sensor_squares,
            sensor_counts
        )

    def run(self, n=2000, to_kv=None, format_='hello'):
        counts, sensor_sums, sensor_squares, sensor_counts = self._run_process(n, to_kv, format_)

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
        print('run: n = {}, counts = {}, result = {}'
              ''.format(n, counts, result))
        print('sensors = {}'.format(sensors))
        time.sleep(self.pause)
        self.port += 1
        return result


def main():
    logging.basicConfig(level=logging.WARNING)

    def kv_from_text(text):
        k, _, v = text.partition('|')
        return k, float(v)

    def kv_from_json(text):
        j = json.loads(text)
        return list(j.items())[0]

    def kv_from_struct(b):
        s, v = struct.unpack('If', b)
        return 'sensor{}'.format(s), v

    with open('tests/tcpperf_messages.csv', 'w') as f:
        f.write('# messages, hello, text, json, bello, struct\n')
        server_1k = Server(pause=2, values=1000, processes=5)
        for n in reversed(N_CONNECTIONS_1K):
            data = (
                n * 1000,
                server_1k.run(n),
                server_1k.run(n, None, 'bello'),
                server_1k.run(n, kv_from_text, 'text'),
                server_1k.run(n, kv_from_json, 'json'),
                server_1k.run(n, kv_from_struct, 'struct'),
            )
            f.write(', '.join('{}'.format(d) for d in data) + '\n')

    with open('tests/tcpperf_connections.csv', 'w') as f:
        f.write('# messages, hello, text, json, bello, struct\n')
        server = Server()
        for n in reversed(N_CONNECTIONS):
            data = (
                n,
                server.run(n),
                server.run(n, None, 'bello'),
                server.run(n, kv_from_text, 'text'),
                server.run(n, kv_from_json, 'json'),
                server.run(n, kv_from_struct, 'struct'),
            )
            f.write(', '.join('{}'.format(d) for d in data) + '\n')


if __name__ == '__main__':
    main()
