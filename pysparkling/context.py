"""Imitates SparkContext."""

from __future__ import division

import boto
import glob
import math
import fnmatch
import logging
import functools

from .rdd import RDD
from .broadcast import Broadcast
from .utils import Tokenizer
from .partition import Partition
from .task_context import TaskContext

log = logging.getLogger(__name__)


class Context(object):
    def __init__(self, pool=None):
        if not pool:
            pool = DummyPool()

        self._pool = pool
        self._s3_conn = None
        self._last_rdd_id = 0

    def broadcast(self, x):
        return Broadcast(x)

    def newRddId(self):
        self._last_rdd_id += 1
        return self._last_rdd_id

    def parallelize(self, x, numPartitions=None):
        if not numPartitions:
            return RDD([Partition(x, 0)], self)

        stride_size = int(math.ceil(len(x)/numPartitions))
        def partitioned():
            for i in range(numPartitions):
                yield Partition(x[i*stride_size:(i+1)*stride_size], i)

        return RDD(partitioned(), self)

    def runJob(self, rdd, func, partitions=None, allowLocal=False, resultHandler=None):
        """func is of the form func(TaskContext, Iterator over elements)"""
        # TODO: this is the place to insert proper schedulers
        def map_func(partition):
            task_context = TaskContext(stage_id=0, partition_id=partition.index)
            return func(task_context, rdd.compute(partition, task_context))
        map_result = self._pool.map(map_func, rdd.partitions())
        log.info('Map jobs generated.')

        if resultHandler is not None:
            return resultHandler(map_result)
        return map_result

    def textFile(self, filename):
        lines = []
        for f_name in self._resolve_filenames(filename):
            if f_name.startswith('s3://') or f_name.startswith('s3n://'):
                t = Tokenizer(f_name)
                t.next('//')  # skip scheme
                bucket_name = t.next('/')
                key_name = t.next()
                conn = self._get_s3_conn()
                bucket = conn.get_bucket(bucket_name, validate=False)
                key = bucket.get_key(key_name)
                lines += [l.rstrip('\n')
                          for l in key.get_contents_as_string().splitlines()]
            else:
                f_name_local = f_name
                if f_name_local.startswith('file://'):
                    f_name_local = f_name_local[7:]
                with open(f_name_local, 'r') as f:
                    lines += [l.rstrip('\n') for l in f]

        rdd = self.parallelize(lines)
        rdd._name = filename
        return rdd

    def union(self, rdds):
        return RDD((p for rdd in rdds for p in rdd.p()), self)

    def _get_s3_conn(self):
        if not self._s3_conn:
            self._s3_conn = boto.connect_s3()
        return self._s3_conn

    def _resolve_filenames(self, all_expr):
        files = []
        for expr in all_expr.split(','):
            expr = expr.strip()
            if expr.startswith('s3://') or expr.startswith('s3n://'):
                t = Tokenizer(expr)
                scheme = t.next('://')
                bucket_name = t.next('/')
                prefix = t.next(['*', '?'])

                bucket = self._get_s3_conn().get_bucket(
                    bucket_name,
                    validate=False
                )
                expr_after_bucket = expr[len(scheme)+3+len(bucket_name)+1:]
                for k in bucket.list(prefix=prefix):
                    if fnmatch.fnmatch(k.name, expr_after_bucket) or \
                       fnmatch.fnmatch(k.name, expr_after_bucket+'/part*'):
                        files.append(scheme+'://'+bucket_name+'/'+k.name)
            else:
                expr_local = expr
                if expr_local.startswith('file://'):
                    expr_local = expr_local[7:]
                files += glob.glob(expr_local)+glob.glob(expr_local+'/part*')
        return files


class DummyPool(object):
    def __init__(self):
        pass

    def map(self, f, input_list):
        return (f(x) for x in input_list)
