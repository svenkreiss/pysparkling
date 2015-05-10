"""Imitates SparkContext."""

import boto
import glob
import fnmatch

from .rdd import RDD


class Tokenizer(object):
    def __init__(self, expression):
        self.expression = expression

    def next(self, separator=None):
        if isinstance(separator, list):
            sep_pos = [self.expression.find(s) for s in separator]
            sep_pos = min(s for s in sep_pos if s >= 0)
        elif separator:
            sep_pos = self.expression.find(separator)
        else:
            sep_pos = -1

        if sep_pos < 0:
            value = self.expression
            self.expression = ''
            return value

        value = self.expression[:sep_pos]
        self.expression = self.expression[sep_pos+len(separator):]
        return value


class Context(object):
    def __init__(self, pool=None):
        if not pool:
            pool = DummyPool()

        self.ctx = {
            'pool': pool,
            's3_conn': None,
        }

    def parallelize(self, x, numPartitions=None):
        return RDD(x, self.ctx)

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
                lines += key.get_contents_as_string().splitlines()
            else:
                with open(f_name, 'r') as f:
                    lines += [l.rstrip('\n') for l in f]
        return self.parallelize(lines)

    def _get_s3_conn(self):
        if not self.ctx['s3_conn']:
            self.ctx['s3_conn'] = boto.connect_s3()
        return self.ctx['s3_conn']

    def _resolve_filenames(self, all_expr):
        files = []
        for expr in all_expr.split(','):
            expr = expr.strip()
            if expr.startswith('s3://') or expr.startswith('s3n://'):
                if '*' not in expr and '?' not in expr:
                    files.append(expr)
                    continue

                t = Tokenizer(expr)
                scheme = t.next('://')
                bucket_name = t.next('/')
                prefix = t.next(['*', '?'])

                bucket = self._get_s3_conn().get_bucket(
                    bucket_name,
                    validate=False
                )
                expr_after_bucket = expr[len(scheme)+3+len(bucket_name)+1:]
                files += [scheme+'://'+bucket_name+'/'+k.name
                          for k in bucket.list(prefix=prefix)
                          if fnmatch.fnmatch(k.name, expr_after_bucket)]
            else:
                files += glob.glob(expr)
        return files


class DummyPool(object):
    def __init__(self):
        pass

    def map(self, f, input_list):
        return [f(x) for x in input_list]
