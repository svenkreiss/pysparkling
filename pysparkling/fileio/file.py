from __future__ import absolute_import

import os
import boto
import fnmatch
import logging

from ..utils import Tokenizer

log = logging.getLogger(__name__)


class File(object):
    _s3_conn = None

    def __init__(self):
        """This is an abstract class. Use WholeFile or any other future
        implementation."""
        pass

    @staticmethod
    def _get_s3_conn():
        if not File._s3_conn:
            File._s3_conn = boto.connect_s3()
        return File._s3_conn

    @staticmethod
    def resolve_filenames(all_expr):
        files = []
        for expr in all_expr.split(','):
            expr = expr.strip()
            path_type = File.path_type(expr)
            if path_type == 's3':
                t = Tokenizer(expr)
                scheme = t.next('://')
                bucket_name = t.next('/')
                prefix = t.next(['*', '?'])

                bucket = File._get_s3_conn().get_bucket(
                    bucket_name,
                    validate=False
                )
                expr_after_bucket = expr[len(scheme)+3+len(bucket_name)+1:]
                for k in bucket.list(prefix=prefix):
                    if fnmatch.fnmatch(k.name, expr_after_bucket) or \
                       fnmatch.fnmatch(k.name, expr_after_bucket+'/part*'):
                        files.append(scheme+'://'+bucket_name+'/'+k.name)
            elif path_type == 'local':
                expr_local = expr
                if expr_local.startswith('file://'):
                    expr_local = expr_local[7:]
                if os.path.isfile(expr_local):
                    files.append(expr_local)
                    continue
                t = Tokenizer(expr_local)
                prefix = t.next(['*', '?'])
                for root, dirnames, filenames in os.walk(prefix):
                    root_wo_slash = root[:-1] if root.endswith('/') else root
                    for filename in filenames:
                        if fnmatch.fnmatch(root_wo_slash+'/'+filename,
                                           expr_local) or \
                           fnmatch.fnmatch(root_wo_slash+'/'+filename,
                                           expr_local+'/part*'):
                            files.append(root_wo_slash+'/'+filename)
                # files += glob.glob(expr_local)+glob.glob(expr_local+'/part*')
        log.debug('Filenames: {0}'.format(files))
        return files

    @staticmethod
    def exists(path):
        """Checks both for a file at this location or a directory."""
        pt = File.path_type(path)
        if pt == 's3':
            t = Tokenizer(path)
            t.next('//')  # skip scheme
            bucket_name = t.next('/')
            key_name = t.next()
            conn = File._get_s3_conn()
            bucket = conn.get_bucket(bucket_name, validate=False)
            return (bucket.get_key(key_name) or
                    any(True for _ in bucket.list(prefix=key_name+'/')))
        elif pt == 'local':
            path_local = path
            if path_local.startswith('file://'):
                path_local = path_local[7:]
            return os.path.exists(path_local) or os.path.exists(path_local+'/')
        else:
            log.warning('Could not determine whether {0} exists due to '
                        'unhandled path type {1}.'.format(path, pt))

        return None

    @staticmethod
    def path_type(path):
        if path.startswith(('s3://', 's3n://')):
            return 's3'
        elif path.startswith(('http://', 'https://')):
            return 'http'
        else:
            return 'local'
