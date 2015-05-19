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
            if expr.startswith(('s3://', 's3n://')):
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
            else:
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
