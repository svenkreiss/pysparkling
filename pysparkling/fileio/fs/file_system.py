import logging

log = logging.getLogger(__name__)


class FileSystem(object):
    def __init__(self, file_name):
        self.file_name = file_name

    @staticmethod
    def resolve_filenames(expr):
        log.error('Cannot resolve: {0}'.format(expr))

    def exists(self):
        log.warning('Could not determine whether {0} exists due to '
                    'unhandled scheme.'.format(self.file_name))

    def load(self):
        log.error('Cannot load: {0}'.format(self.file_name))

    def dump(self, stream):
        log.error('Cannot dump: {0}'.format(self.file_name))

    def make_public(self, recursive=False):
        log.warning('Cannot make {0} public.'.format(self.file_name))
