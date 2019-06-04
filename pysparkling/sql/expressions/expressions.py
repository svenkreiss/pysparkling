class Expression(object):
    def eval(self, row):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError

