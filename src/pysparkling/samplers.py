import math
import random

try:
    import numpy
except ImportError:
    numpy = None


def pysparkling_poisson(lambda_):
    if lambda_ == 0.0:
        return 0

    n = 0
    exp_neg_lambda = math.exp(-lambda_)
    prod = 1.0
    while True:
        prod *= random.random()
        if prod > exp_neg_lambda:
            n += 1
        else:
            return n


def poisson(lambda_):
    if numpy is not None:
        return numpy.random.poisson(lambda_)
    return pysparkling_poisson(lambda_)


class BernoulliSampler(object):
    def __init__(self, expectation):
        self.expectation = expectation

    def __call__(self, sample):
        return 1 if random.random() < self.expectation else 0


class PoissonSampler(object):
    def __init__(self, expectation):
        self.expectation = expectation

    def __call__(self, sample):
        return poisson(self.expectation)


class BernoulliSamplerPerKey(object):
    def __init__(self, expectations):
        self.expectations = expectations

    def __call__(self, sample):
        key = sample[0]
        return 1 if random.random() < self.expectations.get(key, 0.0) else 0


class PoissonSamplerPerKey(object):
    def __init__(self, expectations):
        self.expectations = expectations

    def __call__(self, sample):
        key = sample[0]
        return poisson(self.expectations.get(key, 0.0))
