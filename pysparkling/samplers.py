import random


class BernoulliSampler(object):
    def __init__(self, expectation):
        self.expectation = expectation

    def __call__(self, sample):
        return 1 if random.random() < self.expectation else 0


class PoissonSampler(object):
    def __init__(self, expectation):
        self.expectation = expectation

    def __call__(self, sample):
        return random.poisson(self.expectation, 1)


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
        return random.poisson(self.expectations.get(key, 0.0), 1)
