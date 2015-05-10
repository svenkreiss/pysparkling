"""TLD implementation."""

import random


class TLD(object):
    def __init__(self, x, ctx):
        self.x = x
        self.ctx = ctx

    def collect(self):
        return self.x

    def take(self, n):
        return self.x[:n]

    def takeSample(self, n):
        return random.sample(self.x, n)

    def foreach(self, f):
        self.x = self.ctx['pool'].map(f, self.x)

    def map(self, f):
        return TLD(self.ctx['pool'].map(f, self.x), self.ctx)

    def cache(self):
        pass
