from __future__ import division

import pysparkling


def test_mean():
    d = [1, 4, 9, 160]
    s = pysparkling.StatCounter(d)
    assert sum(d) / len(d) == s.mean()


if __name__ == '__main__':
    test_mean()
