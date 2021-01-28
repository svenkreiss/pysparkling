"""Benchmark csv reading performance."""

import argparse
import pysparkling
import random
from string import ascii_uppercase


def create_csv(filename, lines=10000000, columns=12):
    with open(filename, 'w') as f:
        column_names = ','.join(ascii_uppercase[i] for i in range(columns))
        f.write(f'{column_names}\n')

        for _ in range(lines):
            values = ','.join(
                f'{100 * (c + 1) * random.random():.3f}'
                for c in range(columns)
            )
            f.write(f'{values}\n')


def read_csv(filename):
    c = pysparkling.Context()
    r = c.textFile(filename)
    r = r.map(lambda l: l + 'something else')
    print(r.count())


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--create', default=False, action='store_true',
                   help='create csv test file')
    p.add_argument('--testfile', default='test.csv',
                   help='the test file')
    args = p.parse_args()

    if args.create:
        create_csv(filename=args.testfile)
    else:
        read_csv(filename=args.testfile)
