import timeit


def with_generator():
    return (x for x in range(1000))


def with_yield():
    for x in range(1000):
        yield x


if __name__ == '__main__':
    print(timeit.timeit(stmt='list(with_generator())',
                        setup='from __main__ import with_generator',
                        number=10000))
    print(timeit.timeit(stmt='list(with_yield())',
                        setup='from __main__ import with_yield',
                        number=10000))
