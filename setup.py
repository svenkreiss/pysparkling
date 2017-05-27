from setuptools import setup

# workaround: nosetests don't exit cleanly with older
# python version (<=2.6 and even <2.7.4)
try:
    import multiprocessing  # noqa
except ImportError:
    pass


# extract version from __init__.py
with open('pysparkling/__init__.py', 'r') as f:
    version_line = [l for l in f if l.startswith('__version__')][0]
    VERSION = version_line.split('=')[1].strip()[1:-1]


setup(
    name='pysparkling',
    version=VERSION,
    packages=[
        'pysparkling',
        'pysparkling.fileio',
        'pysparkling.fileio.fs',
        'pysparkling.fileio.codec',
        'pysparkling.streaming',
    ],
    license='MIT',
    description='Pure Python implementation of the Spark RDD interface.',
    long_description=open('README.rst').read(),
    author='Sven Kreiss',
    author_email='me@svenkreiss.com',
    url='https://github.com/svenkreiss/pysparkling',

    install_requires=[
        'boto>=2.36.0',
        'future>=0.15',
        'requests>=2.6.0',
    ],
    extras_require={
        'hdfs': ['hdfs>=2.0.0'],
        'performance': ['matplotlib>=1.5.3'],
        'streaming': ['tornado>=4.3'],
        'tests': [
            'backports.tempfile==1.0rc1',
            'cloudpickle>=0.1.0',
            'doctest-ignore-unicode>=0.1.2',
            'futures>=3.0.1',
            'hacking>=0.11.0',
            'nose>=1.3.4',
            'tornado>=4.3',
        ]
    },
    entry_points={
        'console_scripts': [],
    },

    tests_require=[
        'nose>=1.3.4',
    ],
    test_suite='nose.collector',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: PyPy',
    ]
)
