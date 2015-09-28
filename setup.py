from setuptools import setup

# workaround: nosetests don't exit cleanly with older
# python version (<=2.6 and even <2.7.4)
try:
    import multiprocessing
except ImportError:
    pass


# extract version from __init__.py
with open('pysparkling/__init__.py', 'r') as f:
    version_line = [l for l in f if l.startswith('__version__')][0]
    VERSION = version_line.split('=')[1].strip()[1:-1]


setup(
    name='pysparkling',
    version=VERSION,
    packages=['pysparkling',
              'pysparkling.fileio',
              'pysparkling.fileio.fs',
              'pysparkling.fileio.codec'],
    license='MIT',
    description='Pure Python implementation of the Spark RDD interface.',
    long_description=open('README.rst').read(),
    author='Sven Kreiss',
    author_email='me@svenkreiss.com',
    url='https://github.com/svenkreiss/pysparkling',

    install_requires=[],
    extras_require={
        'http': ['requests>=2.6.0'],
        's3': ['boto>=2.36.0'],
        'hdfs': ['hdfs>=1.0.0'],
    },
    entry_points={
        'console_scripts': [],
    },

    tests_require=[
        'nose>=1.3.4',
        'futures>=3.0.1',
        'cloudpickle>=0.1.0',
    ],
    test_suite='nose.collector',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: PyPy',
    ]
)
