from setuptools import setup, find_packages

# workaround: nosetests don't exit cleanly with older
# python version (<=2.6 and even <2.7.4)
try:
    import multiprocessing  # noqa
except ImportError:
    pass


# extract version from __init__.py
with open('src/pysparkling/__init__.py', 'r') as f:
    version_line = [l for l in f if l.startswith('__version__')][0]
    VERSION = version_line.split('=')[1].strip()[1:-1]


setup(
    name='pysparkling',
    version=VERSION,
    packages=find_packages(),
    license='MIT',
    description='Pure Python implementation of the Spark RDD interface.',
    long_description=open('README.rst').read(),
    author='pysparkling contributors',
    url='https://github.com/svenkreiss/pysparkling',

    install_requires=[
        'boto>=2.36.0',
        'future>=0.15',
        'requests>=2.6.0',
        'pytz>=2019.3',
        'python-dateutil>=2.8.0'
    ],
    extras_require={
        'hdfs': ['hdfs>=2.0.0'],
        'pandas': ['pandas>=0.23.2'],
        'performance': ['matplotlib>=1.5.3'],
        'streaming': ['tornado>=4.3'],
        'test': [
            'backports.tempfile==1.0rc1',
            'cloudpickle>=0.1.0',
            'futures>=3.0.1',
            'pylint>=2.3,<2.6',
            'memory-profiler>=0.47',
            'pytest',
            'tornado>=4.3',
        ]
    },
    entry_points={
        'console_scripts': [],
    },

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: PyPy',
    ]
)
