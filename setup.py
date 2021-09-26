from setuptools import find_packages, setup
import versioneer

setup(
    name='pysparkling',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages(),
    license='MIT',
    description='Pure Python implementation of the Spark RDD interface.',
    long_description=open('README.rst').read(),
    author='pysparkling contributors',
    url='https://github.com/svenkreiss/pysparkling',

    install_requires=[
        'boto==2.49.0',
        'future==0.18.2',
        'requests==2.26.0',
        'pytz==2021.1',
        'python-dateutil==2.8.2',
        'pythonsqlparser==0.1.2',
    ],
    extras_require={
        'hdfs': ['hdfs>=2.0.0'],
        'performance': ['matplotlib==3.3.4'],
        'streaming': ['tornado==6.1'],
        'dev': [
            'antlr4-python3-runtime==4.7.1',
        ],
        'sql': [
            'numpy==1.19.5',
            'pandas==1.1.5',
        ],
        'tests': [
            'backports.tempfile==1.0rc1',
            'cloudpickle==1.6.0',
            'futures==3.1.1',
            'pylint==2.10.2',
            'pylzma==0.5.0',
            'memory-profiler==0.58.0',
            'pycodestyle==2.7.0',
            'pytest==6.2.4',
            'pytest-cov==2.12.1',
            'isort==5.9.3',
            'tornado==6.1',
            'parameterized==0.7.4',
        ],
        'scripts': [
            'ipyparallel==6.3.0',
            'pyspark==3.1.2',
            'matplotlib==3.3.4',
        ]
    },

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: PyPy',
    ]
)
