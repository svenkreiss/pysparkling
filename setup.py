from setuptools import setup

# workaround: nosetests don't exit cleanly with older
# python version (<=2.6 and even <2.7.4)
try:
    import multiprocessing
except ImportError:
    pass


# extract version from __init__.py
with open('pytld/__init__.py', 'r') as f:
    version_line = [l for l in f if l.startswith('__version__')][0]
    VERSION = version_line.split('=')[1].strip()[1:-1]


setup(
    name='pytld',
    version=VERSION,
    packages=['pytld'],
    license='MIT',
    description='Data analysis tool using Flask, WebSockets and d3.js.',
    long_description=open('README.rst').read(),
    author='Sven Kreiss',
    author_email='me@svenkreiss.com',
    url='https://github.com/svenkreiss/pytld',

    install_requires=[
        # 'pykka>=1.2.0',
    ],
    entry_points={
        'console_scripts': [],
    },

    tests_require=[
        'nose>=1.3.4',
        'coverage>=3.7.1',
    ],
    test_suite='nose.collector',

    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ]
)
