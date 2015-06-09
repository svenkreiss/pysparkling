.. _parallel:


Parallelization
===============

Pysparkling supports parallelizations on the local machine and across clusters
of computers.


Threads and Processes
---------------------

Single machine parallelization either with
``concurrent.futures.ThreadPoolExecutor``,
``concurrent.futures.ProcessPoolExecutor`` and
``multiprocessing.Pool`` is supported.


StarCluster
-----------

Setup

.. code-block:: bash

    # install
    pip install starcluster

    # create configuration
    starcluster help  # choose the option to create a sample config file

    # add your user id, aws_access_key_id and aws_secret_access_key to config

    # create an ssh key (this creates a new key just for starcluster)
    # and registers it with AWS
    starcluster createkey starclusterkey -o ~/.ssh/starclusterkey.rsa

    # add this key to config:
    [key starclusterkey]
    KEY_LOCATION=~/.ssh/starclusterkey.rsa
    # and use this key in the cluster setup:
    KEYNAME = starclusterkey

    # disable the queue, Sun Grid Engine
    # (unnecessary for pysparkling and takes time during setup)
    DISABLE_QUEUE=True

    # to enable IPython parallel support, uncomment these lines in config:
    [plugin ipcluster]
    SETUP_CLASS = starcluster.plugins.ipcluster.IPCluster

    # and make sure you have this line inside the cluster section
    [cluster smallcluster]
    PLUGINS = ipcluster

    # start the cluster
    starcluster start smallcluster

    # check it has started
    starcluster listclusters

Currently use: ``ami-da180db2`` (Ubuntu 14.04 with 100GB EBS) on
``m3.medium`` instances.

Workarounds:

.. code-block:: bash

    # this seems to be a dependency that does not get installed
    pip install pexpect

    # to validate the ssh host, you need to log in once manually, to add it
    # to the list of known hosts
    starcluster sshmaster smallcluster

In Python, you should now be able to run

.. code-block:: python

    from IPython.parallel import Client

    # the exact command is printed after the cluster started
    rc = Client('/Users/sven/.starcluster/ipcluster/SecurityGroup:@sc-smallcluster-us-east-1.json',
                sshkey='/Users/sven/.ssh/starclusterkey.rsa', packer='pickle')

    view = rc[:]
    results = view.map(lambda x: x**30, range(8))
    print results.get()

which is also in ``tests/starcluster_simple.py``.


Install your own software that is not on pypi:

.. code-block:: python

    pip install wheel
    python setup.py bdist_wheel  # add --universal for Python2 and 3 packages
    starcluster put smallcluster dist/your_package_name.whl /opt/package_name

    # ssh into remote machine
    starcluster sshmaster smallcluster
    > pip install --upgrade pip
    > pip install wheel
    > pip2.7 install /opt/package_name/your_package_name.whl


