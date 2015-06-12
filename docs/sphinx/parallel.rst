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


ipcluster and IPython.parallel
------------------------------

Local test setup:

.. code-block:: bash

    ipcluster start --n=2

.. code-block:: python

    from IPython.parallel import Client

    c = Client()
    print(c[:].map(lambda _: 'hello world', range(2)).get())

which should print ``['hello world', 'hello world']``.

To run on a cluster, create a profile:

.. code-block:: bash

    ipython profile create --parallel --profile=smallcluster

    # start controller:
    # Creates ~/.ipython/profile_smallcluster/security/ipcontroller-engine.json
    # which is used by the engines to identify the location of this controller.
    # This is the local-only IP address. Substitute with the machines IP
    # address so that the engines can find it.
    ipcontroller --ip=127.0.0.1 --port=7123 --profile=smallcluster

    # start engines (assuming they have access to the
    # ipcontroller-engine.json file)
    ipengine --profile=smallcluster

Test it in Python:

.. code-block:: python

    from IPython.parallel import Client

    c = Client(profile='smallcluster')
    print(c[:].map(lambda _: 'hello world', range(2)).get())

If you don't want to start the engines manually, ``ipcluster`` comes with
"Launchers" that can start them for you:
https://ipython.org/ipython-doc/dev/parallel/parallel_process.html#using-ipcluster-in-ssh-mode


StarCluster
-----------

Setting up StarCluster was an experiment. However it does not integrate well
with the rest of our EC2 infrastructure, so we switched to a Chef based setup
where we use ``ipcluster`` directly. A blocker was that the number of engines
per node is not configurable and we have many map jobs that wait on external
responses.

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
    starcluster put smallcluster dist/your_package_name.whl /home/sgeadmin/your_package_name.whl

    # ssh into remote machine
    starcluster sshmaster smallcluster
    > pip install --upgrade pip
    > pip install wheel
    > pip2.7 install /home/sgeadmin/your_package_name.whl


