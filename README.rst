HMSClient
=========

This project aims to be an up to date Python client to interact with the Hive metastore
using the Thrift protocol.

Installation
------------

Install it with ``pip install hmsclient`` or directly from source

.. code-block:: python

    python setup.py install

Usage
-----

Using it from Python is simple:

.. code-block:: python

    from hmsclient import hmsclient
    client = hmsclient.HMSClient(host='localhost', port=9083)
    with client as c:
        c.check_for_named_partition('db', 'table', 'date=20180101')


Regenerate the Python thrift library
------------------------------------

The ``hmsclient.py`` is just a thin wrapper around the generated Python code to
interact with the metastore through the Thrift protocol.

To regenerate the code using a newer version of the ``.thrift`` files, you can
use ``generate.py`` (note: you need to have ``thrift`` installed, see here_)

.. code-block:: sh

    python generate.py --help

    Usage: generate.py [OPTIONS]

    Options:
      --fb303_url TEXT      The URL where the fb303.thrift file can be downloaded
      --metastore_url TEXT  The URL where the hive_metastore.thrift file can be
                            downloaded
      --package TEXT        The package where the client should be placed
      --subpackage TEXT     The subpackage where the client should be placed
      --help                Show this message and exit.

Otherwise the defaults will be used.

.. _here: https://thrift-tutorial.readthedocs.io/en/latest/installation.html
