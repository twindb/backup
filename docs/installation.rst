.. highlight:: shell

============
Installation
============

Requirements
------------

TwinDB Backup package will pull all necessary dependencies except ``aws`` tool. We recommend to install it from PyPi.

.. code-block:: console

    # pip install awscli

Stable release
--------------

To install TwinDB Backup, run this command in your terminal:

.. code-block:: console

    # yum install https://twindb.com/twindb-release-latest.noarch.rpm
    # yum install twindb-backup

This is the preferred method to install TwinDB Backup, as it will always install the most recent stable release.


From sources
------------

The sources for TwinDB Backup can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone git://github.com/twindb/twindb_backup

Or download the `tarball`_:

.. code-block:: console

    $ curl  -OL https://github.com/twindb/twindb_backup/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ make install


.. _Github repo: https://github.com/twindb/twindb_backup
.. _tarball: https://github.com/twindb/twindb_backup/tarball/master
