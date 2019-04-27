.. highlight:: shell

============
Installation
============

**TwinDB Backup** is distributed via package repositories. We provide packages
for CentOS, Ubuntu and Debian operating systems.

Supported versions:

 * CentOS 6, 7
 * Ubuntu trusty, xenial, bionic, cosmic
 * Debian jessie and stretch

The installation process consists of two parts: installing the repository and
installing ``twindb-backup`` package from it.

Repository installation
-----------------------

The installation instructions are published on `the repository website`_.

For CentOS and alike operating systems:

.. code-block:: console

    curl -s https://packagecloud.io/install/repositories/TwinDB/main/script.rpm.sh | sudo bash

For Debian and Ubuntu the command is:

.. code-block:: console

    curl -s https://packagecloud.io/install/repositories/TwinDB/main/script.deb.sh | sudo bash

We also provide `the TwinDB Repo cookbook`_ for Chef users.


Package installation
--------------------

As soon as the repository is installed you can install the TwinDB Backup package.

For CentOS and RedHat:

.. code-block:: console

    yum install twindb-backup

For Debian and Ubuntu:

.. code-block:: console

    apt-get install twindb-backup

The package bundles TwinDB itself, Python, dependencies and tested version
of Percona Xtrabackup. The installed package requires about 800MB of disk space.
Make sure you have enough in ``/opt/``.

Besides the TwinDB Backup software the package installs also the config file
in ``/etc/twindb/twindb-backup.cfg`` and a cron configuration in
``/etc/cron.d/twindb-backup``.

For configuration see :ref:`usage`.

.. _the repository website: https://packagecloud.io/TwinDB/main/install
.. _the TwinDB Repo cookbook: https://supermarket.chef.io/cookbooks/twindb-repo
