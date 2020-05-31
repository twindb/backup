.. highlight:: shell

============
Installation
============

Supported versions:

 * CentOS 6, 7
 * Ubuntu trusty, xenial, bionic, cosmic
 * Debian jessie and stretch

**TwinDB Backup** can be installed from a DEB/RPM package.
The TwinDB Backup package can build on a machine with Docker service.

``make package`` will build the package for the operating system defined in the ``OS_VERSION`` environment variable.
Possible ``OS_VERSION`` values:

 * 6
 * 7
 * jessie
 * stretch
 * trusty
 * xenial
 * bionic
 * cosmic.

.. code-block:: console

    # export OS_VERSION=bionic
    # make package

.. note:: The build process requires a lot of available memory.
    If you see error ``g++: internal compiler error: Killed (program cc1plus)``,
    run ``make package`` on a bigger machine.

The package file will be generated in ``omnibus/pkg/``:

.. code-block:: console

    $ ls omnibus/pkg/*.deb
    omnibus/pkg/twindb-backup_2.20.0-1_amd64.deb

Once the package is built you can install it with rpm/dpkg or upload it to your repository
and install it with apt or yum.

The package bundles TwinDB itself, Python, dependencies and tested version
of Percona Xtrabackup. The installed package requires about 800MB of disk space.
Make sure you have enough in ``/opt/``.

Besides the TwinDB Backup software the package installs also the config file
in ``/etc/twindb/twindb-backup.cfg`` and a cron configuration in
``/etc/cron.d/twindb-backup``.

For configuration see :ref:`usage`.

.. _the repository website: https://packagecloud.io/TwinDB/main/install
.. _the TwinDB Repo cookbook: https://supermarket.chef.io/cookbooks/twindb-repo
