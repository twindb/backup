.. highlight:: shell

============
Installation
============

Supported versions:

 * Ubuntu jammy, focal
 * CentOS 7

**TwinDB Backup** can be installed from a DEB package.
The packages are available in the `Releases <https://github.com/twindb/backup/releases>`_.

How to build TwinDB Backup manually
-----------------------------------

The TwinDB Backup package can build on a machine with Docker service.

``make package`` will build the package for the operating system defined in the ``OS_VERSION`` environment variable.
Possible ``OS_VERSION`` values:

 * jammy
 * focal
 * 7 (for CentOS 7)

.. code-block:: console

    # export OS_VERSION=jammy
    # make package

.. note:: The build process requires a lot of available memory.
    If you see error ``g++: internal compiler error: Killed (program cc1plus)``,
    run ``make package`` on a bigger machine.

The package file will be generated in ``omnibus/pkg/``:

.. code-block:: console

    $ ls omnibus/pkg/*.deb
    omnibus/pkg/twindb-backup_3.2.1-1_amd64.deb

Once the package is built you can install it with rpm/dpkg or upload it to your repository
and install it with apt or yum.

The package bundles TwinDB itself, Python, and dependencies. The installed package requires about 800MB of disk space.
Make sure you have enough in ``/opt/``.

Up to versiom 2.20.2 TwinDB Backup bunded Percona XtraBackup.
In upcoming releases XtraBackup needs to be installed separately.

Besides the TwinDB Backup software the package installs also the config file
in ``/etc/twindb/twindb-backup.cfg`` and a cron configuration in
``/etc/cron.d/twindb-backup``.

For configuration see :ref:`usage`.
