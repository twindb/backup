=============
TwinDB Backup
=============


.. image:: https://img.shields.io/codecov/c/github/twindb/backup.svg
    :target: https://codecov.io/gh/twindb/backup

.. image:: https://readthedocs.org/projects/twindb-backup/badge/
    :target: https://twindb-backup.readthedocs.io/
    :alt: Documentation Status

.. image:: https://img.shields.io/gitter/room/twindb/twindb-backup.svg
    :target: https://gitter.im/twindb/backup
    :alt: Join the chat at https://gitter.im/twindb/backup

.. image:: https://pyup.io/repos/github/twindb/backup/shield.svg
    :target: https://pyup.io/repos/github/twindb/backup/
    :alt: Updates

TwinDB Backup is a multipurpose tool for backing up MySQL database and regulare files/directories on the file system.
It can store backup copies on a remote SSH server, Amazon S3 or
Google Cloud Storage.

TwinDB Backup accepts a backup copy stream from any of supported sources
(MySQL Server, Percona Server, Percona XtraDB Cluster, or file system)
and redirects the stream to a series of configurable modifiers.

The modifiers can compress the stream, encrypt it, and save a copy of
the stream on the local disk.

Compression options:

- gzip
- bzip2
- lbzip2
- pigz

Encryption options:

- Public/private key encryption

Because TwinDB Backup encrypts the stream itself it ensures transfer encryption
as well as encryption at rest.

After the stream passed all modifiers it is sent to one of the configured
backup destination. It can be:

- Amazon S3 bucket
- Google Cloud Storage bucket
- Any server with SSH demon

.. figure:: https://user-images.githubusercontent.com/1763754/56677794-20901b80-6676-11e9-8f71-8de0b0b6f066.png
    :width: 800px
    :align: center
    :height: 600px
    :alt: TwinDB Backup Architecture
    :figclass: align-center

    TwinDB Backup Architecture

The tool can easily restore the backup copies.
Read full documentation on https://twindb-backup.readthedocs.io.


Features
--------

**TwinDB Backup** key features:

- MySQL full and incremental backups
- Zero seconds Recovery Point Objective (RPO) with MySQL binary log backups
- Percona Xtradb Cluster backups
- Files/directories backups
- Backups verification
- Backups monitoring and alerting:
    - Recovery Time Objective (RTO)
    - Backups heartbeat
    - Backups verification heartbeat
- PCI-DSS compliant:
    - Transfers encryption
    - Encryption at rest
- GDPR compliant:
    - Strictly enforced retention policy

**TwinDB Backup** storage options:

- Amazon S3
- Google Cloud Storage
- Remote SSH server
- Optional local copy


Other features:

- Retention policy defines how many hourly/daily/weekly/monthly/yearly copies to keep
- Separate retention policy for remote and local backup copies
- Email notifications
- Datadog/StatsD integration
- cron configuration comes with a package


How do I get set up?
--------------------

**TwinDB Backup** can be installed from a DEB/RPM package.

The packages are available in the `Releases <https://github.com/twindb/backup/releases>`_.


Installing TwinDB Backup on Ubuntu
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install appropriate Percona XtraBackup version (2.4 for MySQL 5.6, 5.7 or 8.0 for MySQL 8.0).

.. code-block:: console

    # # Download the package
    # wget https://downloads.percona.com/downloads/Percona-XtraBackup-2.4/Percona-XtraBackup-2.4.26/binary/debian/bionic/x86_64/percona-xtrabackup-24_2.4.26-1.bionic_amd64.deb
    # # Install XtraBackup
    # apt install ./percona-xtrabackup-24_2.4.26-1.bionic_amd64.deb

Install TwinDB Backup.

.. code-block:: console

    # # Download the package
    # wget https://twindb-release.s3.amazonaws.com/twindb-backup/2.20.2/bionic/twindb-backup_2.20.2-1_amd64.deb
    # # Install TwinDB Backup
    # apt install ./twindb-backup_2.20.2-1_amd64.deb

Installing TwinDB Backup on CetOS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install appropriate Percona XtraBackup version (2.4 for MySQL 5.6, 5.7 or 8.0 for MySQL 8.0).

.. code-block:: console

    # yum install https://downloads.percona.com/downloads/Percona-XtraBackup-2.4/Percona-XtraBackup-2.4.26/binary/redhat/7/x86_64/percona-xtrabackup-24-2.4.26-1.el7.x86_64.rpm

Install TwinDB Backup.

.. code-block:: console

    # yum install https://twindb-release.s3.amazonaws.com/twindb-backup/2.20.2/7/twindb-backup-2.20.2-1.x86_64.rpm

Configuring TwinDB Backup
~~~~~~~~~~~~~~~~~~~~~~~~~

TwinDB Backup is configured in ``/etc/twindb/twindb-backup.cfg``. See :ref:`usage` for details.

How to build TwinDB Backup manually
-----------------------------------

The TwinDB Backup package can build on a machine with Docker service.
``make package`` will build the package for the operating system defined in the ``OS_VERSION`` environment variable.
Possible ``OS_VERSION`` values:

 * 7
 * bionic
 * focal.

.. code-block:: console

    # export OS_VERSION=bionic
    # make package

The package file will be generated in ``omnibus/pkg/``:

.. code-block:: console

    $ ls omnibus/pkg/*.deb
    omnibus/pkg/twindb-backup_2.20.0-1_amd64.deb

Once the package is built you can install it with rpm/dpkg or upload it to your repository
and install it with apt or yum.

Configuration
-------------
Configuration is stored in ``/etc/twindb/twindb-backup.cfg``.
See https://twindb-backup.readthedocs.io/ for more details.

.. include:: ../AUTHORS.rst

Credits
-------

- This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

- TwinDB Backup uses `Percona Xtrabackup`_ for MySQL backups.
- Contributors (in alphabetical order):

  * `Andrew Ernst <https://github.com/ernstae>`_
  * `Arda Beyazoğlu <https://github.com/ardabeyazoglu>`_
  * `Egor Lyutov <https://github.com/el4v>`_
  * `fonthead <https://github.com/fonthead>`_
  * `Maksym Kryva <https://github.com/mkryva>`_
  * `Manjot Singh <https://github.com/ManjotS>`_
  * `Michael Rikmas <https://github.com/catyellow>`_
  * `Ovais Tariq <https://github.com/ovaistariq>`_
  * `Pim Widdershoven <https://github.com/piwi91>`_

TwinDB Backup uses `Percona Xtrabackup`_ for MySQL backups.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _`Percona Xtrabackup`: https://www.percona.com/software/mysql-database/percona-xtrabackup
