=============
TwinDB Backup
=============


.. image:: https://img.shields.io/travis/twindb/backup.svg
    :target: https://travis-ci.com/twindb/backup

.. image:: https://img.shields.io/codecov/c/github/twindb/backup.svg
    :target: https://codecov.io/gh/twindb/backup

.. image:: https://readthedocs.org/projects/twindb-backup/badge/?version=master
    :target: https://twindb-backup.readthedocs.io/en/master/?badge=master
    :alt: Documentation Status

.. image:: https://img.shields.io/gitter/room/twindb/twindb-backup.svg
    :target: https://gitter.im/twindb/backup
    :alt: Join the chat at https://gitter.im/twindb/backup

TwinDB Backup is a multipurpose tool for backing up MySQL and file system.
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

After the stream passed all modifiers it is sent to one of configured
backup destination. It can be:

- Amazon S3 bucket
- Google Cloud Storage bucket
- Any server with SSH demon

.. figure:: https://user-images.githubusercontent.com/1763754/56677794-20901b80-6676-11e9-8f71-8de0b0b6f066.png
    :width: 400px
    :align: center
    :height: 300px
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
- Datadog integration
- cron configuration comes with a package


How do I get set up?
--------------------

**TwinDB Backup** is distributed via package repositories.

See installation instruction on https://packagecloud.io/twindb/main/install.
Once the repository for your operating system is configured, install the ``twindb-backup`` package.

**On CentOS and RedHat**

.. code-block:: console

    # curl -s https://packagecloud.io/install/repositories/twindb/main/script.rpm.sh | sudo bash
    # yum install twindb-backup

**On Debian and Ubuntu**

.. code-block:: console

    # curl -s https://packagecloud.io/install/repositories/twindb/main/script.deb.sh | sudo bash
    # apt-get install twindb-backup


Configuration
-------------
Configuration is stored in ``/etc/twindb/twindb-backup.cfg``.
See https://twindb-backup.readthedocs.io/ for more details.

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

TwinDB Backup uses `Percona Xtrabackup`_ for MySQL backups.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _`Percona Xtrabackup`: https://www.percona.com/software/mysql-database/percona-xtrabackup
