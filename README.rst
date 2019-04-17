=============
TwinDB Backup
=============


.. image:: https://img.shields.io/travis/twindb/backup.svg
    :target: https://travis-ci.org/twindb/backup

.. image:: https://img.shields.io/codecov/c/github/twindb/backup.svg
    :target: https://codecov.io/gh/twindb/backup

.. image:: https://readthedocs.org/projects/twindb-backup/badge/?version=master
    :target: https://twindb-backup.readthedocs.io/en/master/?badge=master
    :alt: Documentation Status


TwinDB Backup is a multipurpose tool for backing up MySQL and file system.
It can store backup copies on a remote SSH server, Amazon S3 or Google Cloud Storage.


The tool can easily restore the backup copies.

Read full documentation on https://twindb-backup.readthedocs.io.


Features
--------

**twindb-backup** key features:

- Files/directories backups
- MySQL backups
- Incremental MySQL backups
- Encrypting backup copies

**twindb-backup** store backups on:

- Remote SSH server
- Amazon S3
- Google Cloud Storage
- Optionally save local copy


Other features:

- Retention policy defines how many hourly/daily/weekly/monthly/yearly copies to keep
- Separate retention policy for remote and local backup copies
- Supports non-impacting Percona XtraDB Cluster backups
- Email notifications
- cron configuration comes with a package


How do I get set up?
~~~~~~~~~~~~~~~~~~~~

**twindb-backup** is distributed via package repositories. See installation instruction on https://packagecloud.io/twindb/main/install.
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
~~~~~~~~~~~~~
Configuration is stored in ``/etc/twindb/twindb-backup.cfg``.
See http://twindb-backup.readthedocs.io/en/master/usage.html for more details.

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _instructions: https://twindb.com/twindb-software-repository/
.. _wiki page: https://github.com/twindb/backup/wiki
