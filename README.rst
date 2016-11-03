=============
TwinDB Backup
=============


.. image:: https://img.shields.io/travis/twindb/backup.svg
    :target: https://travis-ci.org/twindb/backup

.. image:: https://readthedocs.org/projects/twindb-backup/badge/?version=master
    :target: https://twindb-backup.readthedocs.io/en/master/?badge=master
    :alt: Documentation Status


TwinDB Backup tool for files, MySQL et al.


* Free software: Apache Software License 2.0
* Documentation: https://twindb-backup.readthedocs.io.


Features
--------

**twindb-backup** takes backups of:

- Files and directories
- MySQL with XtraBackup

**twindb-backup** store backups on:

- Remote SSH server
- Amazon sS3
- Optionally save local copy

Other features:

- Retention policy defines how many hourly/daily/weekly/monthly/yearly copies to keep
- Separate retention policy for remote and local backup copies
- Enables/disables ``wsrep_desync`` for Percona Cluster backups
- Email notifications
- cron configuration comes with a package


How do I get set up?
~~~~~~~~~~~~~~~~~~~~

**twindb-backup** is distributed via the YUM repository.
Check instructions_ on how to set up the repository.
Once the repo is configured install the ``twindb-backup`` package.

.. code-block:: console

    # yum install twindb-backup



Configuration
~~~~~~~~~~~~~
Configuration is stored in ``/etc/twindb/twindb-backup.cfg``. Configuration options are self-explanatory.
We will describe specifics in the `wiki page`_ if any questions arrive.

The rpm package installs a cron job, so no additional configuration is required.


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _instructions: https://twindb.com/twindb-software-repository/
.. _wiki page: https://github.com/twindb/backup/wiki
