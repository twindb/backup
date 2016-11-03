=====
Usage
=====

Configuration
-------------

Once the ``twindb-backup`` package is installed you need to edit configuration file ``/etc/twindb/twindb-backup.cfg``.

Let's review each configuration section.

Source
~~~~~~

``[source]`` section defines what to backup.

twindb-backup supports backing up local directories and MySQL database.


In ``backup_dirs`` you specify which directories to backup. Each directory is separated by a white space.


``backup_mysql`` tells whether the tool backups MySQL or not.

::

    [source]
    backup_dirs=/etc /root /home
    backup_mysql=no

Destination
~~~~~~~~~~~

``[destination]`` specifies where to store backup copies.

``backup_destination`` can be either ``ssh`` (if you want to store backups on a remote SSh server)
or ``s3`` (if you want to store backups in Amazon S3).

In the optional ``keep_local_path`` you can specify a local path where the tool will store a local copy of the backup.
It's useful if you want to stream a MySQL backup to S3 and would like to keep a local copy as well.

::

    [destination]
    backup_destination=ssh
    keep_local_path=/var/backup/local

Amazon S3
~~~~~~~~~

In ``[s3]`` section you specify Amazon credentials as well as an S3 bucked where to store backups.

::

    [s3]

    # S3 destination settings

    AWS_ACCESS_KEY_ID=XXXXX
    AWS_SECRET_ACCESS_KEY=YYYYY
    AWS_DEFAULT_REGION=us-east-1
    BUCKET=twindb-backups

SSH Settings
~~~~~~~~~~~~

If your backup destination is an SSH server, you specify ssh parameters in ``[ssh]`` section.
It is assumed you configured `SSH keys authentication`_. It will not work if you need to enter a password to login to ``backup_host``.

::

    [ssh]

    backup_host=127.0.0.1
    ssh_user=root
    backup_dir=/tmp/backup


MySQL
~~~~~

XtraBackup needs to connect to MySQL. In ``[mysql]`` section you specify a defaults file with user and password.

::

    [mysql]
    mysql_defaults_file=/etc/twindb/my.cnf


Retention Policy
~~~~~~~~~~~~~~~~

In ``[retention]`` section you specify how many copies you want to keep on the remote storage (s3 or ssh).

::

    [retention]
    hourly_copies=24
    daily_copies=7
    weekly_copies=4
    monthly_copies=12
    yearly_copies=3


Local Retention Policy
~~~~~~~~~~~~~~~~~~~~~~

if ``keep_local_path`` is defined in Destination_ the tool will apply ``[retention_local]`` on the local copies.

::

    [retention_local]
    hourly_copies=1
    daily_copies=1
    weekly_copies=0
    monthly_copies=0
    yearly_copies=0

Running Intervals
~~~~~~~~~~~~~~~~~

By default **twindb-backup** will run `hourly`, `daily`, `weekly`, `monthly` and `yearly`.
If you would like to skip some runs ``[intervals]`` section is the right place to do so.

::

    [intervals]
    run_hourly=yes
    run_daily=yes
    run_weekly=yes
    run_monthly=yes
    run_yearly=yes


Email notification
------------------
The RPM package installs a cron job. If a backup job fails it will send standard error output to the specified email.
The email address is specified in the cron configuration file ``/etc/cron.d/twindb-backup``.

::

    MAILTO=nagios@twindb.com
    @hourly  root twindb-backup backup hourly
    @daily   root twindb-backup backup daily
    @weekly  root twindb-backup backup weekly
    @monthly root twindb-backup backup monthly
    @yearly  root twindb-backup backup yearly


.. _SSH keys authentication: https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Deployment_Guide/s2-ssh-configuration-keypairs.html
