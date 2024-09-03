.. _usage:

=====
Usage
=====

Once the ``twindb-backup`` package is installed you need to edit configuration file ``/etc/twindb/twindb-backup.cfg``.
The configuration defines what has to be backed up, where to and all other options.

Let's review each configuration section.

Backup Source
~~~~~~~~~~~~~

``[source]`` section defines what to backup.

**TwinDB Backup** supports backing up MySQL database and local directories .


In ``backup_dirs`` you specify which directories to backup. Each directory is separated by a white space.
If the directory contains spaces it must be quoted..


``backup_mysql`` tells whether the tool should backup MySQL or not.

.. code-block:: ini

    [source]

    backup_dirs = /etc /root /home "/path/to/important files"
    backup_mysql = yes

When you back up files, ``tar_options`` might be useful. It's a string that is passed to the ``tar`` command.
Personally, I added it to skip files ``.gitignore`` would ignore.

.. code-block:: ini

    [source]

    backup_dirs = /etc /root /home "/path/to/important files"
    tar_options = --exclude-vcs-ignores


Backup Destination
~~~~~~~~~~~~~~~~~~

The ``[destination]`` section specifies where to store backup copies.

``backup_destination`` can be either ``ssh`` (if you want to store backups on a remote SSH server),
``s3`` (if you want to store backups in Amazon S3), ``az`` (if the backup should be stored in Azure Blob Storage), or ``gcs`` (if the backup should be stored in Google Cloud).

In the optional ``keep_local_path`` you can specify a local path where the tool will store a local copy of the backup.
It's useful if you want to stream a MySQL backup to S3 and would like to keep a local copy as well.

.. code-block:: ini

    [destination]

    backup_destination = ssh
    keep_local_path = /var/backup/local

Compression
-----------

In the ``[compression]`` section you can specify compression method such as gzip, pigz, bzip2 and lbzip2.
You can use parallel compression by using pigz or lbzip2, and specify number of threads to use in parallel.
Number of threads defaults to number of cores minus one, if not specified.
Level specifies the compression level from 1 to 9. By default the tool uses gzip for compression.

.. code-block:: ini

    [compression]

    program = pigz
    threads = 4
    level = 9

Amazon S3
~~~~~~~~~

In the ``[s3]`` section you specify Amazon credentials as well as an S3 bucket where to store backups.

.. code-block:: ini

    [s3]

    AWS_ACCESS_KEY_ID = XXXXX
    AWS_SECRET_ACCESS_KEY = YYYYY
    AWS_DEFAULT_REGION = us-east-1
    BUCKET = twindb-backups

Azure Blob Storage
~~~~~~~~~~~~~~~~~~~~

In the ``[az]`` section you specify Azure credentials as well as Azure Blob Storage container where to store backups.

.. code-block:: ini

    [az]

    connection_string = "DefaultEndpointsProtocol=https;AccountName=ACCOUNT_NAME;AccountKey=ACCOUNT_KEY;EndpointSuffix=core.windows.net"
    container_name = twindb-backups
    remote_path = /backups/mysql # optional


Google Cloud Storage
~~~~~~~~~~~~~~~~~~~~

In the ``[gcs]`` section you specify Google credentials as well as cloud storage bucket where to store backups.

.. code-block:: ini

    [gcs]

    GC_CREDENTIALS_FILE = XXXXX
    BUCKET = twindb-backups

SSH Settings
~~~~~~~~~~~~

If your backup destination is an SSH server, you specify the ssh parameters in ``[ssh]`` section.
It is assumed you configured `SSH keys authentication`_. It will not work if you need to enter a password to login to ``backup_host``.

.. code-block:: ini

    [ssh]

    backup_host = 127.0.0.1
    backup_dir = /path/to/directory_with_backups
    ssh_user = root
    ssh_key = /root/.ssh/id_rsa
    port = 22


MySQL
~~~~~

XtraBackup needs to connect to MySQL. In the ``[mysql]`` section you specify a defaults file with user and password.

It also tells the tool how often it should take full copies. By default it will take the full copy daily.
if so, the hourly copies will be incremental. If ``full_backup`` is set to ``weekly`` then the tool will take full
backups every week, and daily and hourly copies will be incremental.


The ``expire_log_days`` options specifies the retention period for MySQL binlogs. By default it's seven days.

.. code-block:: ini

    [mysql]

    mysql_defaults_file = /etc/twindb/my.cnf
    full_backup = daily
    expire_log_days = 7

Backing up MySQL Binlog
-----------------------

Every time **TwinDB Backup** runs it also copies MySQL binary log. However you
probably want to copy binlogs more often than the incremental backup runs.
It's not feasible to run incremental backup let's say every five minutes.

To keep `Recovery Point Objective`_ minimal it is recommended take incremental copies every hour
and additionally copy binlogs every five minutes. The cron configuration should look like this:

.. code-block:: console

    */5      root twindb-backup backup --binlogs-only hourly
    @hourly  root twindb-backup backup hourly
    @daily   root twindb-backup backup daily
    @weekly  root twindb-backup backup weekly
    @monthly root twindb-backup backup monthly
    @yearly  root twindb-backup backup yearly


Encryption
~~~~~~~~~~
The tool uses GPG_ for encrypting/decrypting backup copies.
To enable encryption add ``[gpg]`` section to the configuration file.
It's your responsibility to generate and manage the encryption key.

.. code-block:: ini

    [gpg]

    keyring = /root/.gnupg/pubring.gpg
    secret_keyring = /root/.gnupg/secring.gpg
    recipient = backupuser@youdomain.com


Retention Policy
~~~~~~~~~~~~~~~~

In ``[retention]`` section you specify how many copies you want to keep on the remote storage (s3 or ssh).

.. code-block:: ini

    [retention]

    hourly_copies  = 24
    daily_copies   = 7
    weekly_copies  = 4
    monthly_copies = 12
    yearly_copies  = 3


Local Retention Policy
~~~~~~~~~~~~~~~~~~~~~~

if ``keep_local_path`` is defined in `Backup Destination`_ the tool will apply ``[retention_local]`` on the local copies.

.. code-block:: ini

    [retention_local]

    hourly_copies  = 1
    daily_copies   = 1
    weekly_copies  = 0
    monthly_copies = 0
    yearly_copies  = 0

Running Intervals
~~~~~~~~~~~~~~~~~

By default **twindb-backup** will run `hourly`, `daily`, `weekly`, `monthly` and `yearly`.
If you would like to skip some runs ``[intervals]`` section is the right place to do so.

.. code-block:: ini

    [intervals]

    run_hourly  = yes
    run_daily   = yes
    run_weekly  = yes
    run_monthly = yes
    run_yearly  = yes

Monitoring
~~~~~~~~~~

**TwinDB Backup** currently supports two ways to monitor backups. For a simple
case you can use emails notifications. The tool doesn't produce any output if a run
was successful and will log any errors to standard error output.

For comprehensive monitoring **TwinDB Backup** exports backup and restore metrics to Datadog_.

Email notification
------------------

The ``twindb-backup`` package installs a cron job.
If a backup job fails it will send standard error output to an email from the ``$MAILTO`` environment variable.
It can be defined in the cron configuration file ``/etc/cron.d/twindb-backup``.

.. code-block:: console

    MAILTO = alerts@yourdomain.com
    @hourly  root twindb-backup backup hourly
    @daily   root twindb-backup backup daily
    @weekly  root twindb-backup backup weekly
    @monthly root twindb-backup backup monthly
    @yearly  root twindb-backup backup yearly


Datadog integration
-------------------

To configure **TwinDB Backup** with Datadog you need to get ``api_key`` and
``app_key`` from Datadog. Check out https://app.datadoghq.com/account/settings#api for these.

When configured, **TwinDB Backup** will export two metrics to Datadog:

 * twindb.mysql.backup_time
 * twindb.mysql.restore_time

.. figure:: https://user-images.githubusercontent.com/1763754/56821474-426ad900-6803-11e9-8229-0aa47d8c51a4.png
    :width: 400px
    :align: center
    :height: 300px
    :alt: Backup time
    :figclass: align-center

.. figure:: https://user-images.githubusercontent.com/1763754/56821478-44cd3300-6803-11e9-91bf-ba5ab682769e.png
    :width: 400px
    :align: center
    :height: 300px
    :alt: Restore time
    :figclass: align-center


You can use those for building graphs and monitors.
Check out the `Monitoring MySQL Backups With Datadog and TwinDB Backup Tool`_
for more details [#]_.

.. code-block:: ini

    [export]

    transport = datadog
    api_key = 0269463bdd00317688ce40371b0774ab
    app_key = d925774d7ae7ba22538eaf89e659f157f89e659f1


.. _SSH keys authentication: https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Deployment_Guide/s2-ssh-configuration-keypairs.html
.. _GPG: https://www.gnupg.org/
.. _Datadog: https://www.datadoghq.com/
.. _Monitoring MySQL Backups With Datadog and TwinDB Backup Tool: https://twindb.com/monitoring-mysql-backups/
.. _Recovery Point Objective: https://en.wikipedia.org/wiki/Disaster_recovery#Recovery_Point_Objective
.. [#] The keys are fake.
