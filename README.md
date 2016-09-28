# README #

**twindb-backup** is a shell script that backups local files and sends them to a remote server via ssh. It can also backup MySQL with mysqldump.


### How do I get set up? ###

# Summary of set up

**twindb-backup** is distributed via the YUM repository. See http://repo.twindb.com/ for instructions on how to set up the repository. Once the repo is set up install the twindb-backup package:


```
# yum install twindb-backup
```

 
twindb-backup is not included in the APT repo as of time of writing this README. However you can build a deb package. Pull the source code and build the package:


```
# make deb
```


Then install it:


```
# dpkg -i twindb-backup-1.0.4-1_noarch.deb
```


# Configuration

The script is configured in `/etc/twindb/twindb-backup.cfg`.


```
# Source
backup_dirs="/etc /root /home"
backup_mysql=TRUE

# Destination
# Uncomment one
# backup_destination="s3"
backup_destination="ssh"

# S3 destination settings
#AWS_ACCESS_KEY_ID="XXXXX"
#AWS_SECRET_ACCESS_KEY="YYYYY"
#AWS_DEFAULT_REGION="us-east-1"
#BUCKET="twindb-backups"

# SSH destination settings
backup_host=127.0.0.1
ssh_user="root"
backup_dir=/path/to/twindb-server-backups

# MySQL
mysql_defaults_file=/etc/twindb/my.cnf

# Retention
hourly_copies=24
daily_copies=7
weekly_copies=4
monthly_copies=12
yearly_copies=3

# Run intervals
run_hourly="YES"
run_daily="YES"
run_weekly="YES"
run_monthly="YES"
run_yearly="YES"
```


The script backups directories listed in `backup_dirs`. If `backup_mysql` is `TRUE` it will take mysqldump, too.

The backups destination is configured in variables `backup_host`, `ssh_user`, and `backup_dir`. User `root` must be able to connect to `backup_host` as `ssh_user` without password. [Setup key based authentication](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Deployment_Guide/s2-ssh-configuration-keypairs.html) to make it work.

To connect to the local MySQL instance the script uses options file `mysql_defaults_file`. You may want to specify MySQL user and password here.

Retention policy is defined in variables `hourly_copies`, `daily_copies`, `weekly_copies`, `monthly_copies`, and `yearly_copies`.

By default the script runs hourly, daily, run_weekly, monthly and yearly. If you want to skip either cycle set respective `run_*` variable to NO e.g. `run_hourly="NO"`.

### Who do I talk to? ###

**twindb-backup** is maintained by TwinDB development team. Contact us via https://twindb.com or by [e-mail](mailto:dev@twindb.com).