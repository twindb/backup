#!/usr/bin/env bash
set -eu

CONFIG=/etc/twindb/twindb-backup.cfg
if test -f "${CONFIG}"
then
    source ${CONFIG}
fi

# Get time when script started
start_time=`date +%s`

scriptname=$(basename $0)
pidfile="/var/run/${scriptname}.pid"
ssh_opt="-o StrictHostKeyChecking=no"

function get_ssh_key_path() {
    case ${OSTYPE} in
        "darwin14")
            echo "/var/root/.ssh/id_rsa"
            ;;
        *)
            echo "/root/.ssh/id_rsa"
    esac
}

SSH="ssh $ssh_opt -l ${ssh_user} -i `get_ssh_key_path`"
MYSQLDUMP="mysqldump"
MYSQLDUMP_ARGS="--defaults-file=${mysql_defaults_file} -A"

trap "rm -f ${pidfile}" INT
trap "rm -f ${pidfile}" ERR
trap "rm -f ${pidfile}" EXIT

function vlog() {
    echo "`date`: $1"
}

function check_ssh() {
    local storage_timeout=3600
    while true
    do
        r=`${SSH} ${backup_host} echo 123 2>/dev/null`
        now=`date +%s`
        if [ $(( $now - $start_time )) -gt ${storage_timeout} ]
        then
            echo "Backup script could not reach ${backup_host} after ${storage_timeout} seconds"
            exit -1
        fi
        if [ "$r" == "123" ]
        then
            return
        else
            wait_time=$(( $RANDOM * $max_delay / 32767))
            sleep ${wait_time}
        fi
    done
}

function backup_mysql() {
    if [ "${backup_mysql}" = TRUE ]
    then
        mysql_backup_file=$1
        mysql_backup_dir="`dirname ${mysql_backup_file}`"
        ${SSH} ${backup_host} mkdir -p "${mysql_backup_dir}"
        ${MYSQLDUMP} ${MYSQLDUMP_ARGS} | gzip -c - | ${SSH} ${backup_host} "cat - > $mysql_backup_file"
    fi
}

function backup_files() {
    for d in ${backup_dirs}
    do
        desc=`echo ${d} | sed 's/\///'`
        desc=`echo ${desc} | sed 's/\//_/g'`
        f="$desc-`date +%F_%H-%M-%S`.tar.gz"
        ${SSH} ${backup_host} mkdir -p "$1"
        case ${OSTYPE} in
            "darwin14")
                TAR_ARGS="zcLf -"
                ;;
            *)
                TAR_ARGS="zchf -"
        esac

        tar ${TAR_ARGS} "$d" 2>/dev/null | ${SSH} ${backup_host} "cat - > $1/$f"
    done
}

function cleanup_old_backups() {

    case ${suffix} in
        "hourly")
            mtime=$(( ${hourly_copies} * 1 * 60 ))
            ;;
        "daily")
            mtime=$(( ${daily_copies} * 24 * 60 ))
            ;;
        "weekly")
            mtime=$(( ${weekly_copies} * 7 * 24 * 60 ))
            ;;
        "monthly")
            mtime=$(( ${monthly_copies} * 30 * 24 * 60 ))
            ;;
        "yearly")
            mtime=$(( ${yearly_copies} * 365 * 24 * 60 ))
            ;;
        *)
            vlog "Warning: Unknown backup period $suffix"
            exit -1
    esac
    ${SSH} ${backup_host} "find $1 -mmin +$mtime -type f -delete"
}

if  (! test -z "$1") && (
    [ "$1" = "hourly" ]  || \
    [ "$1" = "daily" ]   || \
    [ "$1" = "weekly" ]  || \
    [ "$1" = "monthly" ] || \
    [ "$1" = "yearly" ] )
then
        suffix="$1"
else
    echo "Usage:"
    echo "    `basename $0` <hourly | daily | weekly | monthly | yearly>"
    exit -1
fi
mysql_backup_dir="${backup_dir}/`hostname`/${suffix}/mysql"
mysql_backup_file="${mysql_backup_dir}/mysql-`date +%F_%H-%M-%S`.sql.gz"
files_backup_dir="${backup_dir}/`hostname`/$suffix"

# Do not run backups if run_* is set and it is NO
if [ "$suffix" == "hourly" ] && ! [ -z ${run_hourly+x} ] && [ "${run_hourly}" == "NO" ]
then
    exit
fi
if [ "$suffix" == "daily" ] && ! [ -z ${run_daily+x} ] && [ "${run_daily}" == "NO" ]
then
    exit
fi
if [ "$suffix" == "weekly" ] && ! [ -z ${run_weekly+x} ] && [ "${run_weekly}" == "NO" ]
then
    exit
fi
if [ "$suffix" == "monthly" ] && ! [ -z ${run_monthly+x} ] && [ "${run_monthly}" == "NO" ]
then
    exit
fi
if [ "$suffix" == "yearly" ] && ! [ -z ${run_yearly+x} ] && [ "${run_yearly}" == "NO" ]
then
    exit
fi

# Acquire a lock
# Wait for the lock not more than $lock_wait_time seconds
lock_wait_time=1800
exec 8>${pidfile}
if test -x flock
then
    flock -w ${lock_wait_time} 8 || (echo "Could not acquire lock after $lock_wait_time seconds"; exit -1)
fi
pid=$$
echo ${pid} 1>&8

# wait random time between 0 and 600 seconds to avoid storm starts
max_delay=600
let wait_time=$RANDOM*$max_delay/32767
sleep ${wait_time}
check_ssh
cleanup_old_backups "${files_backup_dir}"
#try create
${SSH} ${backup_host} true || (vlog "Can not ssh to ${backup_host}"; exit -1)
${SSH} ${backup_host} mkdir -p "${backup_dir}/`hostname`"
backup_mysql "${mysql_backup_file}"
backup_files "${files_backup_dir}"
