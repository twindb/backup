# -*- coding: utf-8 -*-
"""
Module that restores backup copies.
"""
from __future__ import print_function

import sys
from subprocess import Popen, PIPE
import os
from os import path as osp
import tempfile
import errno
import time
import psutil

from twindb_backup import (
    LOG,
    XBSTREAM_BINARY,
    XTRABACKUP_BINARY,
    DEFAULT_FILE_ENCODING,
)
from twindb_backup.destination.exceptions import DestinationError
from twindb_backup.destination.local import Local
from twindb_backup.exceptions import TwinDBBackupError
from twindb_backup.export import export_info
from twindb_backup.exporter.base_exporter import (
    ExportCategory,
    ExportMeasureType,
)
from twindb_backup.modifiers.gpg import Gpg
from twindb_backup.status.mysql_status import MySQLStatus
from twindb_backup.util import mkdir_p, empty_dir


def get_my_cnf(status, key):
    """
    Get MySQL config from the status.

    :param status: Backup status.
    :type status: MySQLStatus
    :param key: Backup name.
    :type key: str
    :return: Content of my.cnf or None if not found
    :rtype: str
    """
    for path in status[key].config:
        if isinstance(status[key].config[path], str):
            yield path, status[key].config[path]
        elif isinstance(status[key].config[path], bytes):
            yield path, status[key].config[path].decode("utf-8")
        else:
            raise TwinDBBackupError(
                f"Unexpected type of {status[key].config[path]}"
            )


def restore_from_mysql_full(
    stream,
    dst_dir,
    config,
    redo_only=False,
    xtrabackup_binary=XTRABACKUP_BINARY,
    xbstream_binary=XBSTREAM_BINARY,
):
    """
    Restore MySQL datadir from a backup copy

    :param stream: Generator that provides backup copy
    :param dst_dir: Path to destination directory. Must exist and be empty.
    :type dst_dir: str
    :param config: Tool configuration.
    :type config: TwinDBBackupConfig
    :param redo_only: True if the function has to do final apply of
        the redo log. For example, if you restore backup from a full copy
        it should be False. If you restore from incremental copy and
        you restore base full copy redo_only should be True.
    :type redo_only: bool
    :param xtrabackup_binary: path to xtrabackup binary.
    :param xbstream_binary: Path to xbstream binary
    :return: If success, return True
    :rtype: bool
    """
    # GPG modifier
    if config.gpg:
        gpg = Gpg(
            stream,
            config.gpg.recipient,
            config.gpg.keyring,
            secret_keyring=config.gpg.secret_keyring,
        )
        LOG.debug("Decrypting stream")
        stream = gpg.revert_stream()
    else:
        LOG.debug("Not decrypting the stream")

    if config.mysql.xtrabackup_binary:
        xtrabackup_binary = config.mysql.xtrabackup_binary

    stream = config.compression.get_modifier(stream).revert_stream()

    with stream as handler:
        if not _extract_xbstream(handler, dst_dir, xbstream_binary):
            return False

    mem_usage = psutil.virtual_memory()
    try:
        xtrabackup_cmd = [
            xtrabackup_binary,
            f"--use-memory={mem_usage.available / 2}",
            "--prepare",
        ]
        if redo_only:
            xtrabackup_cmd += ["--apply-log-only"]

        xtrabackup_cmd += ["--target-dir", dst_dir]

        LOG.debug("Running %s", " ".join(xtrabackup_cmd))
        with Popen(xtrabackup_cmd, stdout=None, stderr=None) as xtrabackup_proc:
            xtrabackup_proc.communicate()
            ret = xtrabackup_proc.returncode
            if ret:
                LOG.error(
                    "%s exited with code %d", " ".join(xtrabackup_cmd), ret
                )
            return ret == 0
    except OSError as err:
        raise TwinDBBackupError(
            f"Failed to prepare backup in {dst_dir}: {err}"
        ) from err


def _extract_xbstream(
    input_stream, working_dir, xbstream_binary=XBSTREAM_BINARY
):
    """
    Extract xbstream stream in directory

    :param input_stream: The stream in xbstream format
    :param working_dir: directory
    :param xbstream_binary: Path to xbstream
    :return: True if extracted successfully
    """
    try:
        cmd = [xbstream_binary, "-x"]
        LOG.debug("Running %s", " ".join(cmd))
        LOG.debug("Working directory: %s", working_dir)
        LOG.debug("Xbstream binary: %s", xbstream_binary)
        with Popen(
            cmd, stdin=input_stream, stdout=PIPE, stderr=PIPE, cwd=working_dir
        ) as proc:
            cout, cerr = proc.communicate()
            ret = proc.returncode
            if ret:
                LOG.error("%s exited with code %d", " ".join(cmd), ret)
                if cout:
                    LOG.error("STDOUT: %s", cout)
                if cerr:
                    LOG.error("STDERR: %s", cerr)
            return ret == 0

    except OSError as err:
        raise TwinDBBackupError(f"Failed to extract xbstream: {err}") from err


# pylint: disable=too-many-locals,too-many-branches,too-many-statements
def restore_from_mysql_incremental(
    stream,
    dst_dir,
    config,
    tmp_dir=None,
    xtrabackup_binary=XTRABACKUP_BINARY,
    xbstream_binary=XBSTREAM_BINARY,
):
    """
    Restore MySQL datadir from an incremental copy.

    :param stream: Generator that provides backup copy
    :param dst_dir: Path to destination directory. Must exist and be empty.
    :type dst_dir: str
    :param config: Tool configuration.
    :type config: TwinDBBackupConfig
    :param tmp_dir: Path to temp dir
    :type tmp_dir: str
    :param xtrabackup_binary: Path to xtrabackup binary.
    :param xbstream_binary: Path to xbstream binary
    :return: If success, return True
    :rtype: bool
    """
    if tmp_dir is None:
        try:
            inc_dir = tempfile.mkdtemp()
        finally:
            empty_dir(dst_dir)
    else:
        inc_dir = tmp_dir
    # GPG modifier
    if config.gpg:
        gpg = Gpg(
            stream,
            config.gpg.recipient,
            config.gpg.keyring,
            secret_keyring=config.gpg.secret_keyring,
        )
        LOG.debug("Decrypting stream")
        stream = gpg.revert_stream()
    else:
        LOG.debug("Not decrypting the stream")

    if config.mysql.xtrabackup_binary:
        xtrabackup_binary = config.mysql.xtrabackup_binary

    stream = config.compression.get_modifier(stream).revert_stream()

    with stream as handler:
        if not _extract_xbstream(handler, inc_dir, xbstream_binary):
            return False

    try:
        mem_usage = psutil.virtual_memory()
        try:
            xtrabackup_cmd = [
                xtrabackup_binary,
                f"--use-memory={mem_usage.available / 2}",
                "--prepare",
                "--apply-log-only",
                f"--target-dir={dst_dir}",
            ]
            LOG.debug("Running %s", " ".join(xtrabackup_cmd))
            with Popen(
                xtrabackup_cmd, stdout=None, stderr=None
            ) as xtrabackup_proc:
                xtrabackup_proc.communicate()
                ret = xtrabackup_proc.returncode
                if ret:
                    LOG.error(
                        "%s exited with code %d", " ".join(xtrabackup_cmd), ret
                    )
                    return False

            xtrabackup_cmd = [
                xtrabackup_binary,
                f"--use-memory={mem_usage.available / 2}",
                "--prepare",
                f"--target-dir={dst_dir}",
                f"--incremental-dir={inc_dir}",
            ]
            LOG.debug("Running %s", " ".join(xtrabackup_cmd))
            with Popen(
                xtrabackup_cmd, stdout=None, stderr=None
            ) as xtrabackup_proc:
                xtrabackup_proc.communicate()
                ret = xtrabackup_proc.returncode
                if ret:
                    LOG.error(
                        "%s exited with code %d", " ".join(xtrabackup_cmd), ret
                    )
                return ret == 0
        except OSError as err:
            LOG.error("Failed to prepare backup in %s: %s", dst_dir, err)
            return False
    finally:
        try:
            pass
        except OSError as exc:
            if exc.errno != errno.ENOENT:  # ENOENT - no such file or directory
                raise  # re-raise exception


def gen_grastate(path, version, uuid, seqno):
    """
    Generate and save grastate file.

    :param path: Path to grastate file.
    :param version: Galera version from grastate.dat.
    :param uuid: UUID from grastate.dat.
    :param seqno: seqno from grastate.dat.
    """
    with open(path, "w", encoding=DEFAULT_FILE_ENCODING) as file_desc:
        file_desc.write(
            f"""# GALERA saved state
version: {version}
uuid:    {uuid}
seqno:   {seqno}
cert_index:
"""
        )


def update_grastate(dst_dir, status, key):
    """
    If xtrabackup_galera_info exists in the destination directory
    then parse it and generate grastate.dat file.

    :param dst_dir: Path to destination directory.
    :type dst_dir: str
    :param status: Backup status
    :type status: dict
    :param key: Backup name
    :type key: str
    """
    if osp.exists(osp.join(dst_dir, "xtrabackup_galera_info")):
        version = status[key].wsrep_provider_version

        with open(
            osp.join(dst_dir, "xtrabackup_galera_info"),
            encoding=DEFAULT_FILE_ENCODING,
        ) as galera_info:
            galera_info = galera_info.read()
            uuid = galera_info.split(":")[0]
            seqno = galera_info.split(":")[1]

        gen_grastate(dst_dir + "/grastate.dat", version, uuid, seqno)


# pylint: disable=too-many-locals,too-many-branches,too-many-statements
def restore_from_mysql(
    twindb_config, copy, dst_dir, tmp_dir=None, cache=None, hostname=None
):
    """
    Restore MySQL datadir in a given directory

    :param twindb_config: tool configuration
    :type twindb_config: TwinDBBackupConfig
    :param copy: Backup copy instance.
    :type copy: MySQLCopy
    :param dst_dir: Destination directory. Must exist and be empty.
    :type dst_dir: str
    :param tmp_dir: Path to temp directory
    :type tmp_dir: str
    :param cache: Local cache object.
    :type cache: Cache
    :param hostname: Hostname
    :type hostname: str

    """
    LOG.info("Restoring %s in %s", copy, dst_dir)
    mkdir_p(dst_dir)

    dst = None
    restore_start = time.time()
    keep_local_path = twindb_config.keep_local_path
    if keep_local_path and osp.exists(osp.join(keep_local_path, copy.key)):
        dst = Local(twindb_config.keep_local_path)

    if not dst:
        if not hostname:
            hostname = copy.host
            if not hostname:
                raise DestinationError(f"Failed to get hostname from {copy}")
        dst = twindb_config.destination(backup_source=hostname)

    key = copy.key
    status = MySQLStatus(dst=dst, status_directory=hostname)

    stream = dst.get_stream(copy)

    if status[key].type == "full":

        cache_key = os.path.basename(key)
        if cache:
            if cache_key in cache:
                # restore from cache
                cache.restore_in(cache_key, dst_dir)
            else:
                restore_from_mysql_full(
                    stream, dst_dir, twindb_config, redo_only=False
                )
                cache.add(dst_dir, cache_key)
        else:
            restore_from_mysql_full(
                stream, dst_dir, twindb_config, redo_only=False
            )

    else:
        full_copy = status.candidate_parent(copy.run_type)
        full_stream = dst.get_stream(full_copy)
        LOG.debug("Full parent copy is %s", full_copy.key)
        cache_key = os.path.basename(full_copy.key)

        if cache:
            if cache_key in cache:
                # restore from cache
                cache.restore_in(cache_key, dst_dir)
            else:
                restore_from_mysql_full(
                    full_stream, dst_dir, twindb_config, redo_only=True
                )
                cache.add(dst_dir, cache_key)
        else:
            restore_from_mysql_full(
                full_stream, dst_dir, twindb_config, redo_only=True
            )

        restore_from_mysql_incremental(stream, dst_dir, twindb_config, tmp_dir)

    config_dir = os.path.join(dst_dir, "_config")

    for path, content in get_my_cnf(status, key):
        config_sub_dir = os.path.join(
            config_dir, os.path.dirname(path).lstrip("/")
        )
        mkdir_p(config_sub_dir, mode=0o755)

        with open(
            os.path.join(config_sub_dir, os.path.basename(path)),
            "w",
            encoding=DEFAULT_FILE_ENCODING,
        ) as mysql_config:
            mysql_config.write(content)

    update_grastate(dst_dir, status, key)
    export_info(
        twindb_config,
        data=time.time() - restore_start,
        category=ExportCategory.mysql,
        measure_type=ExportMeasureType.restore,
    )
    LOG.info("Successfully restored %s in %s.", copy.key, dst_dir)
    LOG.info(
        "Now copy content of %s to MySQL datadir: " "cp -R %s /var/lib/mysql/",
        dst_dir,
        osp.join(dst_dir, "*"),
    )
    LOG.info("Fix permissions: chown -R mysql:mysql /var/lib/mysql/")
    LOG.info(
        "Make sure innodb_log_file_size and innodb_log_files_in_group "
        "in %s/backup-my.cnf and in /etc/my.cnf are same.",
        dst_dir,
    )

    if osp.exists(config_dir):
        LOG.info("Original my.cnf is restored in %s.", config_dir)

    LOG.info("Then you can start MySQL normally.")


def restore_from_file(twindb_config, copy, dst_dir):
    """
    Restore a directory from a backup copy in the directory

    :param twindb_config: tool configuration
    :type twindb_config: TwinDBBackupConfig
    :param copy: Instance of BaseCopy or and inheriting classes.
    :type copy: BaseCopy
    :param dst_dir: Path to destination directory. Must exist and be empty.
    :type dst_dir: str
    """
    LOG.info("Restoring %s in %s", copy.key, dst_dir)
    mkdir_p(dst_dir)
    restore_start = time.time()
    keep_local_path = twindb_config.keep_local_path

    if keep_local_path and os.path.exists(osp.join(keep_local_path, copy.key)):
        dst = Local(osp.join(keep_local_path, copy.key))
        stream = dst.get_stream(copy)
    else:
        dst = twindb_config.destination()
        stream = dst.get_stream(copy)

        # GPG modifier
        if twindb_config.gpg:
            gpg = Gpg(
                stream,
                twindb_config.gpg.recipient,
                twindb_config.gpg.keyring,
                secret_keyring=twindb_config.gpg.secret_keyring,
            )
            LOG.debug("Decrypting stream")
            stream = gpg.revert_stream()
        else:
            LOG.debug("Not decrypting the stream")

    with stream as handler:
        try:
            LOG.debug("handler type: %s", type(handler))
            LOG.debug("stream type: %s", type(stream))
            cmd = ["tar", "zvxf", "-"]
            LOG.debug("Running %s", " ".join(cmd))
            with Popen(cmd, stdin=handler, cwd=dst_dir) as proc:
                cout, cerr = proc.communicate()
                ret = proc.returncode
                if ret:
                    LOG.error("%s exited with code %d", cmd, ret)
                    if cout:
                        LOG.error("STDOUT: %s", cout)
                    if cerr:
                        LOG.error("STDERR: %s", cerr)
                    return
            LOG.info("Successfully restored %s in %s", copy.key, dst_dir)
        except (OSError, DestinationError) as err:
            LOG.error("Failed to decompress %s: %s", copy.key, err)
            sys.exit(1)

    export_info(
        twindb_config,
        data=time.time() - restore_start,
        category=ExportCategory.files,
        measure_type=ExportMeasureType.restore,
    )
