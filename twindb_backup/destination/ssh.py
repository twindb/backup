# -*- coding: utf-8 -*-
"""
Module for SSH destination.
"""
import socket

import os
from contextlib import contextmanager
from multiprocessing import Process

import time

from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import SshDestinationError
from twindb_backup.source.exceptions import MySQLSourceError
from twindb_backup.ssh.client import SshClient
from twindb_backup.ssh.exceptions import SshClientException
from twindb_backup.status.mysql_status import MySQLStatus


class SshConnectInfo(object):  # pylint: disable=too-few-public-methods
    """Options for SSH connection"""

    def __init__(self, host='127.0.0.1', port=22, key='/root/.id_rsa',
                 user='root'):
        self.host = host
        if isinstance(port, int):
            self.port = port
        else:
            raise ValueError("Port is not integer")
        self.key = key
        self.user = user


class Ssh(BaseDestination):
    """
    SSH destination class

    :param ssh_connect_info: SSH connection info
    :type ssh_connect_info: SshConnectInfo
    :param remote_path: Path to store backup
    :param hostname: Hostname
    """
    def __init__(self, remote_path,
                 ssh_connect_info=SshConnectInfo(),
                 hostname=socket.gethostname()):
        super(Ssh, self).__init__(remote_path)

        self._ssh_client = SshClient(ssh_connect_info)

        self.status_path = "{remote_path}/{hostname}/status".format(
            remote_path=self.remote_path,
            hostname=hostname
        )
        self.status_tmp_path = self.status_path + ".tmp"

    def save(self, handler, name):
        """
        Read from handler and save it on remote ssh server

        :param name: relative path to a file to store the backup copy.
        :param handler: stream with content of the backup.
        """
        remote_name = self.remote_path + '/' + name
        try:
            self._mkdirname_r(remote_name)
        except SshClientException as err:
            LOG.error('Failed to create directory for %s: %s',
                      remote_name, err)
            return False

        try:
            cmd = "cat - > %s" % remote_name
            with self._ssh_client.get_remote_handlers(cmd) \
                    as (cin, _, _):
                with handler as file_obj:
                    while True:
                        chunk = file_obj.read(1024)
                        if chunk:
                            cin.write(chunk)
                        else:
                            break
            return True
        except SshClientException:
            return False
        except MySQLSourceError:
            return False

    def _mkdir_r(self, path):
        """
        Create directory on the remote server

        :param path: remote directory
        :type path: str
        """
        cmd = 'mkdir -p "%s"' % path
        self.execute_command(cmd)

    def list_files(self, prefix, recursive=False):
        """
        Get list of file by prefix

        :param prefix: Path
        :param recursive: Recursive return list of files
        :type prefix: str
        :type recursive: bool
        :return: List of files
        :rtype: list
        """
        return sorted(self._ssh_client.list_files(prefix, recursive))

    def find_files(self, prefix, run_type):
        """
        Find files by prefix

        :param prefix: Path
        :param run_type: Run type for search
        :type prefix: str
        :type run_type: str
        :return: List of files
        :rtype: list
        """
        cmd = "find {prefix}/ -wholename '*/{run_type}/*' -type f".format(
            prefix=prefix,
            run_type=run_type
        )

        cout, _ = self._ssh_client.execute(cmd)
        return sorted(cout.split())

    def delete(self, obj):
        """
        Delete file by path

        :param obj: path to a remote file.
        """
        cmd = "rm %s" % obj
        self.execute_command(cmd)

    @contextmanager
    def get_stream(self, path):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination

        :param path: Path to file
        :type path: str
        :return: Standard output.
        """
        cmd = "cat %s" % path

        def _read_write_chunk(channel, write_fd, size=1024):
            while channel.recv_ready():
                chunk = channel.recv(size)
                LOG.debug('read %d bytes', len(chunk))
                if chunk:
                    os.write(write_fd, chunk)

        def _write_to_pipe(read_fd, write_fd):
            try:
                os.close(read_fd)

                with self._ssh_client.session() as channel:
                    LOG.debug('Executing %s', cmd)
                    channel.exec_command(cmd)

                    while not channel.exit_status_ready():
                        _read_write_chunk(channel, write_fd)

                    LOG.debug('closing channel')
                    _read_write_chunk(channel, write_fd)
                    channel.recv_exit_status()

            except KeyboardInterrupt:
                return

        read_process = None

        try:
            read_pipe, write_pipe = os.pipe()
            read_process = Process(target=_write_to_pipe,
                                   args=(read_pipe, write_pipe),
                                   name='_write_to_pipe')
            read_process.start()
            os.close(write_pipe)
            yield read_pipe

            os.close(read_pipe)
            read_process.join()

            if read_process.exitcode:
                raise SshDestinationError('Failed to download %s' % path)
            LOG.debug('Successfully streamed %s', path)
        finally:
            if read_process:
                read_process.join()

    def _read_status(self):
        if self._status_exists():
            cmd = "cat %s" % self.status_path
            with self._ssh_client.get_remote_handlers(cmd) as (_, stdout, _):
                return MySQLStatus(content=stdout.read())
        else:
            return MySQLStatus()

    def _write_status(self, status):
        cmd = "cat - > %s" % self.status_path
        with self._ssh_client.get_remote_handlers(cmd) as (cin, _, _):
            cin.write(status.serialize())

    def _status_exists(self):
        """
        Check, if status exist

        :return: Exist status
        :rtype: bool
        :raise SshDestinationError: if any error.
        """
        cmd = "bash -c 'if test -s %s; " \
              "then echo exists; " \
              "else echo not_exists; " \
              "fi'" % self.status_path
        status, cerr = self._ssh_client.execute(cmd)

        if status.strip() == 'exists':
            return True
        elif status.strip() == 'not_exists':
            return False
        else:
            LOG.error(cerr)
            msg = 'Unrecognized response: %s' % status
            if status:
                raise SshDestinationError(msg)
            else:
                raise SshDestinationError('Empty response from '
                                          'SSH destination')

    def execute_command(self, cmd, quiet=False, background=False):
        """Execute ssh command


        :param cmd: Command for execution
        :type cmd: str
        :param quiet: If True don't print errors
        :param background: Don't wait until the command exits.
        :type background: bool
        :return: Handlers of stdin, stdout and stderr
        :rtype: tuple
        """
        LOG.debug('Executing: %s', cmd)
        return self._ssh_client.execute(
            cmd,
            quiet=quiet,
            background=background
        )

    @property
    def client(self):
        """Return client"""
        return self._ssh_client

    @property
    def host(self):
        """IP address of the destination."""
        return self._ssh_client.ssh_connect_info.host

    def _mkdirname_r(self, remote_name):
        """Create directory for a given file on the destination.
        For example, for a given file '/foo/bar/xyz' it would create
        directory '/foo/bar/'.

        :param remote_name: Full path to a file
        :type remote_name: str
        """
        return self._mkdir_r(os.path.dirname(remote_name))

    def netcat(self, command, port=9990):
        """
        Run netcat on the destination pipe it to a given command.

        """
        try:
            return self.execute_command("ncat -l %d --recv-only | "
                                        "%s" % (port, command))
        except SshDestinationError as err:
            LOG.error(err)

    def ensure_tcp_port_listening(self, port, wait_timeout=10):
        """
        Check that tcp port is open and ready to accept connections.
        Keep checking up to wait_timeout seconds.

        :param port: TCP port that is supposed to be listening.
        :type port: int
        :param wait_timeout: wait this many seconds until the port is ready.
        :type wait_timeout: int
        :return: True if the TCP port is listening.
        :rtype: bool
        """
        stop_waiting_at = time.time() + wait_timeout
        while time.time() < stop_waiting_at:
            try:

                cmd = "netstat -ln | grep -w 0.0.0.0:%d 2>&1 " \
                      "> /dev/null" % port
                cout, cerr = self.execute_command(cmd)
                LOG.debug('stdout: %s', cout)
                LOG.debug('stderr: %s', cerr)
                return True
            except SshClientException as err:
                LOG.debug(err)
                time.sleep(1)

        return False

    def _get_file_content(self, path):
        cmd = "cat %s" % path
        with self._ssh_client.get_remote_handlers(cmd) as (_, stdout, _):
            return stdout.read()

    def _move_file(self, source, destination):
        cmd = 'yes | cp -rf %s %s' % (source, destination)
        self.execute_command(cmd)
