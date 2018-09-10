# -*- coding: utf-8 -*-
"""
Module for SSH destination.
"""
import socket

import os
from os import path as osp
from contextlib import contextmanager
from multiprocessing import Process

import time

from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import SshDestinationError
from twindb_backup.ssh.client import SshClient
from twindb_backup.ssh.exceptions import SshClientException
from twindb_backup.status.mysql_status import MySQLStatus


class Ssh(BaseDestination):
    """
    SSH destination class

    :param remote_path: Path to store backup
    :param kwargs: Keyword arguments. See below
    :param kwargs: dict

    :**hostname**(str): Hostname of the host where backup is taken from.
    :**ssh_host**(str): Hostname for SSH connection. Default '127.0.0.1'.
    :**ssh_user**(str): Username for SSH connection. Default 'root'.
    :**ssh_port**(int): TCP port for SSH connection. Default 22.
    :**ssh_key**(str): File with an rsa/dsa key for SSH authentication.
        Default '/root/.ssh/id_rsa'.
    """
    def __init__(self, remote_path, **kwargs):

        super(Ssh, self).__init__(remote_path)

        self._ssh_client = SshClient(
            host=kwargs.get('ssh_host', '127.0.0.1'),
            port=kwargs.get('ssh_port', 22),
            user=kwargs.get('ssh_user', 'root'),
            key=kwargs.get('ssh_key', '/root/.ssh/id_rsa')
        )

        self._hostname = kwargs.get('hostname', socket.gethostname())

    def __str__(self):
        return "Ssh(ssh://%s@%s:%d%s)" % (
            self.user,
            self.host,
            self.port,
            self.remote_path,
        )

    def status_path(self, cls=MySQLStatus):
        return "{remote_path}/{hostname}/{basename}".format(
            remote_path=self.remote_path,
            hostname=self._hostname,
            basename=cls().basename
        )

    def save(self, handler, name):
        """
        Read from handler and save it on remote ssh server

        :param name: relative path to a file to store the backup copy.
        :param handler: stream with content of the backup.
        """
        remote_name = osp.join(
            self.remote_path,
            name
        )
        self._mkdir_r(osp.dirname(remote_name))

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

    def _mkdir_r(self, path):
        """
        Create directory on the remote server

        :param path: remote directory
        :type path: str
        """
        if path == "/var/lib/mysql":
            1/0

        cmd = 'mkdir -p "%s"' % path
        self.execute_command(cmd)

    def _list_files(self, path, recursive=False, files_only=False):
        return self._ssh_client.list_files(
            path,
            recursive=recursive,
            files_only=files_only
        )

    def delete(self, obj):
        """
        Delete file by path

        :param obj: path to a remote file.
        """
        cmd = "rm %s" % obj
        self.execute_command(cmd)

    @contextmanager
    def get_stream(self, copy):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination

        :param copy: Backup copy
        :type copy: BaseCopy
        :return: Standard output.
        """

        path = "%s/%s" % (self.remote_path, copy.key)
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

    def _read_status(self, cls=MySQLStatus):
        if self._status_exists(cls=cls):
            cmd = "cat %s" % self.status_path(cls=cls)
            with self._ssh_client.get_remote_handlers(cmd) as (_, stdout, _):
                return cls(content=stdout.read())
        else:
            return cls()

    def _write_status(self, status, cls=MySQLStatus):
        cmd = "cat - > %s" % self.status_path(cls=cls)
        with self._ssh_client.get_remote_handlers(cmd) as (cin, _, _):
            cin.write(status.serialize())

    def _status_exists(self, cls=MySQLStatus):
        """
        Check, if status exist

        :return: Exist status
        :rtype: bool
        :raise SshDestinationError: if any error.
        """
        cmd = "bash -c 'if test -s %s; " \
              "then echo exists; " \
              "else echo not_exists; " \
              "fi'" % self.status_path(cls=cls)
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
                raise SshDestinationError(
                    'Empty response from SSH destination'
                )

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
        return self._ssh_client.host

    @property
    def port(self):
        """TCP port of the destination."""
        return self._ssh_client.port

    @property
    def user(self):
        """SSH user."""
        return self._ssh_client.user

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
