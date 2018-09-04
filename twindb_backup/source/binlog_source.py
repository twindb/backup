"""
Module defines MySQL binlog source class for backing them up.
"""
from os.path import dirname, join, basename

from subprocess import Popen, PIPE

import struct

from twindb_backup import LOG
from twindb_backup.source.base_source import BaseSource
from twindb_backup.source.exceptions import BinlogSourceError


class BinlogV4Event(object):
    def __init__(self, **kwargs):
        self.timestamp = kwargs.get('timestamp')
        self.type_code = kwargs.get('type_code')
        self.server_id = kwargs.get('server_id')
        self.event_length = kwargs.get('event_length')
        self.curr_position = kwargs.get('curr_position')
        self.next_position = kwargs.get('next_position')
        self.flags = kwargs.get('flags')


class BinlogParser(object):
    """
    Class parses a binlog file.

    :param binlog: path to a binlog file.
    :type binlog: str
    """
    def __init__(self, binlog):
        self._binlog = binlog

    @property
    def name(self):
        """Binlog base name"""
        return basename(self._binlog)

    @property
    def created_at(self):
        """Timestamp when the binlog was created"""
        try:
            with open(self._binlog) as fp:
                self.__read_magic_number(fp)
                return self.__read_int(fp, 4)
        except IOError as err:
            raise BinlogSourceError(
                "Failed to read the 'created_at' attribute: %s" % err
            )

    @property
    def start_position(self):
        """Minimal position in the binlog"""
        return 4

    @property
    def end_position(self):
        """Last position in the binlog"""
        last_position = self.start_position
        with open(self._binlog) as fp:
            self.__read_magic_number(fp)
            while True:
                event = self.__read_binlog_event(fp)
                if event:
                    last_position = event.curr_position
                else:
                    break
        return last_position

    @staticmethod
    def __read_magic_number(fdesc):
        return fdesc.read(4)

    @staticmethod
    def __read_int(fdesc, n):
        if n == 4:
            return struct.unpack('i', fdesc.read(n))[0]
        elif n == 2:
            return struct.unpack('h', fdesc.read(n))[0]
        elif n == 1:
            return struct.unpack('b', fdesc.read(n))[0]
        else:
            raise NotImplementedError(
                'Reading %d bytes integer is unsupported'
                % n
            )

    def __read_binlog_event(self, fp):
        """
        Read binlog event from file descriptor
        :param fp: File descriptor
        :return: Binlog event
        :rtype: BinlogV4Event
        """
        position = fp.tell()
        try:
            event = BinlogV4Event(
                timestamp=self.__read_int(fp, 4),
                type_code=self.__read_int(fp, 1),
                server_id=self.__read_int(fp, 4),
                event_length=self.__read_int(fp, 4),
                curr_position=position,
                next_position=self.__read_int(fp, 4),
                flags=self.__read_int(fp, 2)
            )
            fp.read(event.event_length - 19)
            return event
        except struct.error:
            return None


class BinlogSource(BaseSource):
    """
    MySQL Binlog source.

    :param run_type: The backup copy interval. hourly, daily, etc.
    :type run_type: str
    :param mysql_client: Instance that can be used to execute queries in MySQL.
    :type mysql_client: MySQLClient
    :param binlog_file: Name of the binlog file as it appears in
        ``SHOW BINARY LOGS``.
    :type binlog_file: str
    """
    def __init__(self, run_type, mysql_client, binlog_file=None):
        super(BinlogSource, self).__init__(run_type)
        self._mysql_client = mysql_client
        self._media_type = 'binlog',
        self._binlog_file = binlog_file
        self._suffix = ''

    @property
    def suffix(self):
        return self._suffix

    @suffix.setter
    def suffix(self, value):
        self._suffix = value

    def get_stream(self):
        """
        Stream content of one binary file.

        :return: stream of bytes with the binlog content.
        """
        with self._mysql_client.cursor() as cursor:
            cursor.execute("SELECT @@log_bin_basename AS log_bin_basename")
            row = cursor.fetchone()
            log_bin_basename = row['log_bin_basename']

        log_bin_dirname = dirname(log_bin_basename)
        log_bin_file = join(log_bin_dirname, self._binlog_file)

        cmd = [
            "tar",
            "cf",
            "-",
            log_bin_file,
        ]
        try:
            LOG.debug('Running %s', ' '.join(cmd))

            proc = Popen(cmd, stderr=PIPE, stdout=PIPE)

            yield proc.stdout

            _, cerr = proc.communicate()
            if proc.returncode:
                LOG.error('Failed to read from %s: %s', log_bin_file, cerr)
                exit(1)
            else:
                LOG.debug('Successfully streamed %s', log_bin_file)

        except OSError as err:
            LOG.error('Failed to run %s: %s', cmd, err)
            exit(1)

    def get_name(self):
        return self._binlog_file
