"""Class to describe MySQL backup copy"""
import json

from twindb_backup import MARIABACKUP_BINARY, MBSTREAM_BINARY, XBSTREAM_BINARY, XTRABACKUP_BINARY
from twindb_backup.copy.exceptions import WrongInputData
from twindb_backup.copy.periodic_copy import PeriodicCopy
from twindb_backup.source.mysql_source import MySQLFlavor


class MySQLCopy(PeriodicCopy):  # pylint: disable=too-many-instance-attributes
    """
    Instantiate a MySQL copy.

    :param host: Hostname where the backup was taken from.
    :type host: str
    :param run_type: Run type when the backup was taken: daily, weekly, etc.
    :type run_type: str
    :param name: Base name of the backup copy file as it's stored
        on the destination.
    :type name: str
    :raise WrongInputData: if type is neither full or incremental,
        if name is not a basename.
    """

    __attr = [
        "host",
        "run_type",
        "name",
        "binlog",
        "position",
        "lsn",
        "parent",
        "galera",
        "wsrep_provider_version",
        "config",
        "backup_started",
        "backup_finished",
        "type",
        "server_vendor",
    ]

    def __init__(self, *args, **kwargs):

        super(MySQLCopy, self).__init__(*args, **kwargs)

        self._source_type = "mysql"

        if "type" in kwargs and kwargs.get("type") in ["full", "incremental"]:
            self._type = kwargs.get("type")
        else:
            self._type = None

        if "/" in self.name:
            raise WrongInputData("name must be relative, without any slashes." " Got %s instead." % self.name)

        self._backup_started = int(kwargs.get("backup_started", 0)) or None
        self._backup_finished = int(kwargs.get("backup_finished", 0)) or None
        self._binlog = kwargs.get("binlog", None)
        self._position = kwargs.get("position", None)
        self._lsn = kwargs.get("lsn", None)
        self._parent = kwargs.get("parent", None)
        self._server_vendor = MySQLFlavor(kwargs.get("server_vendor", MySQLFlavor.ORACLE))

        if "wsrep_provider_version" in kwargs:
            self._wsrep_provider_version = kwargs.get("wsrep_provider_version")
            self._galera = self._wsrep_provider_version is not None
        else:
            self._galera = False
            self._wsrep_provider_version = None

        if "config" in kwargs and "config_files" in kwargs:
            raise WrongInputData("Either config or config_files can be used " "to initialize config attribute")

        if "config_files" in kwargs:
            self._config = {}
            config_files = kwargs.get("config_files", [])
            for config_file in config_files:
                with open(config_file, "r") as config_descr:
                    self._config[config_file] = config_descr.read()

        else:
            self._config = kwargs.get("config", {})

    def __eq__(self, other):
        comparison = ()
        for attr in self.__attr:
            comparison += (getattr(self, attr) == getattr(other, attr),)

        return all(comparison)

    def __le__(self, other):
        return self.lsn <= other.lsn

    def __lt__(self, other):
        return self.lsn < other.lsn

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "%s(%s) = %s" % (
            self.__class__.__name__,
            self.key,
            json.dumps(self.as_dict(), sort_keys=True, indent=4),
        )

    @property
    def created_at(self):
        """Timestamp when the backup job started."""
        return self._backup_started

    @property
    def backup_started(self):
        """Timestamp when the backup job started."""
        return self._backup_started

    @property
    def backup_finished(self):
        """Timestamp when the backup job finished."""
        return self._backup_finished

    @property
    def duration(self):
        """Time in seconds it took to take the backup."""
        return self._backup_finished - self._backup_started

    @property
    def binlog(self):
        """File name of the binlog."""
        return self._binlog

    @property
    def position(self):
        """Binlog position of the backup copy."""
        return self._position

    @property
    def type(self):
        """Full or incremental."""
        return self._type

    @property
    def parent(self):
        """For incremental backup it is a base full copy name."""
        return self._parent

    @property
    def lsn(self):
        """LSN of the backup."""
        return self._lsn

    @property
    def config(self):
        """Dictionary of configs and their content."""
        return self._config

    @property
    def galera(self):
        """True if the backup was taken from Galera."""
        return self._galera

    @property
    def server_vendor(self):
        return self._server_vendor

    @property
    def sort_key(self):
        return self.created_at or 0

    @property
    def xbstream_binary(self):
        return MBSTREAM_BINARY if self.server_vendor is MySQLFlavor.MARIADB else XBSTREAM_BINARY

    @property
    def xtrabackup_binary(self):
        return MARIABACKUP_BINARY if self.server_vendor is MySQLFlavor.MARIADB else XTRABACKUP_BINARY

    @property
    def wsrep_provider_version(self):
        """If it was Galera, value of wsrep_provider_version"""
        return self._wsrep_provider_version

    def as_dict(self):
        """Return representation of the class instance for output purposes."""
        result = {}
        for attr in self.__attr:
            result[attr] = getattr(self, attr)
        return result

    def serialize(self):
        """Prepare the status for storing as a string."""
        return json.dumps(self.as_dict())
