"""Backup copy cache"""
import os
import shutil

from twindb_backup import LOG
from twindb_backup.cache.exceptions import CacheException


class Cache:
    """Class implements local cache to save full backup copies"""

    def __init__(self, path):
        """Init Cache object with cache storage in local path.
        The cache is a directory on a local file system e.g.
        ``/var/tmp/cache``.
        Each item is a sub directory in the cache::

            /var/tmp/cache/mysql-2017-05-12_03_47_21.xbstream.gz/
            /var/tmp/cache/mysql-2017-05-13_22_04_06.xbstream.gz/

        :param path: path to directory that becomes cache
        :raise CacheException: if path doesn't exist
        """
        if os.path.exists(path):
            self.path = path
        else:
            raise CacheException("Cache directory %s doesn't exist" % path)

    def __contains__(self, item):
        return item in os.listdir(self.path)

    def add(self, path, key=None):
        # pylint: disable=line-too-long
        """Add directory to cache.
        The directory may be a full or relative path with backup copy.
        The directory name must match with a file name of the backup copy.
        If backup copy is
        ``/path/to/backups/master1/daily/mysql/mysql-2017-05-13_22_04_06.xbstream.gz``.
        then the directory can be something like
        ``/var/tmp/mysql-2017-05-13_22_04_06.xbstream.gz/``.

        Let's say we want to add
        ``/var/tmp/mysql-2017-05-13_22_04_06.xbstream.gz/`` to the cache in
        ``/var/tmp/cache``.
        Then this method will create directory
        ``/var/tmp/cache/mysql-2017-05-13_22_04_06.xbstream.gz/``.

        If you want to save directory ``/var/tmp/foo`` in cache under
        a key name ``mysql-2017-05-13_22_04_06.xbstream.gz``
        you need to specify the key e.g.
        ``add('/var/tmp/cache', 'mysql-2017-05-13_22_04_06.xbstream.gz')``

        :param path: full or relative path
        :type path: str
        :param key: if specified the directory will be added as this key name
            in the cache
        :raise: CacheException if errors
        """
        if key:
            LOG.debug("Cache key %s", key)
            dst = os.path.join(self.path, key)
        else:
            dst = os.path.join(self.path, os.path.basename(path))

        LOG.debug("Saving content of %s in %s", path, dst)
        try:
            shutil.copytree(path, dst)
        except OSError as err:
            raise CacheException(err)

    def restore_in(self, item, path):
        """Restore backup copy item in path.

        :param item: directory in the cache
        :type item: str
        :param path: directory where to restore item
        :type path: str
        """

        item_content_path = os.path.join(self.path, item)

        for entry in os.listdir(item_content_path):
            full_path = os.path.join(item_content_path, entry)
            if os.path.isdir(full_path):
                shutil.copytree(full_path, os.path.join(path, entry))
            else:
                shutil.copy(full_path, path)

    def purge(self):
        """Remove all entries from the cache"""
        for item in os.listdir(self.path):
            shutil.rmtree(os.path.join(self.path, item))
