from subprocess import PIPE

import mock
import pytest

from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import DestinationError


# noinspection PyUnresolvedReferences
@mock.patch.object(BaseDestination, '_get_pretty_status')
@mock.patch.object(BaseDestination, '_write_status')
@mock.patch.object(BaseDestination, '_read_status')
def test_status(mock_read_status,
                mock_write_status,
                mock_pretty_status):
    dst = BaseDestination()
    dst.status_path = "/foo/bar"
    dst.status()
    mock_read_status.assert_called_once_with()
    dst.status(status={"foo": "bar"})
    mock_pretty_status.assert_called_once_with(dst.status_path)

@mock.patch('twindb_backup.destination.base_destination.Popen')
def test__save(mock_popen, tmpdir):
    dst = BaseDestination()
    f = tmpdir.join('some_dst')

    mock_proc = mock.Mock()
    mock_proc.communicate.return_value = ('foo', 'bar')
    mock_proc.returncode = 0

    mock_popen.return_value = mock_proc

    fo = open(str(f), 'w')
    # noinspection PyProtectedMember
    dst._save('foo', fo)

    mock_popen.assert_called_once_with('foo', stdin=fo,
                                       stdout=PIPE,
                                       stderr=PIPE)


@mock.patch('twindb_backup.destination.base_destination.Popen')
def test__save_error(mock_popen, tmpdir):
    dst = BaseDestination()
    f = tmpdir.join('some_dst')

    mock_proc = mock.Mock()
    mock_proc.communicate.return_value = ('foo', 'bar')
    mock_proc.returncode = 1

    mock_popen.return_value = mock_proc

    fo = open(str(f), 'w')
    with pytest.raises(DestinationError):
        # noinspection PyProtectedMember
        dst._save('foo', fo)


@mock.patch('twindb_backup.destination.base_destination.Popen')
def test__save_popen_error(mock_popen, tmpdir):
    dst = BaseDestination()
    f = tmpdir.join('some_dst')

    mock_popen.side_effect = OSError

    fo = open(str(f), 'w')
    with pytest.raises(DestinationError):
        # noinspection PyProtectedMember
        dst._save('foo', fo)


# noinspection PyUnresolvedReferences
@pytest.mark.parametrize('status, path, full_copy', [
    (
        {
            u'daily': {
                u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'position': None,
                    u'type': u'full'}},
            u'hourly': {
                u'46cf72633004/hourly/mysql/mysql-2017-03-20_03_11_19.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'parent': u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg',
                    u'position': None,
                    u'type': u'incremental'}},
            u'monthly': {},
            u'weekly': {},
            u'yearly': {}
        },
        's3://twindb-backup-test-travis-99649/46cf72633004/hourly/mysql/mysql-2017-03-20_03_11_19.xbstream.gz.gpg',
        's3://twindb-backup-test-travis-99649/46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg'
    ),
    (
        {
            u'daily': {
                u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'position': None,
                    u'type': u'full'}},
            u'hourly': {
                u'46cf72633004/hourly/mysql/mysql-2017-03-20_03_11_19.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'parent': u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg',
                    u'position': None,
                    u'type': u'incremental'}},
            u'monthly': {},
            u'weekly': {},
            u'yearly': {}
        },
        's3://twindb-backup-test-travis-99649/46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg',
        's3://twindb-backup-test-travis-99649/46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg'
    ),
    (
        {
            u'daily': {
                u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'position': None,
                    u'type': u'full'}},
            u'hourly': {
                u'46cf72633004/hourly/mysql/mysql-2017-03-20_03_11_19.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'parent': u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg',
                    u'position': None,
                    u'type': u'incremental'
                },
                u'foo': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'parent': u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg',
                    u'position': None,
                    u'type': u'incremental'
                }
            },
            u'monthly': {},
            u'weekly': {},
            u'yearly': {}
        },
        's3://twindb-backup-test-travis-99649/foo',
        's3://twindb-backup-test-travis-99649/46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg'
    )
])
@mock.patch.object(BaseDestination, 'status')
def test_get_full_copy_name(mock_status, status, path, full_copy):
    mock_status.return_value = status
    dst = BaseDestination()
    assert dst.get_full_copy_name(path) == full_copy


# noinspection PyUnresolvedReferences
@pytest.mark.parametrize('status, path', [
    (
        {
            u'daily': {
                u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'position': None,
                    u'type': u'full'}},
            u'hourly': {
                u'46cf72633004/hourly/mysql/mysql-2017-03-20_03_11_19.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'parent': u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg',
                    u'position': None,
                    u'type': u'incremental'}},
            u'monthly': {},
            u'weekly': {},
            u'yearly': {}
        },
        'foo'
    ),
    (
        None,
        'foo'
    ),
    (
        None,
        None
    ),
    (
        {
            u'daily': {
                u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'position': None,
                    u'type': u'full'}},
            u'hourly': {
                u'46cf72633004/hourly/mysql/mysql-2017-03-20_03_11_19.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'position': None,
                    u'type': u'incremental'}},
            u'monthly': {},
            u'weekly': {},
            u'yearly': {}
        },
        's3://twindb-backup-test-travis-99649/46cf72633004/hourly/mysql/mysql-2017-03-20_03_11_19.xbstream.gz.gpg'
    ),
])
@mock.patch.object(BaseDestination, 'status')
def test_get_full_copy_name_error(mock_status, status, path):
    mock_status.return_value = status
    dst = BaseDestination()
    with pytest.raises(DestinationError):
        dst.get_full_copy_name(path)


# noinspection PyUnresolvedReferences
@pytest.mark.parametrize('status, path', [
    (
        {
            u'daily': {
                u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'position': None,
                    u'type': u'full'}},
            u'hourly': {
                u'46cf72633004/hourly/mysql/mysql-2017-03-20_03_11_19.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'parent': u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg',
                    u'position': None,
                    u'type': u'incremental'}},
            u'monthly': {},
            u'weekly': {},
            u'yearly': {}
        },
        '/46cf72633004/hourly/mysql/mysql-2017-03-20_03_11_19.xbstream.gz.gpg'
    ),
    (
        {
            u'daily': {
                u'46cf72633004/daily/mysql/mysql-2017-03-20_03_11_13.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'backup_started': 0,
                    u'backup_finished': 1,
                    u'position': None,
                    u'type': u'full',}},
            u'hourly': {
                u'46cf72633004/hourly/mysql/mysql-2017-03-20_03_11_19.xbstream.gz.gpg': {
                    u'binlog': None,
                    u'config': [],
                    u'lsn': 1632036,
                    u'backup_started': 1,
                    u'backup_finished': 2,
                    u'position': None,
                    u'type': u'incremental'}},
            u'monthly': {},
            u'weekly': {},
            u'yearly': {}
        },
        '/46cf72633004/hourly/mysql/mysql-2017-03-20_03_11_19.xbstream.gz.gpg'
    ),
])
@mock.patch.object(BaseDestination, 'status')
def test_get_latest_backup(mock_status, status, path):
    mock_status.return_value = status
    dst = BaseDestination()
    url = dst.get_latest_backup()
    assert url == path

