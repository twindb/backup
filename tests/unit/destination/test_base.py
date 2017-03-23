from subprocess import PIPE

import mock
import pytest

from twindb_backup.destination.base_destination import BaseDestination, \
    DestinationError


@mock.patch.object(BaseDestination, '_write_status')
@mock.patch.object(BaseDestination, '_read_status')
def test_status(mock_read_status, mock_write_status):
    dst = BaseDestination()
    dst.status(status='foo')
    mock_write_status.assesrt_called_once_with('foo')
    dst.status()
    mock_read_status.assert_called_once_with()


@mock.patch('twindb_backup.destination.base_destination.Popen')
def test__save(mock_popen, tmpdir):
    dst = BaseDestination()
    f = tmpdir.join('some_dst')

    mock_proc = mock.Mock()
    mock_proc.communicate.return_value = ('foo', 'bar')
    mock_proc.returncode = 0

    mock_popen.return_value = mock_proc

    fo = open(str(f), 'w')
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
        dst._save('foo', fo)


@mock.patch('twindb_backup.destination.base_destination.Popen')
def test__save_popen_error(mock_popen, tmpdir):
    dst = BaseDestination()
    f = tmpdir.join('some_dst')

    mock_popen.side_effect = OSError

    fo = open(str(f), 'w')
    with pytest.raises(DestinationError):
        dst._save('foo', fo)


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


