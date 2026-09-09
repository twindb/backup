"""Tests for ``_seed_binlog_status_from_legacy``.

The legacy layout stored a single shared ``<server_name>/binlog-status``
blob. After upgrading, binlog status is written per-replica at
``<server_name>/<hostname>/binlog-status``. Without a seed step the
post-upgrade run would think it had uploaded nothing and attempt to
re-upload every binlog that already made it up — Azure's
``upload_blob`` (without ``overwrite``) errors on existing blobs.

The seed helper copies the entries from the legacy blob that belong to
the local replica (match by MySQL hostname prefix in the filename) into
the new per-host status.
"""
from unittest import mock

from twindb_backup.backup import _seed_binlog_status_from_legacy
from twindb_backup.copy.binlog_copy import BinlogCopy
from twindb_backup.status.binlog_status import BinlogStatus
from twindb_backup.status.exceptions import CorruptedStatus


def _mk_config():
    cfg = mock.Mock()
    cfg.server_name = "cluster-xyz"
    return cfg


def _legacy_with(entries):
    """Build a ``BinlogStatus`` populated from a list of (host, name)
    tuples without touching a destination.
    """
    legacy = BinlogStatus()
    for host, name in entries:
        legacy.add(BinlogCopy(host=host, name=name, created_at=0))
    return legacy


@mock.patch("twindb_backup.backup.socket")
@mock.patch("twindb_backup.backup.BinlogStatus")
def test_seed_noop_when_per_host_not_empty(mock_binlog_status_cls, _mock_socket):
    status = mock.Mock()
    status.__len__ = mock.Mock(return_value=1)

    _seed_binlog_status_from_legacy(mock.Mock(), status, _mk_config())

    mock_binlog_status_cls.assert_not_called()
    status.add.assert_not_called()


@mock.patch("twindb_backup.backup.socket")
@mock.patch("twindb_backup.backup.BinlogStatus")
def test_seed_noop_when_legacy_empty(mock_binlog_status_cls, _mock_socket):
    status = mock.Mock()
    status.__len__ = mock.Mock(return_value=0)

    empty_legacy = mock.Mock()
    empty_legacy.__len__ = mock.Mock(return_value=0)
    mock_binlog_status_cls.return_value = empty_legacy

    _seed_binlog_status_from_legacy(mock.Mock(), status, _mk_config())

    status.add.assert_not_called()
    status.save.assert_not_called()


@mock.patch("twindb_backup.backup.socket")
@mock.patch("twindb_backup.backup.BinlogStatus")
def test_seed_copies_only_local_replica_entries(mock_binlog_status_cls, mock_socket):
    mock_socket.gethostname.return_value = "replica-a.example.com"

    status = mock.Mock()
    status.__len__ = mock.Mock(return_value=0)

    legacy = _legacy_with(
        [
            ("cluster-xyz", "replica-a_binlog.000001"),
            ("cluster-xyz", "replica-b_binlog.000001"),
            ("cluster-xyz", "replica-a_binlog.000002"),
            ("cluster-xyz", "replica-c_binlog.000003"),
        ]
    )
    mock_binlog_status_cls.return_value = legacy

    _seed_binlog_status_from_legacy(mock.Mock(), status, _mk_config())

    added = [call.args[0].name for call in status.add.call_args_list]
    assert added == ["replica-a_binlog.000001", "replica-a_binlog.000002"]
    status.save.assert_called_once()


@mock.patch("twindb_backup.backup.socket")
@mock.patch("twindb_backup.backup.BinlogStatus")
def test_seed_swallows_corrupt_legacy_status(mock_binlog_status_cls, _mock_socket):
    """A corrupt legacy blob must not block the upgrade path."""
    status = mock.Mock()
    status.__len__ = mock.Mock(return_value=0)

    mock_binlog_status_cls.side_effect = CorruptedStatus("bad checksum")

    # No exception propagates and no entries are seeded.
    _seed_binlog_status_from_legacy(mock.Mock(), status, _mk_config())

    status.add.assert_not_called()
    status.save.assert_not_called()


@mock.patch("twindb_backup.backup.socket")
@mock.patch("twindb_backup.backup.BinlogStatus")
def test_seed_handles_no_local_matches(mock_binlog_status_cls, mock_socket):
    mock_socket.gethostname.return_value = "replica-z"

    status = mock.Mock()
    status.__len__ = mock.Mock(return_value=0)

    legacy = _legacy_with(
        [
            ("cluster-xyz", "replica-a_binlog.000001"),
            ("cluster-xyz", "replica-b_binlog.000001"),
        ]
    )
    mock_binlog_status_cls.return_value = legacy

    _seed_binlog_status_from_legacy(mock.Mock(), status, _mk_config())

    status.add.assert_not_called()
    status.save.assert_not_called()
