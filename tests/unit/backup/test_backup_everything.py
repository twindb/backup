"""Tests for ``backup_everything`` lock-scope behaviour.

The two key invariants we want to pin down:

1. The ``binlogs_only`` path does NOT take the destination's cluster
   lock. Binlogs run on every replica for PITR redundancy, and wrapping
   them in the cluster lock would needlessly race with full/incremental
   backups at the top of the hour.
2. The full/incremental path still gates on the cluster lock so only
   one replica produces the large xtrabackup/file payloads.
"""
from contextlib import contextmanager
from unittest import mock

from twindb_backup.backup import backup_everything
from twindb_backup.destination.base_destination import ClusterLock


@contextmanager
def _acquired_lock():
    yield ClusterLock(acquired=True)


@contextmanager
def _contended_lock():
    yield ClusterLock(acquired=False)


def _mk_config():
    """Return a config mock with the minimum attributes used by
    ``backup_everything``. We bypass actual destination construction by
    returning a MagicMock whose ``cluster_lock`` can be stubbed per test.
    """
    cfg = mock.MagicMock()
    cfg.server_name = "cluster-xyz"
    return cfg


@mock.patch("twindb_backup.backup.set_open_files_limit")
@mock.patch("twindb_backup.backup.backup_binlogs")
@mock.patch("twindb_backup.backup.backup_mysql")
@mock.patch("twindb_backup.backup.backup_files")
def test_binlogs_only_skips_cluster_lock(
    mock_backup_files,
    mock_backup_mysql,
    mock_backup_binlogs,
    _mock_set_limit,
):
    cfg = _mk_config()
    # cluster_lock must NOT be called on the binlogs_only path.
    cfg.destination.return_value.cluster_lock.side_effect = AssertionError(
        "binlogs_only=True should not acquire the cluster lock"
    )

    backup_everything("hourly", cfg, binlogs_only=True)

    mock_backup_binlogs.assert_called_once_with("hourly", cfg)
    mock_backup_files.assert_not_called()
    mock_backup_mysql.assert_not_called()


@mock.patch("twindb_backup.backup.save_measures")
@mock.patch("twindb_backup.backup.set_open_files_limit")
@mock.patch("twindb_backup.backup.backup_binlogs")
@mock.patch("twindb_backup.backup.backup_mysql")
@mock.patch("twindb_backup.backup.backup_files")
def test_full_run_acquires_cluster_lock_and_runs_everything(
    mock_backup_files,
    mock_backup_mysql,
    mock_backup_binlogs,
    _mock_set_limit,
    _mock_save_measures,
):
    cfg = _mk_config()
    cfg.destination.return_value.cluster_lock.return_value = _acquired_lock()

    backup_everything("hourly", cfg, binlogs_only=False)

    cfg.destination.return_value.cluster_lock.assert_called_once_with("cluster-xyz")
    mock_backup_files.assert_called_once_with("hourly", cfg)
    mock_backup_mysql.assert_called_once_with("hourly", cfg)
    mock_backup_binlogs.assert_called_once_with("hourly", cfg)


@mock.patch("twindb_backup.backup.save_measures")
@mock.patch("twindb_backup.backup.set_open_files_limit")
@mock.patch("twindb_backup.backup.backup_binlogs")
@mock.patch("twindb_backup.backup.backup_mysql")
@mock.patch("twindb_backup.backup.backup_files")
def test_full_run_skips_when_lock_not_acquired(
    mock_backup_files,
    mock_backup_mysql,
    mock_backup_binlogs,
    _mock_set_limit,
    _mock_save_measures,
):
    cfg = _mk_config()
    cfg.destination.return_value.cluster_lock.return_value = _contended_lock()

    backup_everything("hourly", cfg, binlogs_only=False)

    mock_backup_files.assert_not_called()
    mock_backup_mysql.assert_not_called()
    mock_backup_binlogs.assert_not_called()
