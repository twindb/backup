"""Tests for AZ.cluster_lock context manager.

These tests exercise the lease acquisition/release logic without hitting
Azure. Each test patches the ``BlobLeaseClient`` so we can assert the
outcomes deterministically.
"""

from unittest.mock import MagicMock, patch

import azure.core.exceptions as ae
import pytest

from twindb_backup.destination.az import _CLUSTER_LOCK_BLOB
from twindb_backup.destination.base_destination import ClusterLock

from .util import mocked_az


def _make_http_error(error_code):
    err = ae.HttpResponseError(message=error_code)
    err.error_code = error_code
    return err


def test_cluster_lock_acquired_creates_blob_and_lease():
    """Happy path: lease is acquired, blob is created, lease is released."""
    c = mocked_az()
    blob_client = MagicMock()
    c.container_client.get_blob_client = MagicMock(return_value=blob_client)

    with patch("twindb_backup.destination.az.BlobLeaseClient") as mock_lease_cls:
        lease = MagicMock()
        mock_lease_cls.return_value = lease

        with c.cluster_lock("prod-primary-db", ttl=30) as lock:
            assert isinstance(lock, ClusterLock)
            assert lock.acquired is True

    blob_client.upload_blob.assert_called_once()
    args, kwargs = blob_client.upload_blob.call_args
    assert args[0] == b""
    assert kwargs.get("overwrite") is False

    c.container_client.get_blob_client.assert_called_once()
    assert c.container_client.get_blob_client.call_args[0][0].endswith(
        f"prod-primary-db/{_CLUSTER_LOCK_BLOB}"
    )

    lease.acquire.assert_called_once_with(lease_duration=30)
    lease.release.assert_called_once()


def test_cluster_lock_tolerates_existing_blob():
    """If the lease blob already exists we do not raise."""
    c = mocked_az()
    blob_client = MagicMock()
    blob_client.upload_blob.side_effect = ae.ResourceExistsError("BlobAlreadyExists")
    c.container_client.get_blob_client = MagicMock(return_value=blob_client)

    with patch("twindb_backup.destination.az.BlobLeaseClient") as mock_lease_cls:
        lease = MagicMock()
        mock_lease_cls.return_value = lease

        with c.cluster_lock("prod-primary-db") as lock:
            assert lock.acquired is True

    lease.acquire.assert_called_once()
    lease.release.assert_called_once()


def test_cluster_lock_held_by_another_member_yields_not_acquired():
    """LeaseAlreadyPresent → acquired=False and we do not release."""
    c = mocked_az()
    blob_client = MagicMock()
    c.container_client.get_blob_client = MagicMock(return_value=blob_client)

    with patch("twindb_backup.destination.az.BlobLeaseClient") as mock_lease_cls:
        lease = MagicMock()
        lease.acquire.side_effect = _make_http_error("LeaseAlreadyPresent")
        mock_lease_cls.return_value = lease

        with c.cluster_lock("prod-primary-db") as lock:
            assert lock.acquired is False

    lease.acquire.assert_called_once()
    lease.release.assert_not_called()


def test_cluster_lock_transient_error_proceeds_without_coordination():
    """Any non-LeaseAlreadyPresent Azure error must not block the backup."""
    c = mocked_az()
    blob_client = MagicMock()
    c.container_client.get_blob_client = MagicMock(return_value=blob_client)

    with patch("twindb_backup.destination.az.BlobLeaseClient") as mock_lease_cls:
        lease = MagicMock()
        lease.acquire.side_effect = _make_http_error("ServerBusy")
        mock_lease_cls.return_value = lease

        with c.cluster_lock("prod-primary-db") as lock:
            # We fail open: backup still runs, just without coordination.
            assert lock.acquired is True

    lease.acquire.assert_called_once()
    lease.release.assert_not_called()


def test_cluster_lock_ttl_is_clamped():
    """Azure caps blob leases at 60s; values are clamped into [15, 60]."""
    c = mocked_az()
    blob_client = MagicMock()
    c.container_client.get_blob_client = MagicMock(return_value=blob_client)

    with patch("twindb_backup.destination.az.BlobLeaseClient") as mock_lease_cls:
        lease = MagicMock()
        mock_lease_cls.return_value = lease

        with c.cluster_lock("prod-primary-db", ttl=5):
            pass
        lease.acquire.assert_called_with(lease_duration=15)

        lease.reset_mock()
        with c.cluster_lock("prod-primary-db", ttl=999):
            pass
        lease.acquire.assert_called_with(lease_duration=60)


def test_cluster_lock_release_failure_is_swallowed():
    """A best-effort release must not mask a successful backup."""
    c = mocked_az()
    blob_client = MagicMock()
    c.container_client.get_blob_client = MagicMock(return_value=blob_client)

    with patch("twindb_backup.destination.az.BlobLeaseClient") as mock_lease_cls:
        lease = MagicMock()
        lease.release.side_effect = ae.HttpResponseError("boom")
        mock_lease_cls.return_value = lease

        with c.cluster_lock("prod-primary-db") as lock:
            assert lock.acquired is True

    lease.release.assert_called_once()


def test_cluster_lock_exception_inside_context_still_releases():
    """If the backup body raises, we still attempt to release the lease."""
    c = mocked_az()
    blob_client = MagicMock()
    c.container_client.get_blob_client = MagicMock(return_value=blob_client)

    with patch("twindb_backup.destination.az.BlobLeaseClient") as mock_lease_cls:
        lease = MagicMock()
        mock_lease_cls.return_value = lease

        with pytest.raises(RuntimeError):
            with c.cluster_lock("prod-primary-db"):
                raise RuntimeError("simulated backup failure")

    lease.release.assert_called_once()
