"""Base destination ``cluster_lock`` is a no-op that reports acquired."""

from twindb_backup.destination.base_destination import BaseDestination, ClusterLock


class _StubDestination(BaseDestination):
    """Minimal concrete destination to exercise the base ``cluster_lock``."""

    def delete(self, path):  # pragma: no cover - not exercised
        raise NotImplementedError

    def get_stream(self, copy):  # pragma: no cover - not exercised
        raise NotImplementedError

    def read(self, filepath):  # pragma: no cover - not exercised
        raise NotImplementedError

    def save(self, handler, filepath):  # pragma: no cover - not exercised
        raise NotImplementedError

    def write(self, content, filepath):  # pragma: no cover - not exercised
        raise NotImplementedError

    def _list_files(self, prefix=None, recursive=False, files_only=False):  # pragma: no cover
        raise NotImplementedError


def test_base_cluster_lock_is_noop_and_always_acquired():
    dst = _StubDestination(remote_path="/anywhere")
    with dst.cluster_lock("any-id") as lock:
        assert isinstance(lock, ClusterLock)
        assert lock.acquired is True
