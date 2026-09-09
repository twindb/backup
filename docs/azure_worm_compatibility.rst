Azure WORM Compatibility Notes
==============================

This note captures the phase-2 design direction for running TwinDB against immutable Azure Blob containers after the phase-1 managed identity rollout.

Current blockers
----------------

TwinDB is not WORM-compatible today because it still depends on mutable and destructive blob operations in the Azure destination flow:

- ``twindb_backup/source/mysql_source.py`` deletes remote backup copies when applying retention.
- ``twindb_backup/source/file_source.py`` deletes remote file backup copies when applying retention.
- ``twindb_backup/backup.py`` deletes binlogs and cleanup copies through the destination object.
- ``twindb_backup/status/base_status.py`` overwrites ``status`` and ``binlog-status`` blobs in place.

Container-level immutability blocks those delete and overwrite patterns, so authentication changes alone are not enough to make TwinDB WORM-safe.

Phase-2 decisions
-----------------

1. Add a provider-managed retention mode.

   In this mode, TwinDB must stop calling ``dst.delete(...)`` for remote retention and cleanup. Azure lifecycle management and immutable storage retention policies become the source of truth for payload expiration.

2. Move mutable status out of the immutable backup container.

   ``status`` and ``binlog-status`` blobs should live in a separate mutable location. The cleanest follow-on design is a separate Azure status container or destination stanza for metadata, leaving the payload container append-only.

3. Use finite immutability windows.

   The target model should use time-based retention sized to recovery requirements rather than "infinite retention". That keeps the operational model compatible with Azure lifecycle cleanup once blobs age past the immutability window.

4. Validate on unlocked non-production storage before any lock decision.

   The first end-to-end WORM validation should use a non-production container with an unlocked immutability policy. Validate backup writes, status writes, restore reads, and lifecycle-driven cleanup behavior before any container is locked.

Suggested implementation shape
------------------------------

The likely code changes for the phase-2 follow-on are:

- Add a configuration switch such as ``remote_delete = false`` or ``retention_mode = provider_managed`` and thread it into the retention and cleanup call sites.
- Teach the backup/status flow to use a separate mutable Azure location for status metadata.
- Keep the existing phase-1 managed identity authentication path for both payload and status destinations, but allow them to point at different containers.

Recommended validation order
----------------------------

1. Enable managed identity auth first.
2. Pre-create separate payload and status containers.
3. Enable provider-managed retention mode so TwinDB stops issuing remote deletes.
4. Apply a finite, unlocked immutability policy to the payload container.
5. Confirm backups still upload, status metadata still updates, restores still read, and Azure lifecycle cleanup works after the retention window expires.
