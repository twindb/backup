from twindb_backup.status.status import Status


def test_add(status_raw_empty):
    status = Status(status_raw_empty)
    assert status.valid
    status.add(
        "daily",
        "foo",
        binlog="binlog1",
        position=101,
        lsn='aaa:123',
        type='full',
        backup_started=123,
        backup_finished=456,
        config=[
            {
                'my.cnf': 'aaa'
            }, {
                'my-2.cnf': 'bbb'
            }
        ]
    )
    assert len(status.daily) == 1
    assert status.daily['foo']['binlog'] == 'binlog1'
    assert status.daily['foo']['config'] == [
        {
            'my.cnf': 'aaa'
        }, {
            'my-2.cnf': 'bbb'
        }
    ]


def test_add_sets_parent(status_raw_empty):
    status = Status(status_raw_empty)
    assert status.valid
    status.add(
        "daily",
        "foo",
        type='incremental',
        parent='bar'
    )
    assert status.daily['foo']['parent'] == 'bar'


def test_add_sets_wsrep_provider_version(status_raw_empty):
    status = Status(status_raw_empty)
    assert status.valid
    status.add(
        "daily",
        "foo",
        galera=True,
        wsrep_provider_version=12345
    )
    assert status.daily['foo']['wsrep_provider_version'] == 12345
