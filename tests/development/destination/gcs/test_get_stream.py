from twindb_backup import LOG
from twindb_backup.status.mysql_status import MySQLStatus


def test_get_stream(gs):
    status = MySQLStatus(dst=gs)
    copy = status["master1/daily/mysql/mysql-2019-04-04_05_29_05.xbstream.gz"]

    with gs.get_stream(copy) as stream:
        LOG.debug("starting reading from pipe")
        content = stream.read()
        LOG.debug("finished reading from pipe")
    assert len(content), "Failed to read from GS"
    LOG.info("Read %d bytes", len(content))
