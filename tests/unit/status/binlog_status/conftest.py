import pytest


@pytest.fixture
def raw_binlog_status():
    """
    Returns base64 of binlog status
        {
          "master1/binlog/mysqlbin001.bin": {
            "time_created": "100500"
          },
          "master1/binlog/mysqlbin002.bin": {
            "time_created": "100501"
          },
          "master1/binlog/mysqlbin003.bin": {
            "time_created": "100502"
          },
          "master1/binlog/mysqlbin004.bin": {
            "time_created": "100503"
          },
          "master1/binlog/mysqlbin005.bin": {
            "time_created": "100504"
          }
        }
    """
    return """
        {
            "status": "ew0KICAibWFzdGVyMS9iaW5sb2cvbXlzcWxiaW4wMDEuYmluIjogew0KICAgICJ0aW1lX2NyZWF0ZWQiOiAiMTAwNTAwIg0KICB9LA0KICAibWFzdGVyMS9iaW5sb2cvbXlzcWxiaW4wMDIuYmluIjogew0KICAgICJ0aW1lX2NyZWF0ZWQiOiAiMTAwNTAxIg0KICB9LA0KICAibWFzdGVyMS9iaW5sb2cvbXlzcWxiaW4wMDMuYmluIjogew0KICAgICJ0aW1lX2NyZWF0ZWQiOiAiMTAwNTAyIg0KICB9LA0KICAibWFzdGVyMS9iaW5sb2cvbXlzcWxiaW4wMDQuYmluIjogew0KICAgICJ0aW1lX2NyZWF0ZWQiOiAiMTAwNTAzIg0KICB9LA0KICAibWFzdGVyMS9iaW5sb2cvbXlzcWxiaW4wMDUuYmluIjogew0KICAgICJ0aW1lX2NyZWF0ZWQiOiAiMTAwNTA0Ig0KICB9DQp9",
            "version": 1,
            "md5": "2cf1662594b5d873d94d3eacf8a1bcdf"
        }
        """
