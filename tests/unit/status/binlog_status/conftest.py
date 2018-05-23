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
    return """ew0KICAibWFzdGVyMS9iaW5sb2cvbXlzcWxiaW4wMDEuYmluIjogew0KIC
    AgICJ0aW1lX2NyZWF0ZWQiOiAiMTAwNTAwIg0KICB9LA0KICAibWFzdGVyMS9iaW5sb2
    cvbXlzcWxiaW4wMDIuYmluIjogew0KICAgICJ0aW1lX2NyZWF0ZWQiOiAiMTAwNTAxIg
    0KICB9LA0KICAibWFzdGVyMS9iaW5sb2cvbXlzcWxiaW4wMDMuYmluIjogew0KICAgIC
    J0aW1lX2NyZWF0ZWQiOiAiMTAwNTAyIg0KICB9LA0KICAibWFzdGVyMS9iaW5sb2cvbX
    lzcWxiaW4wMDQuYmluIjogew0KICAgICJ0aW1lX2NyZWF0ZWQiOiAiMTAwNTAzIg0KIC
    B9LA0KICAibWFzdGVyMS9iaW5sb2cvbXlzcWxiaW4wMDUuYmluIjogew0KICAgICJ0aW
    1lX2NyZWF0ZWQiOiAiMTAwNTA0Ig0KICB9DQp9"""
