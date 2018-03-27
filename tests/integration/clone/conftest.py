import pytest


@pytest.fixture
def config_content_clone():
    return """

[ssh]
ssh_user=root
ssh_key={PRIVATE_KEY}

[mysql]
mysql_defaults_file={MY_CNF}
"""
