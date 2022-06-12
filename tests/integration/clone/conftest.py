from textwrap import dedent

import pytest


@pytest.fixture
def config_content_clone():
    return dedent(
        """
        [ssh]
        ssh_user=root
        ssh_key={PRIVATE_KEY}

        [mysql]
        mysql_defaults_file={MY_CNF}
        """
    )
