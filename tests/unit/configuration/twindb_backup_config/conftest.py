import pytest


@pytest.fixture
def config_file(config_content, tmpdir):
    cfg_file = tmpdir.join('twindb-backup.cfg')
    dst = tmpdir.mkdir('dst')
    with open(str(cfg_file), 'w') as fp:
        fp.write(
            config_content.format(
                port=123,
                destination=str(dst)
            )
        )
    return cfg_file
