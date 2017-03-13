import pytest


@pytest.fixture
def input_file(tmpdir):
    filename = tmpdir.join('in.txt')
    with open(str(filename), 'w') as f:
        f.write('foo bar')
    return filename


@pytest.fixture
def keyring_file(tmpdir):
    keyring_file = tmpdir.join('keyring')
    with open(str(keyring_file), 'w') as f:
        f.write('foo bar')
    return keyring_file
