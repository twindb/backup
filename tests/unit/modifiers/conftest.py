import pytest


@pytest.fixture
def input_file(tmpdir):
    filename = tmpdir.join('in.txt')
    with open(str(filename), 'w') as f:
        f.write('foo bar')
    return filename


@pytest.fixture
def keyring_file(tmpdir):
    public_file = tmpdir.join('keyring')
    with open(str(public_file), 'w') as f:
        f.write('foo bar')
    return public_file


@pytest.fixture
def secret_keyring_file(tmpdir):
    secret_file = tmpdir.join('secret_keyring')
    with open(str(secret_file), 'w') as f:
        f.write('foo bar')
    return secret_file
