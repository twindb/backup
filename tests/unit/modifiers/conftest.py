import pytest


@pytest.fixture
def input_file(tmpdir):
    filename = tmpdir.join('in.txt')
    with open(str(filename), 'w') as f:
        f.write('foo bar')
    return filename
