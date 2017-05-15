import os

import pytest


@pytest.fixture
def cache_dir(tmpdir):
    cache_path = tmpdir.mkdir('cache')
    return cache_path
