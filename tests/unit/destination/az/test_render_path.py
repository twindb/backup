from .util import mocked_az


def test_render_path():
    """Test render_path method, ensuring the remote path is prepended to the path."""
    c = mocked_az()

    assert c.render_path("test") == f"{c.remote_path}/test"
