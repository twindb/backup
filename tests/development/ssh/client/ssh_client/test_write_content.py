def test_write_content(ssh_client):
    path = "/tmp/foo"
    content = "foo"
    ssh_client.write_content(path, content)
    assert ssh_client.get_text_content(path) == content
