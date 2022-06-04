def test_get_text_content(ssh_client):
    print(ssh_client.get_text_content("/etc/passwd"))
