from os import path as osp


def test_list_files(ssh_client, tmpdir):

    root_dir = osp.join("/home", ssh_client.user, "tmp", "foo")
    print(root_dir)
    ssh_client.execute(f"mkdir -p '{root_dir}'")
    print(ssh_client.list_files(root_dir))

    ssh_client.execute("touch '%s'" % osp.join(root_dir, "bar.txt"))
    print(ssh_client.list_files(root_dir))
    print(ssh_client.list_files("blah"))

    ssh_client.execute("mkdir -p '%s'" % osp.join(root_dir, "subdir"))
    ssh_client.execute(
        "touch '%s'" % osp.join(root_dir, "subdir", "sub_bar.txt")
    )

    print("subdir with dirs")
    print(ssh_client.list_files(root_dir, recursive=True))
    print("subdir without dirs")
    print(ssh_client.list_files(root_dir, recursive=True, files_only=True))
