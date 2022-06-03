from os.path import dirname
from textwrap import dedent

import pytest

from twindb_backup.util import mkdir_p, my_cnfs


@pytest.mark.parametrize(
    "paths, configs",
    [
        (["{root}/etc/my.cnf"], {"{root}/etc/my.cnf": "some content"}),
        (
            ["{root}/etc/my.cnf"],
            {
                "{root}/etc/my.cnf": dedent(
                    """
                [mysqld]
                some_option
                !include {root}/home/mydir/myopt.cnf
                """
                ),
                "{root}/home/mydir/myopt.cnf": dedent(
                    """
                [mysqld]
                some_option
                """
                ),
            },
        ),
        (
            ["{root}/etc/my.cnf"],
            {
                "{root}/etc/my.cnf": dedent(
                    """
                [mysqld]
                some_option
                !include {root}/home/mydir/myopt.cnf
                """
                ),
                "{root}/home/mydir/myopt.cnf": dedent(
                    """
                [mysqld]
                !includedir {root}/other_dir/
                """
                ),
                "{root}/other_dir/a.cnf": dedent(
                    """
                [mysqld]
                some content
                """
                ),
                "{root}/other_dir/b.cnf": dedent(
                    """
                [mysqld]
                some content
                """
                ),
            },
        ),
    ],
)
def test_my_cnfs(paths, configs, tmpdir):
    root = tmpdir.mkdir("root")

    # prefix each path in paths
    full_paths = [x.format(root=str(root)) for x in paths]

    keys = []
    for key, content in configs.items():
        full_path = key.format(root=str(root))
        keys.append(full_path)
        mkdir_p(dirname(full_path))
        with open(full_path, "w") as fp:
            fp.write(content.format(root=str(root)))

    list_a = sorted(my_cnfs(common_paths=full_paths))
    list_b = sorted(keys)

    assert list_a == list_b
