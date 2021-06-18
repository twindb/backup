from os import listdir, environ
from os import path as osp
from pprint import pprint
from subprocess import run

import boto3

from twindb_backup import __version__

OS_VERSIONS = [
    # centos
    "7",
    # # ubuntu
    "focal",
    "bionic",
    "xenial",
    # debian
    "stretch",
]
PKG_DIR = "omnibus/pkg"

OS_DETAILS = {
    "7": {
        "flavor": "CentOS",
        "name": "CentOS 7"
    },
    "focal": {
        "flavor": "Ubuntu",
        "name": "Ubuntu focal"
    },
    "bionic": {
        "flavor": "Ubuntu",
        "name": "Ubuntu bionic"
    },
    "xenial": {
        "flavor": "Ubuntu",
        "name": "Ubuntu xenial"
    },
    "stretch": {
        "flavor": "Debian",
        "name": "Debian stretch"
    },
}


def main():
    my_env = environ

    for os in OS_VERSIONS:
        run(["make", "clean"])
        my_env["OS_VERSION"] = os
        run(["make", "package"], env=my_env, check=True)
        client = boto3.client("s3")
        for fi_name in listdir(PKG_DIR):
            if (
                fi_name.endswith(".rpm")
                or fi_name.endswith(".deb")
                or fi_name.endswith(".json")
            ):
                key = "twindb-backup/{version}/{os_version}/{fi_name}".format(
                    version=__version__, os_version=os, fi_name=fi_name
                )
                with open(osp.join(PKG_DIR, fi_name), "rb") as fp:
                    client.put_object(
                        ACL="public-read", Body=fp, Bucket="twindb-release", Key=key,
                    )
                print("https://twindb-release.s3.amazonaws.com/{key}".format(key=key))

    client = boto3.client("s3")
    for flavor in sorted((set([x["flavor"] for x in OS_DETAILS.values()]))):
        print("## %s" % flavor)
        for os, details in OS_DETAILS.items():
            if details["flavor"] == flavor:
                print("  * %s" % details["name"])
                key = "twindb-backup/{version}/{os_version}/".format(
                    version=__version__, os_version=os,
                )
                response = client.list_objects(
                    Bucket='twindb-release',
                    Prefix=key,
                )
                for fil in response["Contents"]:
                    print("    * https://twindb-release.s3.amazonaws.com/%s" % fil["Key"])


if __name__ == "__main__":
    main()
