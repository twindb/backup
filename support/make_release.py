from os import listdir, environ
from os import path as osp
from subprocess import run

import boto3

from twindb_backup import __version__

OS_VERSIONS = [
    # # ubuntu
    "jammy",
    "focal",
]
PKG_DIR = "omnibus/pkg"

OS_DETAILS = {
    "jammy": {
        "flavor": "Ubuntu",
        "name": "Ubuntu jammy"
    },
    "focal": {
        "flavor": "Ubuntu",
        "name": "Ubuntu focal"
    },
}


def main():
    my_env = environ

    for os in OS_VERSIONS:
        run(["make", "clean"])
        my_env["OS_VERSION"] = os
        run(["make", "package"], env=my_env, check=True)
        session = boto3.Session(profile_name="twindb")
        client = session.client("s3")
        for fi_name in listdir(PKG_DIR):
            if (
                fi_name.endswith(".rpm")
                or fi_name.endswith(".deb")
                or fi_name.endswith(".json")
            ):
                key = f"twindb-backup/{__version__}/{os}/{fi_name}"
                with open(osp.join(PKG_DIR, fi_name), "rb") as fp:
                    client.put_object(
                        ACL="public-read", Body=fp, Bucket="twindb-release", Key=key,
                    )
                print(f"https://twindb-release.s3.amazonaws.com/{key}")

    client = boto3.client("s3")
    for flavor in sorted((set([x["flavor"] for x in OS_DETAILS.values()]))):
        print("## %s" % flavor)
        for os, details in OS_DETAILS.items():
            if details["flavor"] == flavor:
                print(f"  * {details['name']}")
                key = f"twindb-backup/{__version__}/{os}/"
                response = client.list_objects(
                    Bucket='twindb-release',
                    Prefix=key,
                )
                for fil in response["Contents"]:
                    print(f"    * https://twindb-release.s3.amazonaws.com/{fil['Key']}")


if __name__ == "__main__":
    main()
