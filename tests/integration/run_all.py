from os import environ
from subprocess import run

import click

from twindb_backup import LOG, setup_logging

DOCKER_IMAGES = {
    "focal": "twindb/backup-test:focal",
    "bionic": "twindb/backup-test:bionic",
    "7": "twindb/backup-test:centos-7",
}

SUPPORTED_OS = list(DOCKER_IMAGES.keys())


@click.command()
@click.option(
    "--build/--no-build",
    help="Whether build package for selected OS.",
    is_flag=True,
    default=True,
    show_default=True,
)
@click.option(
    "--pause/--no-pause",
    help="Whether pause a test if  it fails.",
    is_flag=True,
    default=True,
    show_default=True,
)
@click.option(
    "--docker-image",
    help="Use the specified docker image instead of default for OS.",
    default=None,
    show_default=True,
)
@click.option(
    "--debug",
    help="Print debug messages.",
    is_flag=True,
    default=False,
    show_default=True,
)
@click.argument("version", type=click.Choice(SUPPORTED_OS), required=False)
def run_all(build, pause, docker_image, debug, version):
    run_on_versions = [version] if version else SUPPORTED_OS
    setup_logging(LOG, debug=debug)
    for version in run_on_versions:
        env = {
            "OS_VERSION": version,
            "AWS_ACCESS_KEY_ID": environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": environ["AWS_SECRET_ACCESS_KEY"],
            "PATH": environ["PATH"],
            "DOCKER_IMAGE": docker_image or DOCKER_IMAGES[version],
        }
        if pause:
            env["PAUSE_TEST"] = "1"

        if build:
            LOG.info("Building package for OS_VERSION=%s", version)
            try:
                run(
                    ["make", "package"],
                    env=env,
                    check=True,
                )
            finally:
                run(["docker", "rm", "builder_xtrabackup", "--force"])

        LOG.info("Running integration tests for OS_VERSION=%s", version)
        try:
            run(
                ["make", "test-integration"],
                env=env,
                check=True,
            )
        finally:
            LOG.info("Cleaning up containers. If you see Error messages - they're expected.")
            for container in ["master1_1", "slave_2", "runner_3"]:
                run(["docker", "rm", container, "--force"])


if __name__ == "__main__":
    run_all()
