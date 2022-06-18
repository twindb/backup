import concurrent.futures as cf
import gc
import json
import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, List, Union

from azure.storage.blob import (
    BlobClient,
    BlobProperties,
    BlobServiceClient,
    ContainerClient,
    StorageStreamDownloader,
)

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

from tests.unittests.excluded_env_config.dummy_content_generation import (
    cave_gen,
    dino_gen,
    painting_gen,
)
from twindb_backup import LOG

PATH_HERE = Path(__file__).parent

MIN_SAMPLE_SIZE = 2**31
BLOCK_SIZE = 2**24  # 2**24 == 16 MiB
INTERVAL_MAPPING = {
    "pebbles": {
        "wilma": "daily",
        "fred": "monthly",
    },
    "bambam": {
        "barney": "hourly",
        "betty": "weekly",
    },
}
TARGET_FILE_HISTORY = set()
DUMMY_OBJ = object()


def get_local_cache_location():
    here = Path(PATH_HERE)
    relative_dirs = "backup/sample_resources/remote_example".split("/")
    while here.name and relative_dirs[0] != here.name:
        here = here.parent
    return here.parent.joinpath("/".join(relative_dirs))


@contextmanager
def _gc_toggle(depth=0):
    gc.disable()
    try:
        yield depth + 1
    finally:
        if depth == 0:
            gc.enable()


def get_key_manager(keychain: list):
    @contextmanager
    def key_master(val):
        keychain.append(val)
        try:
            yield
        finally:
            keychain.pop()

    return key_master


def content_gen_wrapper(blob_names: dict, for_parent: str, sep):
    """This function depends upon the structure of the input argument blob_names which should be derived from the
    dummy_vals portion of the dummy_env_vas.json file."""

    def part_gen(child, child_blobs):
        def inner(parent, blob_name, size):
            nonlocal content, correct_parents
            try:
                if parent in correct_parents:
                    yield True
                    gen = content[blob_name]
                    yield from gen(size)
                else:
                    yield False
            except BaseException as be:
                be.args += (
                    {
                        "parent": parent,
                        "child": child,
                        "blob_name": blob_name,
                        "size": size,
                    },
                )
                raise be

        correct_parents = parent_map[child]
        gen_map = {"dinosaurs": dino_gen, "caves": cave_gen}
        # content is a precomputed mapping of data generators used by our inner function to simulate human-readable data
        content = {
            blob: gen_map.get(
                blob.rsplit(child + sep, 1)[1].split(sep)[1], painting_gen
            )
            for blob in child_blobs
        }
        return inner

    parent_map = {
        childkey: {*INTERVAL_MAPPING[childkey]} for childkey in INTERVAL_MAPPING
    }
    ret = {
        parent: part_gen(kid, blob_names[kid])
        for kid, parents in parent_map.items()
        for parent in parents
        if parent == for_parent
    }
    return ret


def make_blobs(container_name, fname_template_dict):
    def populate_remote_blob():
        content_map = content_gen_wrapper(children_dict, parent, sep)
        with service_client.get_container_client(container_name) as client:
            client: ContainerClient
            if not client.exists():
                client: ContainerClient = service_client.create_container(
                    container_name
                )
            for child, blobs in children_dict.items():
                # sizes = 2**30,*(block_size for _ in range(len(blobs)-1))
                sizes = 2**27, *(BLOCK_SIZE for _ in range(len(blobs) - 1))
                blob: str
                for blob, size in zip(blobs, sizes):
                    blob = blob.strip()
                    while blob.startswith("/"):
                        blob = blob[1:]
                    person, attitude, item_type, *_ = blob.split(".")
                    with client.get_blob_client(blob) as bclient:
                        bclient: BlobClient
                        cache_file_path = cache_location.joinpath(
                            container_name
                        ).joinpath(blob)
                        cache_file_path.parent.mkdir(
                            parents=True, exist_ok=True
                        )
                        data_gen = content_map[parent](parent, blob, size)
                        if not next(data_gen):
                            continue
                        if not cache_file_path.exists():
                            if bclient.exists():
                                bprop: BlobProperties = (
                                    bclient.get_blob_properties()
                                )
                                _size = bprop.size
                                LOG.debug(
                                    f"staging {_size} byte content by downloading from  {bclient.primary_endpoint}"
                                )
                                with open(cache_file_path, "wb") as f:
                                    dl: StorageStreamDownloader = (
                                        bclient.download_blob()
                                    )
                                    dl.readinto(f)
                            else:
                                LOG.debug(
                                    f"staging {size} byte content before uploading to {bclient.primary_endpoint}"
                                )
                                with open(cache_file_path, "wb") as fd:
                                    fd.writelines(data_gen)
                        else:
                            if not bclient.exists():
                                with open(cache_file_path, "rb") as fd:
                                    fd.seek(
                                        0, 2
                                    )  # seeks to the end of the file
                                    size = (
                                        fd.tell()
                                    )  # gets the fd's position which should be the end length of the file
                                    fd.seek(
                                        0, 0
                                    )  # seek back to teh start of the file before we start trying to read
                                    LOG.debug(
                                        f"uploading {size} byte content to {bclient.primary_endpoint}"
                                    )
                                    bclient.upload_blob(data=fd, length=size)
                                    LOG.debug(
                                        f"{fd.tell()} byte content uploaded to {bclient.primary_endpoint}"
                                    )

    blob, person = "", ""
    try:
        cache_location = get_local_cache_location()
        with BlobServiceClient.from_connection_string(
            os.environ["PRIMARY_TEST_CONN_STR"]
        ) as service_client:
            service_client: BlobServiceClient
            children_dict, sep = assemble_blob_names(fname_template_dict)
            parent = container_name.split("-")[0]
            kid_keys = list(children_dict.keys())
            nested_blob_paths = []
            for child in kid_keys:
                child_intervals = INTERVAL_MAPPING[child]
                if parent in child_intervals:
                    prefix = fname_template_dict[
                        "optional_directory_prefix"
                    ].format(interval=INTERVAL_MAPPING[child][parent])
                    for i, blob in enumerate(children_dict[child]):
                        blob = "/".join((prefix, blob))
                        nested_blob_paths.append(blob)
                        children_dict[child][i] = blob
                else:
                    children_dict.pop(child)
            populate_remote_blob()
        return container_name, nested_blob_paths
    except BaseException as be:
        be.args += container_name, blob, person
        raise be


def assemble_blob_names(fname_template_dict):
    template_parts = fname_template_dict["template_parts"]
    fname_templates = fname_template_dict["format_string"]
    sep = template_parts["sep"]
    children = template_parts["child"].split("|")
    dispositions = template_parts["disposition"].split("|")
    items_types = template_parts["item_type"].split("|")
    extension = template_parts["extension"]
    fmt_kwargs = dict(sep=sep, extension=extension)
    blob_names = {}
    for child in children:
        ref = blob_names.setdefault(child, [])
        fmt_kwargs["child"] = child
        for disposition in dispositions:
            fmt_kwargs["disposition"] = disposition
            for itype in items_types:
                fmt_kwargs["item_type"] = itype
                for template in fname_templates:
                    ref.append(template.format(**fmt_kwargs))
    return blob_names, sep


def crawler(
    data: dict, target_key: Any = DUMMY_OBJ, target_val: Any = DUMMY_OBJ
):
    """A support function to craw nested container objects searching for the given targets"""

    def do_dict(d: dict):
        nonlocal keys_ctx
        for k, v in d.items():
            with keys_ctx(k):
                if k == target_key:
                    yield tuple(keychain), v
                yield from enter(v)

    def do_sequence(d: Union[list, tuple]):
        nonlocal keys_ctx
        for k, v in enumerate(d):
            with keys_ctx(k):
                if k == target_key:
                    yield tuple(keychain), v
                yield from enter(v)

    def do_value(d):
        nonlocal keys_ctx
        if d == target_val:
            yield tuple(keychain), d

    def enter(d):
        if isinstance(d, dict):
            yield from do_dict(d)
        elif isinstance(d, (list, tuple)):
            yield from do_sequence(d)
        else:
            yield from do_value(d)

    keychain = []
    keys_ctx = get_key_manager(keychain)
    yield from enter(data)


def set_osenvs(
    target_file: str = None, be_silent: bool = True, use_multi_proc: bool = True
):
    def validate_conn_str(connStr):
        try:
            with BlobServiceClient.from_connection_string(connStr) as client:
                client: BlobServiceClient
                container_list = tuple(client.list_containers())
                if not all(
                    any(s == c.name for c in container_list)
                    for s in vars_dict["dummy_vals"]["container_names"]
                ):
                    vars_dict["dummy_vals"]["container_names"] = container_list
            vars_dict["os.environ"]["test_destination"][
                "PRIMARY_TEST_CONN_STR"
            ] = connStr
            return True
        except BaseException as be:
            return False

    if target_file is None:
        target_file = str(PATH_HERE.joinpath("dummy_env_vars.json"))
    if target_file in TARGET_FILE_HISTORY:
        return
    TARGET_FILE_HISTORY.add(target_file)
    filePath = Path(target_file)
    if filePath.exists():
        with open(filePath, "r", encoding="UTF-8") as f:
            vars_dict = json.load(f)
    else:
        with open(filePath.with_suffix(".json.template"), "r") as f:
            vars_dict = json.load(f)
        LOG.info(
            "\nWARNING:\n\tNo connection stored on local machine\n\tfor a guide on how to get your connection string see:\n\t\thttps://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python?tabs=environment-variable-windows#copy-your-credentials-from-the-azure-portal"
        )
        conn_str = input(
            "Please enter a valid connection string for the target account\n::"
        )

        while not validate_conn_str(conn_str):
            conn_str = input(
                f"{conn_str} is not a valid connection string"
                f"\n\tPlease enter a valid connection string for the target account\n"
            )
        print("\nconnection string valid")
        with open(filePath, "w") as f:
            json.dump(vars_dict, f, indent=4)
    for chain, value in tuple(crawler(vars_dict, target_key="comments")):
        ref = vars_dict
        for k in chain[:-1]:
            ref = ref[k]
        ref.pop(chain[-1])
    test_dest_vars: dict = vars_dict["os.environ"]["test_destination"]
    os.environ["PRIMARY_TEST_CONN_STR"]: str = test_dest_vars[
        "PRIMARY_TEST_CONN_STR"
    ]
    os.environ["TEST_INTERVALS"]: str = ";".join(test_dest_vars["INTERVALS"])
    os.environ["TEST_PATH_PARTS"]: str = ";".join(test_dest_vars["PATH_PARTS"])
    os.environ["TEST_COMPLETE_REMOTE_PATH_TEMPLATE"]: str = test_dest_vars[
        "COMPLETE_REMOTE_PATH_TEMPLATE"
    ]
    os.environ["TEST_CONTAINER_NAMES"] = ";".join(
        vars_dict["dummy_vals"]["container_names"]
    )
    populate_remote_containers(vars_dict, be_silent, use_multi_proc)


def populate_remote_containers(
    vars_dict, be_silent: bool, use_multi_proc: bool
):
    dummy_targets = vars_dict["dummy_vals"]
    containers: List[str] = dummy_targets["container_names"]
    fname_template_dict: dict = dummy_targets["fname_template"]
    container: str
    if use_multi_proc:
        with cf.ProcessPoolExecutor(os.cpu_count()) as ppe:
            ftrs = []
            for loop_container in containers:
                # make_blobs(loop_container,fname_template_dict)
                ftrs.append(
                    ppe.submit(make_blobs, loop_container, fname_template_dict)
                )
            for ftr in cf.as_completed(ftrs):
                if ftr.exception():
                    raise ftr.exception()
                else:
                    container, blobs = ftr.result()
                    LOG.debug(f"{container} completed")
                    os.environ[container.replace("-", "_").upper()] = ";".join(
                        blobs
                    )
                    generate_cli_config(container, blobs)
    else:
        for loop_container in containers:
            try:
                container, blobs = make_blobs(
                    loop_container, fname_template_dict
                )
                LOG.debug(f"{container} completed")
                os.environ[container.replace("-", "_").upper()] = ";".join(
                    blobs
                )
                generate_cli_config(container, blobs)
            except BaseException as be:
                LOG.error("{}: {}".format(type(be).__name__, repr(be.args)))
    if not be_silent:
        strings = []
        longest = max(len(k) for k in os.environ)
        for k in os.environ:
            strings.append(f"{k.strip():<{longest}} : {os.environ[k]}")
        LOG.info("\n" + "\n".join(strings))


def generate_cli_config(container: str, blobs: List[str]):
    from configparser import ConfigParser

    from twindb_backup import INTERVALS
    from twindb_backup import SUPPORTED_DESTINATION_TYPES as SDT
    from twindb_backup import SUPPORTED_QUERY_LANGUAGES as SQ
    from twindb_backup import XBSTREAM_BINARY, XTRABACKUP_BINARY
    from twindb_backup.configuration import (
        DEFAULT_CONFIG_FILE_PATH,
        RetentionPolicy,
    )

    cache_location = get_local_cache_location()
    config_root = cache_location.parent.joinpath("configs").resolve()
    os.environ["TEST_CONFIGS_ROOT"] = str(config_root)
    config_file_path = config_root.joinpath(container).joinpath(
        DEFAULT_CONFIG_FILE_PATH.split("/")[-1]
    )
    config_file_path.parent.mkdir(parents=True, exist_ok=True)
    true_interval, media_type, *fname = blobs[0].split("/")
    prefix: str = "/".join(fname[:-1])
    cache_endpoint = cache_location.joinpath("local_store").joinpath(prefix)
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    cache_endpoint.mkdir(parents=True, exist_ok=True)
    # fname:str = fname[-1]
    conn_str = os.environ["PRIMARY_TEST_CONN_STR"]
    conn_parts = {
        k: v for part in conn_str.split(";") for k, v in (part.split("=", 1),)
    }
    protocol = conn_parts["DefaultEndpointsProtocol"]
    host_name = f'{conn_parts["AccountName"]}.{conn_parts["EndpointSuffix"]}'
    path_parts = {
        "protocol": protocol.strip(":/"),
        "host_name": host_name.strip("/"),
        "container_name": container.strip("/"),
        "interval": true_interval.strip("/"),
        "media_type": media_type.strip("/"),
        "fname_prefix": prefix,
        "fname": "",
    }
    sql_config = {
        "mysql_defaults_file": "/root/.my.cnf",
        "full_backup": INTERVALS[1],
        "expire_log_days": 7,
        "xtrabackup_binary": XTRABACKUP_BINARY,
        "xbstream_binary": XBSTREAM_BINARY,
    }
    mock_config = {
        "compression": {
            "program": "pigz",
            "threads": max(1, os.cpu_count() // 2),
            "level": 9,
        },
        "gpg": {"recipient": "", "keyring": "", "secret_keyring": ""},
        "intervals": {
            f"run_{interval}": interval == true_interval
            for interval in INTERVALS
        },
        "destination": {
            "keep_local_path": True,
            "backup_destination": SDT.azure,
        },
        "export": {
            "transport": "datadog",
            "app_key": "some_app_key",
            "api_key": "some_api_key",
        },
        "source": {"backup_dirs": [str(cache_endpoint)], "backup_mysql": True},
        "retention": {
            f"{interval}_copies": count
            for interval, count in RetentionPolicy._field_defaults.items()
        },
        "retention_local": {
            f"{interval}_copies": count
            for interval, count in RetentionPolicy._field_defaults.items()
        },
        SQ.mysql: sql_config,
        SDT.azure: {
            "remote_path": os.environ[
                "TEST_COMPLETE_REMOTE_PATH_TEMPLATE"
            ].format(
                **path_parts
            ),  # remote_path
            "connection_string": f"'{conn_str}'",  # connection_string
            "can_do_overwrites": False,  # can_do_overwrites
            "cpu_cap": os.cpu_count(),  # cpu_cap
            "max_mem_bytes": 2**24,  # max_mem_bytes
            "default_protocol": path_parts["protocol"],  # default_protocol
            "default_host_name": path_parts["host_name"],  # default_host_name
            "default_container_name": path_parts[
                "container_name"
            ],  # default_container_name
            "default_interval": path_parts["interval"],  # default_interval
            "default_media_type": path_parts[
                "media_type"
            ],  # default_media_type
            "default_fname_prefix": path_parts[
                "fname_prefix"
            ],  # default_fname_prefix
        },
    }
    writer = ConfigParser()
    writer.read_dict(mock_config)
    with open(config_file_path, "w") as fd:
        writer.write(fd)


if __name__ == "__main__":
    set_osenvs("dummy_env_vars.json")
