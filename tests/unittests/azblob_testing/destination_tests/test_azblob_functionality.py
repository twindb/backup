import io
import logging
import os
import sys
import time
import types
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# third-party imports
import coverage
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

# azure imports (also a third-party import) ;)
from azure.storage.blob import BlobClient, BlobProperties, ContainerClient
from azure.storage.blob._shared.response_handlers import (
    PartialBatchErrorException,
)

from tests.unittests.azblob_testing import (
    PART_NAMES,
    SAMPLE_TARGETS,
    do_set_osenvs,
)

# local project imports
from twindb_backup import LOG

DO_TEST_SKIPPING = False


def get_root(path: Path, dir_name: str):
    if path.name and path.name == dir_name:
        return path
    return get_root(path.parent, dir_name)


def handle_coverage():
    root = get_root(Path(__file__).parent, "backup")

    @contextmanager
    def cover_ctx():
        cov = coverage.Coverage(data_file=str(root.joinpath("cov/.coverage")))
        cov.start()
        try:
            yield
        finally:
            cov.stop()
            cov.save()
            cov.html_report()

    return cover_ctx


test_function_logger = LOG
test_function_logger.setLevel(0)


class AzureBlobBaseCase(unittest.TestCase):
    """No direct tests provided here. This class defines basic setup of testing resources which subclasses will need."""

    @staticmethod
    def _reproduce_potential_import_error(err: ImportError, msg):
        def repeatable_raiser(*args, **kwargs):
            nonlocal err
            try:
                raise ImportError(msg) from err
            except ImportError as ie:
                # creation of trimmed traceback inspired by the stack-overflow answer found here:
                #   https://stackoverflow.com/a/58821552/7412747
                tb = sys.exc_info()[2]
                back_frame = tb.tb_frame.f_back
                back_tb = types.TracebackType(
                    tb_next=None,
                    tb_frame=back_frame,
                    tb_lasti=back_frame.f_lasti,
                    tb_lineno=back_frame.f_lineno,
                )
                immediate_err = ie.with_traceback(back_tb)
                raise immediate_err

        return repeatable_raiser

    # noinspection PyUnresolvedReferences
    @classmethod
    def setUpClass(cls) -> None:
        """Provides a class level function that will only be run one time
        when this TestCase instance is first initialized."""
        try:
            from twindb_backup.destination.azblob import AzureBlob
        except ImportError as ie:
            msg = f"Attempted use of object twindb_backup.destination.azblob.AzureBlob failed due to import error"
            AzureBlob = cls._reproduce_potential_import_error(ie, msg)
        try:
            from twindb_backup.destination.azblob import logger

            # during testing it would be nice to see all console log output (if any).
            logger.setLevel(0)
        except ImportError as ie:
            pass
        if "PRIMARY_TEST_CONN_STR" not in os.environ:
            from tests.unittests.excluded_env_config.build_out_dummy_env import (
                set_osenvs,
            )

            logging.getLogger(
                "azure.core.pipeline.policies.http_logging_policy"
            ).setLevel(logging.WARNING)
            do_set_osenvs(set_osenvs)
        conn_str = os.environ["PRIMARY_TEST_CONN_STR"]
        conn_str_split = conn_str.split(";")
        conn_str_parts = {}
        for part in conn_str_split:
            try:
                k, v = [s for s in part.split("=", 1)]
                conn_str_parts[k] = v
            except ValueError as ve:
                obj = [v for v in part.split("=", 1)]
                k, v = obj
                ve.args += part, obj, len(obj), k, v
                raise ve
        # conn_str_parts = {k:v for part in conn_str.split(";") for k,v in part.split("=",1)}
        cls._connection_string = conn_str
        cls._remote_protocol = conn_str_parts["DefaultEndpointsProtocol"]
        remote_host = (
            cls._remote_host
        ) = f'{conn_str_parts["AccountName"]}.blob.{conn_str_parts["EndpointSuffix"]}'
        cls._remote_path_template = os.environ[
            "TEST_COMPLETE_REMOTE_PATH_TEMPLATE"
        ]
        cls._AzureBlob = AzureBlob
        sample_resources = Path(__file__).resolve().parent
        while not any(
            "sample_resources" in str(p) for p in sample_resources.iterdir()
        ):
            sample_resources = sample_resources.parent
        cls._sample_resource_folder = sample_resources.joinpath(
            "sample_resources"
        )
        sample_targets = cls._sample_targets = SAMPLE_TARGETS
        for i in range(len(sample_targets)):
            sample_targets[i] = sample_targets[i].format(host=remote_host)
        cls._part_names = PART_NAMES
        cls._arg_names = [
            "default_protocol",
            "default_host_name",
            "default_container_name",
            "default_interval",
            "default_media_type",
            "default_fname_prefix",
        ]
        cls._blank_parts = {
            "default_protocol": "",
            "default_host_name": "",
            "default_container_name": "",
            "default_interval": "",
            "default_media_type": "",
            "default_fname_prefix": "",
        }
        cls._none_parts = {
            "default_protocol": None,
            "default_host_name": None,
            "default_container_name": None,
            "default_interval": None,
            "default_media_type": None,
            "default_fname_prefix": None,
        }
        cls._basic_src_dst_kwargs = {
            "protocol": "https",
            "host_name": f"{remote_host}",
            "container_name": "{default_container_name}",
            "interval": "{default_interval}",
            "media_type": "mysql",
            "fname_prefix": "{default_fname_prefix}",
        }
        cls._container_names: Optional[str] = None
        # For clarification on the following class members and their structure,
        # see their associated properties defined below.
        cls._basic_remote_dest_path: Optional[str] = None
        cls._basic_remote_source_path: Optional[str] = None
        cls._complex_invalid_remote_paths: Optional[
            List[Tuple[str, Dict[str, str], Dict[str, str]]]
        ] = None
        cls._complex_valid_remote_paths: Optional[
            Dict[str, List[Tuple[str, Dict[str, str], Dict[str, str]]]]
        ] = None
        cls._easy_container_names_map: Optional[Dict[str, str]] = None
        cls._simple_valid_remote_paths: Optional[
            List[Tuple[str, Dict[str, str], Dict[str, str]]]
        ] = None
        cls._simple_valid_save_source_paths: Optional[List[str]] = None
        cls._structured_parts: Optional[Dict[str, Dict[str, str]]] = None
        cls._unique_backup_locations: Optional[Tuple[str]] = None

    @property
    def AzureBlob(self):
        return self._AzureBlob

    @property
    def basic_remote_source_path(self):
        if not self._basic_remote_source_path:
            self._basic_remote_source_path = self._remote_path_template[
                : -len("{fname}")
            ].format(**self._basic_src_dst_kwargs)
        return self._basic_remote_source_path

    @property
    def basic_remote_dest_path(self):
        if not self._basic_remote_dest_path:
            self._basic_remote_dest_path = self._remote_path_template[
                : -len("{fname}")
            ].format(**self._basic_src_dst_kwargs)
        return self._basic_remote_dest_path

    @property
    def complex_valid_remote_paths(self):
        if not self._complex_invalid_remote_paths:
            # create mutable_parts as a deep copy of structured_parts
            mutable_parts = {
                k: {kk: vv for kk, vv in v.items()}
                for k, v in self.structured_parts.items()
            }
            path_template = "{default_protocol}://{default_host_name}/{default_container_name}/{default_interval}/{default_media_type}/{default_fname_prefix}"
            self._complex_valid_remote_paths = {
                "sub_all": [
                    (
                        "",
                        {k: v for k, v in mutable_parts[name].items()},
                        self.structured_parts[name],
                    )
                    for name in mutable_parts
                ]
            }
            split_point = len("default_")
            # build out a suite of test inputs that have part-wise substitution changes marked
            for part in self._arg_names:
                # marks the part to flag for substitution
                [
                    mutable_parts[name].update({part: "..."})
                    for name in mutable_parts
                ]
                _part = part[split_point:]
                sub_part = f"sub_{_part}"
                self._complex_valid_remote_paths[sub_part] = [
                    (
                        path_template.format(**args_d),
                        {part: self.structured_parts[name][part]},
                        self.structured_parts[name],
                    )
                    for name, args_d in mutable_parts.items()
                ]
                # reset the flagged part with its original value in preparation for next loop.
                [
                    mutable_parts[name].update(
                        {part: self.structured_parts[name][part]}
                    )
                    for name in mutable_parts
                ]
        return self._complex_valid_remote_paths

    @property
    def complex_invalid_remote_paths(self):
        if not self._complex_invalid_remote_paths:
            blank_parts = self._blank_parts
            none_parts = self._none_parts
            self._complex_invalid_remote_paths = [
                # (f"azure://{cls._remote_host}/barney-of-buffalo-lodge/hourly/mysql/",{}),
                ("../../../hourly/mysql/", {}),
                ("../../../hourly/mysql/", blank_parts),
                ("../../../hourly/mysql/", none_parts),
                (
                    f"../../../https://{self._remote_host}/{self._structured_parts['wilma']['default_container_name']}/hourly/mysql/",
                    {},
                ),
                (
                    f"../../../https://{self._remote_host}/{self._structured_parts['wilma']['default_container_name']}/hourly/mysql/",
                    blank_parts,
                ),
                (
                    f"../../../https://{self._remote_host}/{self._structured_parts['wilma']['default_container_name']}/hourly/mysql/",
                    none_parts,
                ),
                # (f"https://{cls._remote_host}/wilma-of-impossibly-good-figure/daily/mysql/",{}),
                (f"https://{self._remote_host}/.../daily/mysql/", {}),
                (f"https://{self._remote_host}/.../daily/mysql/", blank_parts),
                (f"https://{self._remote_host}/.../daily/mysql/", none_parts),
                # (f"azure://{cls._remote_host}/betty-of-impossibly-good-figure/weekly/mysql/",{}),
                (f"azure://{self._remote_host}/.../", {}),
                (f"azure://{self._remote_host}/.../", blank_parts),
                (f"azure://{self._remote_host}/.../", none_parts),
                # (f"https://{cls._remote_host}/fred-of-buffalo-lodge/monthly/mysql/",{}),
                (f"https://{self._remote_host}/", {}),
                (f"https://{self._remote_host}/", blank_parts),
                (f"https://{self._remote_host}/", none_parts),
            ]
        return self._complex_invalid_remote_paths

    @property
    def connection_string(self):
        return self._connection_string

    @property
    def container_names(self):
        if not self._container_names:
            self._container_names = os.environ["TEST_CONTAINER_NAMES"].split(
                ";"
            )
            self._container_names.extend(
                "save-function-test,write-function-test,delete-function-test,combo-all-flintstones".split(
                    ","
                )
            )
        return self._container_names

    @property
    def easy_container_names(self):
        if not self._easy_container_names_map:
            self._easy_container_names_map = {
                v.split("-")[0]: v for v in self.container_names
            }
        return self._easy_container_names_map

    @property
    def part_names(self):
        return self._part_names

    @property
    def remote_path_template(self):
        return self._remote_path_template

    @property
    def sample_targets(self):
        return self._sample_targets

    @property
    def simple_valid_remote_paths(self):
        if not self._simple_valid_remote_paths:
            none_parts = self._none_parts
            blank_parts = self._blank_parts
            remote_host = self._remote_host
            self._simple_valid_remote_paths = [
                (
                    f"https://{remote_host}/barney-of-buffalo-lodge/hourly/mysql/backup/",
                    {},
                    {},
                ),
                (
                    f"https://{remote_host}/barney-of-buffalo-lodge/hourly/mysql/backup/",
                    blank_parts,
                    {},
                ),
                (
                    f"https://{remote_host}/barney-of-buffalo-lodge/hourly/mysql/backup/",
                    none_parts,
                    {},
                ),
                (
                    f"https://{remote_host}/barney-of-buffalo-lodge/hourly/mysql/backup/",
                    self.structured_parts["barney"],
                    {},
                ),
                (
                    f"https://{remote_host}/wilma-of-impossibly-good-figure/daily/mysql/backup/",
                    {},
                    {},
                ),
                (
                    f"https://{remote_host}/wilma-of-impossibly-good-figure/daily/mysql/backup/",
                    blank_parts,
                    {},
                ),
                (
                    f"https://{remote_host}/wilma-of-impossibly-good-figure/daily/mysql/backup/",
                    none_parts,
                    {},
                ),
                (
                    f"https://{remote_host}/wilma-of-impossibly-good-figure/daily/mysql/backup/",
                    self.structured_parts["wilma"],
                    {},
                ),
                (
                    f"https://{remote_host}/betty-of-impossibly-good-figure/weekly/mysql/backup/",
                    {},
                    {},
                ),
                (
                    f"https://{remote_host}/betty-of-impossibly-good-figure/weekly/mysql/backup/",
                    blank_parts,
                    {},
                ),
                (
                    f"https://{remote_host}/betty-of-impossibly-good-figure/weekly/mysql/backup/",
                    none_parts,
                    {},
                ),
                (
                    f"https://{remote_host}/betty-of-impossibly-good-figure/weekly/mysql/backup/",
                    self.structured_parts["betty"],
                    {},
                ),
                (
                    f"https://{remote_host}/fred-of-buffalo-lodge/monthly/mysql/backup/",
                    {},
                    {},
                ),
                (
                    f"https://{remote_host}/fred-of-buffalo-lodge/monthly/mysql/backup/",
                    blank_parts,
                    {},
                ),
                (
                    f"https://{remote_host}/fred-of-buffalo-lodge/monthly/mysql/backup/",
                    none_parts,
                    {},
                ),
                (
                    f"https://{remote_host}/fred-of-buffalo-lodge/monthly/mysql/backup/",
                    self.structured_parts["fred"],
                    {},
                ),
            ]
            for path, kwargs, out in self._simple_valid_remote_paths:
                self._get_remote_parts(path, kwargs, out)
        return self._simple_valid_remote_paths

    @property
    def simple_valid_save_source_paths(self):
        if not self._simple_valid_save_source_paths:
            save_trunkate_len = len("backup/")
            self._simple_valid_save_source_paths = [
                p[:-save_trunkate_len] for p in self.unique_backup_locations
            ]
        return self._simple_valid_save_source_paths

    @property
    def structured_parts(self):
        if not self._structured_parts:
            remote_host = self._remote_host
            self._structured_parts = {
                "barney": {
                    "default_protocol": "https",
                    "default_host_name": f"{remote_host}",
                    "default_container_name": "barney-of-buffalo-lodge",
                    "default_interval": "hourly",
                    "default_media_type": "mysql",
                    "default_fname_prefix": "",
                },
                "betty": {
                    "default_protocol": "https",
                    "default_host_name": f"{remote_host}",
                    "default_container_name": "betty-of-impossibly-good-figure",
                    "default_interval": "weekly",
                    "default_media_type": "mysql",
                    "default_fname_prefix": "",
                },
                "wilma": {
                    "default_protocol": "https",
                    "default_host_name": f"{remote_host}",
                    "default_container_name": "wilma-of-impossibly-good-figure",
                    "default_interval": "daily",
                    "default_media_type": "mysql",
                    "default_fname_prefix": "",
                },
                "fred": {
                    "default_protocol": "https",
                    "default_host_name": f"{remote_host}",
                    "default_container_name": "fred-of-buffalo-lodge",
                    "default_interval": "monthly",
                    "default_media_type": "mysql",
                    "default_fname_prefix": "",
                },
            }
        return self._structured_parts

    @property
    def unique_backup_locations(self):
        if not self._unique_backup_locations:
            self._unique_backup_locations = tuple(
                set(p for p, _, _ in self._simple_valid_remote_paths)
            )
        return self._unique_backup_locations

    @staticmethod
    def _get_remote_parts(path: str, kwargs: dict, out: dict):
        """
        "default_protocol"
        "default_host_name"
        "default_container_name"
        "default_interval"
        "default_media_type"
        "default_fname_prefix"

        :param path:
        :type path:
        :param kwargs:
        :type kwargs:
        :param out:
        :type out:
        :return:
        :rtype:
        """
        path = path.rstrip("/")
        _path = path
        part_names = [
            "default_host_name",
            "default_container_name",
            "default_interval",
            "default_media_type",
            "default_fname_prefix",
        ]
        if path:
            protocol, sep, path = path.partition("://")
            if not path:
                path = protocol
                protocol = ""
            out["default_protocol"] = protocol or kwargs.get(
                "default_protocol", ""
            )
            for name in part_names[:-1]:
                if not path:
                    break
                part, _, path = path.partition("/")
                if not path:
                    path = part
                    part = ""
                kpart = kwargs.get(name, "")
                out[name] = part or kpart
            else:
                name = part_names[-1]
                part, _, path = path.partition("/")
                kpart = kwargs.get(name, "")
                out[name] = part or kpart
        else:
            out.update(kwargs)

    def _cleanup_remote(self):
        delete_count = 0
        for kwargs in self.structured_parts.values():
            remote = self.AzureBlob(
                self.basic_remote_source_path.format(
                    **{k: v.strip(":/") for k, v in kwargs.items()}
                ),
                self.connection_string,
            )
            delete_targets = [
                f
                for f in remote.list_files()
                if any(s in f for s in ("backup", "delete"))
            ]
            if not delete_targets:
                continue
            parts = [
                f.partition("://")[2].split("/")[1:] for f in delete_targets
            ]
            containers = [fparts[0] for fparts in parts]
            full_fnames = ["/".join(fparts[1:]) for fparts in parts]
            container_map = {}
            for cont, fname in zip(containers, full_fnames):
                container_map.setdefault(cont, []).append(fname)
            containers = tuple(container_map.keys())
            with remote.connection_manager(containers) as cmanager:
                cclients: list[ContainerClient] = cmanager.client
                for cclient in cclients:
                    targets = container_map[cclient.container_name]
                    cclient.delete_blobs(*targets)
                    delete_count += len(targets)
        return delete_count


class TC_000_ImportsTestCase(unittest.TestCase):
    def test_00_successful_imports(self):
        from twindb_backup.destination.azblob import AzureBlob

    def test_01_correct_os_environs(self):
        from tests.unittests.excluded_env_config.build_out_dummy_env import (
            set_osenvs,
        )

        do_set_osenvs(set_osenvs)


class TC_001_AzureBlobInstantiationTestCase(AzureBlobBaseCase):
    def test_01_complex_valid_remote_paths(self) -> None:
        expected: dict
        for sub_type, sub_args in self.complex_valid_remote_paths.items():
            for remote_path, kwargs, expected in sub_args:
                dest = self.AzureBlob(
                    remote_path, self.connection_string, **kwargs
                )
                attr: str
                expected_val: str
                for attr, expected_val in expected.items():
                    produced_val = getattr(dest, attr)
                    expected_val = expected_val.strip(":/")
                    with self.subTest(
                        objective="checks if dest's computed properties match expectations, where dest is an instance of the twindb_backup.destinations.azblob.AzureBlob class",
                        sub_type=sub_type,
                        remote_path=remote_path,
                        kwargs=kwargs,
                        expected=expected,
                        attr=attr,
                        produced_val=produced_val,
                        expected_val=expected_val,
                    ):
                        self.assertEqual(
                            produced_val,
                            expected_val,
                            msg=(
                                f"\n\t{sub_type=}"
                                f"\n\t{remote_path=}"
                                f"\n\t{kwargs=}"
                                f"\n\t{expected=}"
                                f"\n\t{attr=}"
                                f"\n\t{produced_val=}"
                                f"\n\t{expected_val=}"
                            ),
                        )

    def test_00_simple_valid_remote_paths(self) -> None:
        expected: dict
        for remote_path, kwargs, expected in self.simple_valid_remote_paths:
            dest = self.AzureBlob(remote_path, self.connection_string, **kwargs)
            attr: str
            val: str
            for attr, val in expected.items():
                with self.subTest(
                    objective="checks if dest's computed properties match expectations, where dest is an instance of the twindb_backup.destinations.azblob.AzureBlob class",
                    remote_path=remote_path,
                    kwargs=kwargs,
                    expected=expected,
                    attr=attr,
                    expected_val=val,
                ):
                    self.assertEqual(getattr(dest, attr), val)


class TC_002_ListFilesTestCase(AzureBlobBaseCase):
    """Tests an AzureBlob class instance's ability to produce a valid list of files when
    given a relative path to some file or directory root in the same storage account as
    its connection-string is associated with.

    When given an invalid path, that is incorrectly configured or asking for a file name
    that doesn't exist, the correct behavior should be to return an empty list, and not
    raise any errors.

    """

    def setUp(self) -> None:
        kwargs = self.structured_parts["fred"]
        remote_path = self.basic_remote_source_path.format(
            **{k: v.strip(":/") for k, v in kwargs.items()}
        )
        self.remote_source = self.AzureBlob(remote_path, self.connection_string)
        self.expected = {}
        for parent, parts_dict in self.structured_parts.items():
            self.expected[parent] = []
            container = parts_dict["default_container_name"]
            fnames = os.environ[
                container.strip("/").replace("-", "_").upper()
            ].split(";")
            path = "{default_protocol}://{default_host_name}/{default_container_name}/{{fname}}".format(
                **parts_dict
            )
            for fname in fnames:
                self.expected[parent].append(path.format(fname=fname))
        non_suspend_brk = 0

    def test_00_list_files_recursive_no_args(self):
        retrieved = [
            f
            for f in self.remote_source.list_files(
                prefix=self.remote_source.default_container_name,
                recursive=True,
                files_only=True,
            )
            if not f.endswith("sticker.png")
        ]
        expected = [
            name for cname, names in self.expected.items() for name in names
        ]
        for retrieved_f in retrieved:
            path_f = Path(retrieved_f)
            with self.subTest(
                objective="confirm that retrieved_f is among our expected files list.",
                retrieved_f=retrieved_f,
                expected=expected,
            ):
                self.assertIn(
                    retrieved_f, expected, f"\n\t{retrieved_f=}\n\t{expected=}"
                )

    @unittest.skipUnless(
        not DO_TEST_SKIPPING, "slow test case, skipping for now"
    )
    def test_01_list_files_prefixed(self):
        dest = self.remote_source
        # prefix:str=None, recursive=False, files_only=False
        pref_expected = [
            (".../", 6),
            ("...", 6),
            (".../hourly", 0),
            (".../monthly", 6),
            (".../monthly/mysql", 6),
            (".../monthly/mysql/does_not_exist", 0),
            (".../hourly/mysql", 0),
            (".../.../does_not_exist", 0),
            (".../hourly/does_not_exist", 0),
            (".../monthly/does_not_exist", 0),
            (".../.../mysql", 6),
            ("barney-of-buffalo-lodge", 6),
            ("barney*/", 6),
            ("barney-of-buffalo-lodge/hourly/mysql", 6),
        ]
        tf_patterns = [
            (False, False),
            (True, False),
            (True, True),
            (False, True),
        ]
        testable_prefixes = [
            (
                dict(prefix=prefix, recursive=recursive, files_only=files_only),
                expected_res_len,
            )
            for prefix, expected_res_len in pref_expected
            for recursive, files_only in tf_patterns
        ]
        for _kwargs, expected_ret_len in testable_prefixes:
            retrieved = dest.list_files(**_kwargs)
            ret_str = "\n\t\t".join(retrieved)
            ret_len = len(retrieved)
            kwarg_str = "\n\t\t".join(f"{k}: {v}" for k, v in _kwargs.items())
            failure_msg = (
                f"A prefix of {_kwargs} should result in {expected_ret_len}, actual retrieval got {ret_len}, files found."
                f"\n\t{dest.default_protocol=}"
                f"\n\t{dest.default_host_name=}"
                f"\n\t{dest.default_container_name=}"
                f"\n\t{dest.default_interval=}"
                f"\n\t{dest.default_media_type=}"
                f"\n\t{dest.default_fname_prefix=}"
                f"\n\tkwargs=\n\t\t{kwarg_str}"
                f"\n\tretrieved=\n\t\t{ret_str}"
            )
            with self.subTest(
                objective="ensure that the number of returned files for given prefixes matches expectations",
                ret_len=ret_len,
                expected_ret_len=expected_ret_len,
                _kwargs=_kwargs,
            ):
                self.assertEqual(ret_len, expected_ret_len, failure_msg)


class TC_003_ReadTestCase(AzureBlobBaseCase):
    def setUp(self) -> None:
        kwargs = self.structured_parts["fred"]
        src_path = self.basic_remote_source_path.format(**kwargs)
        self.remote_source = self.AzureBlob(src_path, self.connection_string)
        self.local_copy_location = self._sample_resource_folder.joinpath(
            "remote_example"
        )
        container_paths = tuple(self.local_copy_location.iterdir())
        flist = []
        expected_data = {}
        for cpath in container_paths:
            ref = expected_data.setdefault(cpath.name, {})
            for bpath in cpath.rglob("**/*.txt"):
                ref[bpath.name] = bpath.read_text()
                flist.append(bpath)
        smallest_file = min(
            filter(
                lambda p: self.remote_source.default_container_name in p.parts,
                flist,
            ),
            key=lambda p: p.stat().st_size,
        )
        self.smallest_file = (
            str(smallest_file)
            .split(self.remote_source.default_container_name)[1]
            .lstrip("/")
        )
        self.expected_data = expected_data

    def test_read(self):
        targets = tuple(
            filter(
                lambda s: self.smallest_file in s,
                self.remote_source.list_files(),
            )
        )
        containers = tuple(self.expected_data.keys())
        for f in targets:
            _, _, path = f.partition("://")
            parts = path.split("/")
            container = parts[1]
            if container in containers and "likes.dinosaurs.txt" in f:
                test_function_logger.debug(
                    f"Running test on:"
                    f"\n\ttarget={f}\n\tas_bytes={False}\n\tcontainer={container}\n\tfname={parts[-1]}"
                )
                with self.subTest(
                    objective="evaluate if data read from remote blob correctly matches the seed data stored locally.",
                    container=container,
                    fname=parts[-1],
                    target_file=f,
                ):
                    data = self.remote_source.read(f)
                    expected = self.expected_data[container][parts[-1]]
                    data = data.decode("utf-8")
                    self.assertEqual(len(data), len(expected))
                    self.assertMultiLineEqual(data, expected)


class TC_004_DeleteTestCase(AzureBlobBaseCase):
    def setUp(self) -> None:
        """
        Creates a temporary container (named delete-function-test) in the configured Azure blob storage endpoint,
        and populates it with files copied from the "wilma-of-impossible-figure" sample container.
        This container and its contents will be cleaned up at the end of each test function in this test-case.
        """
        dst_container = self.test_container = self.easy_container_names[
            "delete"
        ]
        src_container = self.easy_container_names["wilma"]
        kwargs = {
            k: (v if "prefix" not in k else "").strip(":/")
            for k, v in self.structured_parts["wilma"].items()
        }
        src_path = self.basic_remote_source_path.format(**kwargs)
        kwargs["default_container_name"] = dst_container
        dst_path = self.basic_remote_dest_path.format(**kwargs)
        src = self.azure_source = self.AzureBlob(
            src_path, self.connection_string, False
        )
        dest = self.azure_del_target = self.AzureBlob(
            dst_path, self.connection_string, True
        )
        blob_names = [
            p.split(src_container)[1][1:] for p in src.list_files(src_container)
        ]
        self.participating_files = []
        with dest.connection_manager(dest.default_container_name) as cont_iter:
            iter_type = next(cont_iter)
            if iter_type != "ContainerClient":
                from twindb_backup.destination.azblob import (
                    AzureClientManagerError,
                )

                raise AzureClientManagerError(
                    "Failed to get the right type of blob iterator"
                )
            dst_client: ContainerClient = next(cont_iter)
            with src.connection_manager(
                src.default_container_name, blob=blob_names
            ) as client_iterator:
                iter_type = next(client_iterator)
                if iter_type != "BlobClient":
                    from twindb_backup.destination.azblob import (
                        AzureClientManagerError,
                    )

                    raise AzureClientManagerError(
                        "Failed to get the right type of blob iterator"
                    )
                copy_polls = []
                for src_bclient in client_iterator:
                    src_bclient: BlobClient
                    bname = src_bclient.blob_name
                    src_url = src_bclient.url
                    dst_bclient: BlobClient = dst_client.get_blob_client(bname)
                    self.participating_files.append(
                        (bname, src_url, dst_bclient.url)
                    )
                    copy_polls.append(dst_bclient.start_copy_from_url(src_url))
                    tries = 0
                    while copy_polls and tries < 100:
                        for i in range(len(copy_polls) - 1, -1, -1):
                            if copy_polls[i]["copy_status"] == "success":
                                copy_polls.pop(i)
                        tries += 1

    def tearDown(self) -> None:
        with self.azure_source.connection_manager(
            self.test_container
        ) as cont_iter:
            iter_type = next(cont_iter)
            if iter_type != "ContainerClient":
                from twindb_backup.destination.azblob import (
                    AzureClientManagerError,
                )

                raise AzureClientManagerError(
                    "Failed to get the right type of blob iterator"
                )
            for client in cont_iter:
                client: ContainerClient
                try:
                    client.delete_blobs(
                        *(tpl[2] for tpl in self.participating_files)
                    )
                except PartialBatchErrorException:
                    pass

    def test_00_delete_one_file(self):
        del_target = self.participating_files[0][2]
        self.azure_del_target.delete(del_target)
        remaining_files = self.azure_del_target.list_files(".../.../.../")
        readable_remaining = [
            f.split(self.test_container)[1] for f in remaining_files
        ]
        with self.subTest(
            objective="ensure that once a file is deleted, it does not a member of the updated list of remaining_files",
            del_target=del_target,
            remaining_files=readable_remaining,
        ):
            self.assertNotIn(del_target, remaining_files)
        for _, _, should_remain in self.participating_files[1:]:
            with self.subTest(
                objective="ensure that files not specified for deletion still remain",
                should_remain=should_remain,
                del_target=del_target,
                remaining_files=readable_remaining,
            ):
                self.assertIn(should_remain, remaining_files)

    @unittest.skipUnless(
        not DO_TEST_SKIPPING, "slow test case, skipping for now"
    )
    def test_01_delete_multiple_files(self):
        del_targets = self.participating_files[1::2]
        remaining_participants = self.participating_files[::2]
        for target in del_targets:
            self.azure_del_target.delete(target[2])
        remaining_files = self.azure_del_target.list_files(".../.../.../")
        readable_remaining = [
            f.split(self.test_container)[1] for f in remaining_files
        ]
        for target in del_targets:
            del_target = target[2]
            with self.subTest(
                del_target=del_target, remaining_files=readable_remaining
            ):
                self.assertNotIn(del_target, remaining_files)
        for _, _, should_remain in remaining_participants:
            with self.subTest(
                should_remain=should_remain, remaining_files=readable_remaining
            ):
                self.assertIn(should_remain, remaining_files)

    @unittest.skipUnless(
        not DO_TEST_SKIPPING, "slow test case, skipping for now"
    )
    def test_02_delete_all(self):
        for bname, src_url, dst_url in self.participating_files:
            self.azure_del_target.delete(dst_url)
        remaining_files = [
            f
            for f in self.azure_del_target.list_files(".../.../.../")
            for p, _, fname in [f.rpartition("/")]
            if fname and fname != "delete-function-test"
        ]
        if remaining_files:
            self.fail(
                f"Failed to delete all files in target container: {remaining_files}"
            )


class TC_005_WriteTestCase(AzureBlobBaseCase):
    """Tests the different ways the `AzureBlob.write(...)` function can be called.

    We are drawing the source data from a single Azure storage subscription and writing the data back to the same
    subscription in a different location. So, before running this set of tests, run the tests in the ReadTestCase class
    to ensure proper source data is being provided to the writer.
    """

    def setUp(self) -> None:
        self.test_container = self.easy_container_names["write"]
        self.src_kwargs = self.structured_parts["barney"]
        self.dst_kwargs = {k: v for k, v in self.src_kwargs.items()}
        self.dst_kwargs["default_container_name"] = self.test_container

        self.local_copy_location = self._sample_resource_folder.joinpath(
            "remote_example"
        )
        container_paths = tuple(
            p
            for p in self.local_copy_location.iterdir()
            if p.name == self.src_kwargs["default_container_name"]
        )
        smallest_file = min(
            (p for c in container_paths for p in c.rglob("**/*.txt")),
            key=lambda p: p.stat().st_size,
        )
        self.smallest_file = (
            str(smallest_file)
            .split(self.src_kwargs["default_container_name"], 1)[1]
            .lstrip("/")
        )

    def test_00_write_generated_data_overwrite_fail(self):
        test_str_content = "This is a simple and small bit of text to write to the destination_tests endpoint"
        dest = self.AzureBlob(
            self.basic_remote_dest_path.format(**self.dst_kwargs),
            self.connection_string,
            can_do_overwrites=False,
        )
        pstr = dest.remote_path + "/overwrite.target.txt"
        with self.subTest(content=test_str_content, path=pstr):
            err = None
            try:
                dest.write(test_str_content, pstr)
            except BaseException as be:
                err = be
            self.assertIsInstance(err, ResourceExistsError)

    def smallest_file_filter(self, file_url: str):
        return self.smallest_file in file_url

    @unittest.skipUnless(
        not DO_TEST_SKIPPING, "slow test case, skipping for now"
    )
    def test_01_write_generated_data_overwrite_ok(self):
        test_str_content = "This is a simple and small bit of text to write to the destination_tests endpoint"
        dest = self.AzureBlob(
            self.basic_remote_dest_path.format(**self.dst_kwargs),
            self.connection_string,
            can_do_overwrites=True,
        )
        pstr = dest.remote_path + "/overwrite.target.txt"
        with self.subTest(content=test_str_content, path=pstr):
            try:
                dest.write(test_str_content, pstr)
            except BaseException as be:
                self.fail(
                    f"Failed to write to target file with exception details:\n\t{type(be)}: {be.args}"
                )

    @unittest.skipUnless(
        not DO_TEST_SKIPPING, "slow test case, skipping for now"
    )
    def test_02_write_from_remote_overwrite_ok(self):

        source = self.AzureBlob(
            self.basic_remote_source_path.format(**self.src_kwargs),
            self.connection_string,
        )
        dest = self.AzureBlob(
            self.basic_remote_dest_path.format(**self.dst_kwargs),
            self.connection_string,
            can_do_overwrites=True,
        )
        src_flist = tuple(
            filter(self.smallest_file_filter, source.list_files(".../"))
        )
        for spath in src_flist:
            parts = spath.partition("://")[2].split("/")
            container = parts[1]
            bname = "/".join(parts[2:])
            with BlobClient.from_connection_string(
                self.connection_string, container, bname
            ) as bclient:
                bclient: BlobClient
                bprops: BlobProperties = bclient.get_blob_properties()
                size = bprops.size
            with source.get_stream(spath) as content:
                parts[1] = dest.default_container_name
                dpath = "/".join(parts)
                with self.subTest(content_len=size, spath=spath, dpath=dpath):
                    try:
                        dest.write(content, dpath)
                    except BaseException as be:
                        self.fail(
                            f"Failed to write to target file with exception details:\n\t{type(be)}: {be.args}"
                        )

    @unittest.skipUnless(
        not DO_TEST_SKIPPING, "slow test case, skipping for now"
    )
    def test_03_write_from_remote_overwrite_fail(self):
        source = self.AzureBlob(
            self.basic_remote_source_path.format(**self.src_kwargs),
            self.connection_string,
        )
        dest = self.AzureBlob(
            self.basic_remote_dest_path.format(**self.dst_kwargs),
            self.connection_string,
            can_do_overwrites=False,
        )
        src_flist = tuple(
            filter(self.smallest_file_filter, source.list_files(".../"))
        )
        for spath in src_flist:
            parts = spath.partition("://")[2].split("/")
            container = parts[1]
            bname = "/".join(parts[2:])
            with BlobClient.from_connection_string(
                self.connection_string, container, bname
            ) as bclient:
                bclient: BlobClient
                bprops: BlobProperties = bclient.get_blob_properties()
                size = bprops.size
            with source.get_stream(spath) as content:
                parts[1] = dest.default_container_name
                dpath = "/".join(parts)
                with self.subTest(content_len=size, spath=spath, dpath=dpath):
                    self.assertRaises(
                        ResourceExistsError, dest.write, content, dpath
                    )


class TC_006_SaveTestCase(AzureBlobBaseCase):
    def setUp(self) -> None:
        remote_dest_target = Path(
            self.basic_remote_dest_path.partition("://")[2]
        )
        dparts = remote_dest_target.parts
        container_names = self.container_names
        container: str
        container, *_ = tuple(
            cont for cont in container_names if "betty" in cont
        )
        self.source_container = container
        self.dest_container = self.easy_container_names["save"]
        fnames = os.environ[container.upper().replace("-", "_")].split(";")
        dparts = dparts[0], self.dest_container, fnames[0].rpartition("/")[0]
        sparts = dparts[0], self.source_container, fnames[0].rpartition("/")[0]
        remote_dest_target = "https://" + "/".join(dparts)
        remote_src_target = "https://" + "/".join(sparts)
        self.remote_dest_target = remote_dest_target
        self.remote_src_target = remote_src_target
        self.dest = self.AzureBlob(remote_dest_target, self.connection_string)
        self.source = self.AzureBlob(remote_src_target, self.connection_string)
        local_copy = self._sample_resource_folder.joinpath("remote_example")
        local_copy = tuple(
            p for p in local_copy.iterdir() if "betty-of" in str(p)
        )[0]
        local_copy = list(local_copy.iterdir())
        while not all(p.suffix and p.suffix == ".txt" for p in local_copy):
            extension = []
            for p in local_copy:
                extension.extend(p.iterdir())
            local_copy = extension
        local_copy = [min(local_copy, key=lambda s: Path(s).stat().st_size)]
        self.local_target_files = local_copy
        # ".../.../.../" tells our destination instance to use its default names for [protocol, host, container]
        remote_blob_names = []
        for p in local_copy:
            rel = ".../.../.../" + str(p).split(self.source_container)[
                1
            ].lstrip("/")
            remote_blob_names.append(rel)
        self.remote_blob_names = remote_blob_names

        # because we are testing our destination with the overwrite parameter set to false, we need to make
        # sure our destination does not already exist.
        with ContainerClient.from_connection_string(
            self.connection_string, self.dest_container
        ) as cclient:
            cclient: ContainerClient
            try:
                cclient.delete_blobs(*cclient.list_blobs())
            except ResourceNotFoundError:
                pass
        self.local_target_files = sorted(
            self.local_target_files, key=lambda p: p.name
        )
        self.remote_blob_names = sorted(
            self.remote_blob_names, key=lambda s: s.rpartition("/")[2]
        )
        self.smallest_file = (
            str(min(self.local_target_files, key=lambda p: p.stat().st_size))
            .split(self.source.default_container_name)[1]
            .strip("/")
        )

    def tearDown(self) -> None:
        cclient: ContainerClient = ContainerClient.from_connection_string(
            self.connection_string, self.dest_container
        )
        try:
            cclient.delete_blobs(*cclient.list_blobs())
        finally:
            cclient.close()

    @unittest.skipUnless(
        not DO_TEST_SKIPPING, "slow test case, skipping for now"
    )
    def test_00_save_from_local_fd(self):
        for local_p, remote_p in zip(
            self.local_target_files, self.remote_blob_names
        ):
            if self.smallest_file not in str(local_p):
                continue
            with open(local_p, "rb") as f:
                expected = f.read()
                f.seek(0)
                try:
                    self.dest.save(f, remote_p)
                except ResourceExistsError:
                    self.fail(
                        "attempting to save to destination that already exists is a known failure condition."
                    )
            results = self.dest.read(remote_p)
            with self.subTest(
                objective="ensure that round-trip data transfer, starting in a local file, does not change or lose the data",
                local_path=local_p,
                remote_path=remote_p,
            ):
                self.assertEqual(
                    results,
                    expected,
                    "We've written from byte file to remote, "
                    "then read the stored contents back into a new bytes object for comparison.",
                )

    @unittest.skipUnless(
        not DO_TEST_SKIPPING, "slow test case, skipping for now"
    )
    def test_01_save_from_remote_stream(self):
        source_file_urls = self.source.list_files(
            ".../.../.../",
            True,
            str(self.local_target_files[0])
            .split(self.source.default_container_name)[1]
            .lstrip("/"),
            True,
        )
        for p in source_file_urls:
            if self.smallest_file not in p:
                continue
            dpath = ".../.../.../" + p.split(
                self.source.default_container_name
            )[1].lstrip("/")
            with self.subTest(
                objective="ensure that round-trip data transfer, starting in a remote blob, does not change or lose the data",
                src_path=p,
                dst_path=dpath,
            ):
                with self.source.get_stream(p) as stream_in:
                    with self.subTest(stream_in=stream_in.fileno()):
                        try:
                            self.dest.save(stream_in, dpath)
                        except BaseException as be:
                            self.fail(
                                f"Failed to save content to destination:\n\t{type(be)}: {be.args}"
                            )


class TC_007_StreamTestCase(AzureBlobBaseCase):
    @unittest.skipUnless(
        not DO_TEST_SKIPPING, "slow test case, skipping for now"
    )
    def test_00_acquire_pipe_per_file(self):
        src_kwargs = self.structured_parts["fred"]
        source = self.AzureBlob(
            self.basic_remote_source_path.format(**src_kwargs),
            self.connection_string,
        )
        sample_content_relative_path = "backup/sample_resources/remote_example/fred-of-buffalo-lodge/monthly/mysql".split(
            "/"
        )
        here = Path(__file__).parent.resolve()
        while here.name and here.name != sample_content_relative_path[0]:
            here = here.parent
        sample_path = here.joinpath("/".join(sample_content_relative_path[1:]))
        expected_total_bytes = 0
        paths = []
        for p in sample_path.iterdir():
            paths.append("/".join(p.parts[-3:]))
            with open(p, "rb") as f:
                f.seek(0, 2)
                expected_total_bytes += f.tell()
        test_function_logger.debug(f"{expected_total_bytes=}")
        expected_type = type(b"blah").__name__
        bytes_recieved = 0
        for p in paths:
            dtypes = set()
            with source.get_stream(
                f".../.../{source.default_container_name}/{p}"
            ) as stream_pipe:
                stream_pipe: io.FileIO
                try:
                    strt = time.perf_counter()
                    while time.perf_counter() - strt < 4:
                        data = stream_pipe.read()
                        data_type = type(data).__name__
                        dtypes.add(data_type)
                        if data:
                            strt = time.perf_counter()
                            bytes_recieved += len(data)
                            test_function_logger.debug(f"{bytes_recieved=}")
                except EOFError:
                    pass
            for dtype in dtypes:
                with self.subTest(
                    objective="Ensure that the data type (bytes/str/int) sent over pipe connection match expectations",
                    expected_output_type=expected_type,
                    actual_output_type=dtype,
                    path=p,
                ):
                    self.assertEqual(dtype, expected_type)
        with self.subTest(
            objective="Ensure that all of the data sent into the pipe is was collected on the other side.",
            expected_total_bytes=expected_total_bytes,
            bytes_recieved=bytes_recieved,
        ):
            self.assertEqual(expected_total_bytes, bytes_recieved)

    @unittest.skipUnless(
        not DO_TEST_SKIPPING, "slow test case, skipping for now"
    )
    def test_01_acquire_pipe_per_container(self):
        src_kwargs = self.structured_parts["fred"]
        source = self.AzureBlob(
            self.basic_remote_source_path.format(**src_kwargs),
            self.connection_string,
        )
        sample_content_relative_path = (
            "backup/sample_resources/remote_example".split("/")
        )
        here = Path(__file__).parent.resolve()
        while here.name and here.name != sample_content_relative_path[0]:
            here = here.parent
        sample_path = here.joinpath("/".join(sample_content_relative_path[1:]))
        expected_total_bytes = 0
        paths = []
        for p in sample_path.rglob(
            f"**/{source.default_container_name}/**/*.txt"
        ):
            paths.append("/".join(p.parts[-3:]))
            with open(p, "rb") as f:
                f.seek(0, 2)
                expected_total_bytes += f.tell()
        test_function_logger.debug(f"{expected_total_bytes=}")
        expected_type = type(b"blah").__name__
        bytes_recieved = 0
        dtypes = set()
        with source.get_stream(f".../.../.../") as stream_pipe:
            stream_pipe: io.FileIO
            try:
                strt = time.perf_counter()
                while time.perf_counter() - strt < 4:
                    data = stream_pipe.read()
                    data_type = type(data).__name__
                    dtypes.add(data_type)
                    if data:
                        strt = time.perf_counter()
                        bytes_recieved += len(data)
                        test_function_logger.debug(f"{bytes_recieved=}")
            except EOFError:
                pass
        for dtype in dtypes:
            with self.subTest(
                objective="Ensure that the data type (bytes/str/int) sent over pipe connection match expectations",
                expected_output_type=expected_type,
                actual_output_type=dtype,
            ):
                self.assertEqual(dtype, expected_type)
        with self.subTest(
            objective="Ensure that no data was mishandled or lost when passed through the pipe.",
            expected_total_bytes=expected_total_bytes,
            bytes_recieved=bytes_recieved,
        ):
            self.assertEqual(expected_total_bytes, bytes_recieved)


def main():
    cover_ctx_manager = handle_coverage()
    with cover_ctx_manager():
        unittest.TextTestRunner().run(
            unittest.TestLoader().loadTestsFromTestCase(TC_000_ImportsTestCase)
        )
    print("done")
    dbg_break = 0


if __name__ == "__main__":
    # main()
    unittest.main(verbosity=2)
