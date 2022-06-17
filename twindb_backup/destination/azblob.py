# -*- coding: utf-8 -*-
"""
Module for Azure-blob destination.
"""
# builtin module imports
import gc
import io
import multiprocessing as mp
import os
import sys
import time
import traceback
from contextlib import contextmanager
from functools import wraps
from multiprocessing.connection import Connection as mpConn
from pathlib import Path
from textwrap import indent
from typing import AnyStr, Callable, Dict, Generator, Iterable, List, Optional, Tuple, Union

# Third party module imports
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import (
    BlobClient,
    BlobProperties,
    BlobServiceClient,
    ContainerClient,
    ContainerProperties,
    StorageStreamDownloader,
)

# project sub-module imports
from twindb_backup import LOG
from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import AzureBlobDestinationError

IterableClientType = Iterable[Union[BlobServiceClient, ContainerClient, BlobClient]]
DEFAULT_AVAILABLE_CPU = os.cpu_count()
GC_TOGGLE_DEPTH = 0
"""GC_TOGGLE_DEPTH is used as a reference counter for managing when the _gc_toggle function should call gc.enable()."""
ONE_MiB = 2 ** 20
MAX_PIPE_CHUNK_BYTES = 8 * ONE_MiB
MAX_SYS_MEM_USE = 512 * ONE_MiB
"""MAX_PIPE_CHUNK_BYTES is a conservatively safe upper bound on the number of bytes we send through
`multiprocessing.connections.Connection` objects.

This boundary will be derived for the current machine's OS at runtime.

Per the official Python 3.9.6 documentation:
::

    send(obj)
        Send an object to the other end of the connection which should be read using recv().

        The object must be picklable. Very large pickles (approximately 32 MiB+, though it depends on the OS)
        may raise a ValueError exception.

For source documentation on send(obj) see:

    https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.Connection.send
"""
NONE_LABEL = "None"
BSC_LABEL = "BlobServiceClient"
CC_LABEL = "ContainerClient"
BC_LABEL = "BlobClient"


class ClientWrapper:
    """The ContainerWrapper class exists to simplify the process of ensuring that a container's name
    is accessible from mixed types of inputs.

    """

    def __init__(self, name: str = None, props: Optional[ContainerProperties] = None) -> None:
        self._name = name or None
        if not self._name and props is not None:
            self._name = props.name

    @property
    def name(self) -> str:
        return self._name


HasNameAttr = Union[ClientWrapper, ContainerProperties]
IterableHasName = Iterable[HasNameAttr]
StrOrHasName = Union[str, HasNameAttr]
IterableStrOrHasName = Iterable[StrOrHasName]


def _assemble_fname(path_dict: dict) -> str:
    interval = path_dict.get("interval", None)
    media = path_dict.get("media_type", None)
    prefix = path_dict.get("fname_prefix", None)
    fname = path_dict.get("fname", None)
    return "/".join((part for part in (interval, media, prefix, fname) if part))


@contextmanager
def _gc_toggle():
    """A context manager that toggles garbage collection off-at-entry and back-on-at-exit.
    :return: A bool value indicating if gc was enabled when this context was entered
    """
    global GC_TOGGLE_DEPTH
    try:
        gc.disable()
        GC_TOGGLE_DEPTH += 1
        yield GC_TOGGLE_DEPTH
        GC_TOGGLE_DEPTH -= 1
    finally:
        if GC_TOGGLE_DEPTH == 0:
            gc.enable()


def _client_name_gen(obj: Union[StrOrHasName, IterableStrOrHasName]) -> str:
    if obj:
        if isinstance(obj, (str, ClientWrapper, BlobProperties, ContainerProperties)):
            obj = (obj,)
        for elem in obj:
            if isinstance(elem, str):
                yield elem
            elif isinstance(elem, (ClientWrapper, BlobProperties, ContainerProperties)):
                yield elem.name
            else:
                yield from _client_name_gen(elem)


def _ensure_containers_exist(conn_str: str, container: Union[StrOrHasName, IterableStrOrHasName]):
    """
    If we have been given a container name (or an iterable of container names) we should ensure they
    exist and are ready to be acted upon before returning them to the caller.
    Otherwise they will encounter the potentially troublesome `ResourceNotFoundError`
      Example of how it becomes troublesome:
        If a caller deletes a container just before calling this function,
        there will be an some indeterminate amount of time while that delete operation is being
        performed that any subsequent operations attempting to create the container will
        raise `ResourceExistsError` and operations that would
        interact with an existing resource will raise `ResourceNotFoundError`.
    """
    gen = _client_name_gen(container)
    delay_max = 10
    delay = .1
    while True:
        unfinished = []
        for cont in gen:
            _client: ContainerClient = ContainerClient.from_connection_string(conn_str, cont)
            try:
                cprop: ContainerProperties = _client.get_container_properties(timeout=2)
                # getting etag confirms container is fully created
                etag = getattr(cprop, "etag", cprop["etag"])
            except ResourceNotFoundError:
                try:
                    cprop: ContainerProperties = _client.create_container(timeout=2)
                    # getting etag confirms container is fully created
                    etag = getattr(cprop, "etag", cprop["etag"])
                except ResourceExistsError:
                    # We are getting both resource existance errors, meaning the container
                    # is likely being deleted and we can't recreate it till that operation
                    # has finished. So, add the container back to our queue and we'll try
                    # again later.
                    unfinished.append(cont)
            finally:
                _client.close()
        if not unfinished:
            break
        gen = _client_name_gen(unfinished)
        # added delay to ensure we don't jackhammer requests to remote service.
        time.sleep(delay)
        delay = min(delay_max, delay+delay)


def flatten_client_iters(clients: List[Union[ContainerClient, List[BlobClient]]]):
    errs: Dict[str, List[Dict[str, str]]] = {}
    for cclient in clients:
        if isinstance(cclient, list):
            for bclient in cclient:
                try:
                    yield bclient
                except BaseException as be:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    be.with_traceback(exc_traceback)
                    errs.setdefault(exc_type, []).append({"original": be, "exc_type": exc_type, "exc_value": exc_value})
        else:
            try:
                yield cclient
            except BaseException as be:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                be.with_traceback(exc_traceback)
                errs.setdefault(exc_type, []).append({"original": be, "exc_type": exc_type, "exc_value": exc_value})
    if errs:
        err = AzureClientManagerError(f"There were {len(errs)} errors while accessing the flattened clients iterable.")
        err.aggregated_traceback = []
        for e, lst in errs.items():
            agg_tb = []
            for args in lst:
                args: dict
                oe: BaseException = args["original"]
                tb = "".join(traceback.format_exception(args["exc_type"], args["exc_value"], oe.__traceback__))
                agg_tb.append(indent(tb, "\t"))
            agg_tb = "\n\n".join(agg_tb)
            agg_tb = f"\n{'=' * 120}\n{agg_tb}{'-' * 120}"
            err.aggregated_traceback.append(agg_tb)
        LOG.exception("\n".join(err.aggregated_traceback), exc_info=err)
        # raise err
        err.err_map = errs
        err.args += (errs,)
        raise err


def client_generator(
    conn_str,
    container: Optional[Union[StrOrHasName, IterableStrOrHasName]] = None,
    prefix: Optional[str] = None,
    blob: Optional[Union[StrOrHasName, IterableStrOrHasName]] = None,
    recurse: bool = False,
) -> Generator[Union[str, BlobServiceClient, ContainerClient, BlobClient], None, None]:
    # forward declared type hints
    bprop: BlobProperties
    cprop: ContainerProperties
    # scope shared state flags
    blobs_yielded = False
    containers_yielded = False
    service_clients_yielded = False

    # a couple of inner functions for handling different client iteration strategies
    def client_iter(container_iterable):
        nonlocal blobs_yielded, containers_yielded
        for c in container_iterable:
            with ContainerClient.from_connection_string(conn_str, c) as container_client:
                container_client: ContainerClient
                if prefix is not None or blob is not None:
                    for bprop in container_client.list_blobs(prefix):
                        bname: str = bprop.name
                        _name = bname.rpartition("/")[2]
                        if check_blob(_name):
                            with container_client.get_blob_client(bprop.name) as blob_client:
                                if not blobs_yielded:
                                    yield BC_LABEL
                                blobs_yielded = True
                                yield blob_client
                elif recurse:
                    for bprop in container_client.list_blobs():
                        with container_client.get_blob_client(bprop.name) as blob_client:
                            if not blobs_yielded:
                                yield BC_LABEL
                            blobs_yielded = True
                            yield blob_client
                else:
                    if not containers_yielded:
                        yield CC_LABEL
                    containers_yielded = True
                    yield container_client
        if not (blobs_yielded or containers_yielded):
            for c in _client_name_gen(container):
                with ContainerClient.from_connection_string(conn_str, c) as container_client:
                    container_client: ContainerClient
                    if recurse:
                        for bprop in container_client.list_blobs():
                            with BlobClient.from_connection_string(
                                conn_str, bprop.container, bprop.name
                            ) as blob_client:
                                if not blobs_yielded:
                                    yield BC_LABEL
                                blobs_yielded = True
                                yield blob_client
                    else:
                        if not containers_yielded:
                            yield CC_LABEL
                        containers_yielded = True
                        yield container_client

    # second of the inner functions for client iteration strategies
    def bsc_iter():
        nonlocal service_clients_yielded, containers_yielded, blobs_yielded
        with BlobServiceClient.from_connection_string(conn_str) as service_client:
            service_client: BlobServiceClient
            if (prefix or blob) and not (blobs_yielded or containers_yielded):
                yield from client_iter(service_client.list_containers())
            elif recurse:
                for c in service_client.list_containers():
                    with service_client.get_container_client(c) as container_client:
                        for b in container_client.list_blobs():
                            with container_client.get_blob_client(b) as blob_client:
                                if not blobs_yielded:
                                    yield BC_LABEL
                                blobs_yielded = True
                                yield blob_client
            if not (blobs_yielded or containers_yielded):
                yield BSC_LABEL
                service_clients_yielded = True
                yield service_client

    # begin context_manager function's logic
    if not prefix:
        if blob:
            prefs = set()
            _blob = []
            for b in _client_name_gen(blob):
                pref, _, bname = b.rpartition("/")
                _blob.append(bname)
                if pref:
                    prefs.add(pref)
                # ToDo: work in logic for handling if there are more than 1 kind of prefix found
            blob = _blob
            try:
                _pref = prefs.pop()
            except KeyError:
                _pref = None  # to ensure it's not an empty string
            prefix = _pref
    if blob:
        blob_set = set(_client_name_gen(blob))
        check_blob = lambda name: name in blob_set
    else:
        blob = None
        check_blob = lambda *args: True
    if container:
        _ensure_containers_exist(conn_str, container)
        yield from client_iter(_client_name_gen(container))
    else:
        yield from bsc_iter()

    if not (blobs_yielded or containers_yielded or service_clients_yielded):
        yield from (NONE_LABEL,)


def _client_ctx_mgr_wrapper(conn_str: str, gen_func: Callable = client_generator) -> contextmanager:
    @contextmanager
    @wraps(gen_func)
    def context_manager(*args, **kwargs):
        nonlocal conn_str, gen_func
        try:
            ret = gen_func(conn_str, *args, **kwargs)
            yield ret
        finally:
            del ret

    return context_manager


def _ensure_str(obj: Union[AnyStr, Union[List[AnyStr], Tuple[AnyStr]]]):
    if obj is None:
        return ""
    if isinstance(obj, (list, tuple)):
        if obj:
            obj = obj[0]
        else:
            return ""
    if isinstance(obj, bytes):
        obj = obj.decode("utf-8")
    return str(obj)


def _ensure_list_of_str(obj: Union[List[AnyStr], AnyStr]) -> List[Union[str, List[str]]]:
    """
    A helper function that allows us to ensure that a given argument parameter is a list of strings.

    This function assumes the given object is one of:
       * list
       * str
       * bytes
    :param obj: A string, bytes object, or a list (or nested list) of string/bytes objects.
    :return: A list (or nested list) of string objects.

    :raises AzurBlobInitError: If the given object is not a str or bytes object, or if it's a list/tuple of
                               non-(str/bytes) objects then a logic error has likely occured somewhere and we should
                               fail execution here.
    """
    if obj is None:
        return []
    if isinstance(obj, (list, tuple)):
        if isinstance(obj, tuple):
            obj = list(obj)
    elif isinstance(obj, (str, bytes)):
        if isinstance(obj, bytes):
            obj = obj.decode("utf-8")
        obj = [obj]
    else:
        raise AzureBlobInitError(f"Our attempted to ensure obj is a list of strings failed,\n\tgiven {obj=}")
    for i, elem in enumerate(obj):
        if isinstance(elem, str):
            continue
        elif isinstance(elem, bytes):
            obj[i] = elem.decode("utf-8")
        elif isinstance(obj, (list, tuple)):
            if isinstance(obj, tuple):
                obj = list(obj)
            for j, elem2 in obj:
                obj[j] = _ensure_list_of_str(elem2)
        else:
            err_msg = (
                "Our attempt to ensure obj is a list of strings failed,"
                f"\n\tgiven: {obj=}"
                f"\n\tfailure occured while ensuring each element of given iterable was a string, at element: obj[{i}]={elem}"
            )
            raise AzureBlobInitError(err_msg)
    return obj


class AzureBlobInitError(AzureBlobDestinationError):
    pass


class AzureBlobPathParseError(AzureBlobDestinationError):
    pass


class AzureBlobReadError(AzureBlobDestinationError):
    blob_path: str = ""
    """The path string which lead to this exception"""
    chunk_byte_range: Tuple[int, int] = -1, -1
    """The [start,end) bytes defining the chunk where this exception occurs (if chunking used) else set to (-1,-1)"""
    container_name: str = ""
    blob_name: str = ""
    blob_properties: BlobProperties = None


class AzureBlobWriteError(AzureBlobDestinationError):
    blob_path: str = ""
    """The path string which lead to this exception"""
    container_name: str = ""
    blob_name: str = ""
    blob_properties: BlobProperties = None
    content_type = None


class AzureBlobClientError(AzureBlobDestinationError):
    container_name: str = ""
    blob_name: str = ""


class AzureClientManagerError(AzureBlobDestinationError):
    err_map: Dict[str, List[Dict[str, str]]]
    aggregated_traceback: List[str]


class AzureClientIterationError(AzureBlobDestinationError):
    pass


class AzureBlob(BaseDestination):
    def __getnewargs__(self):
        """utility function that allows an instance of this class to be pickled"""
        return (
            self.remote_path,
            self.connection_string,
            self.can_overwrite,
            self._cpu_cap,
            self._max_mem_bytes,
            self.default_protocol,
            self.default_host_name,
            self.default_container_name,
            self.default_interval,
            self.default_media_type,
            self.default_fname_prefix,
        )

    def __getstate__(self):
        """utility function that allows an instance of this class to be pickled"""
        return {k: v if k != "_connection_manager" else None for k, v in self.__dict__.items()}

    def __init__(
        self,
        remote_path: AnyStr,
        connection_string: AnyStr,
        can_do_overwrites: bool = False,
        cpu_cap: int = DEFAULT_AVAILABLE_CPU,
        max_mem_bytes: int = MAX_SYS_MEM_USE,
        default_protocol: Optional[AnyStr] = None,
        default_host_name: Optional[AnyStr] = None,
        default_container_name: Optional[AnyStr] = None,
        default_interval: Optional[AnyStr] = None,
        default_media_type: Optional[AnyStr] = None,
        default_fname_prefix: Optional[AnyStr] = None,
    ):
        """
        A subclass of BAseDestination; Allows for streaming a backup stream to an Azure-blob destination.

        Here's the expected general form for the remote path:
            [protocol]://[host_name]/[container_name]/[interval]/[media_type]/[default_prefix]/[optional_fname]

            NOTE:
                Components inside square brackets, E.G.: `[some component]`; are optional as long as they are instead
                defined by their corresponding initializer argument.

        :param remote_path:
            REQUIRED; A string or bytes object;
            Defines the URI (or URL) for where to connect to the backup object.

        :param connection_string:
            REQUIRED; A string or bytes object;
            When the application makes a request to Azure Storage, it must be authorized.
            To authorize a request, add your storage account credentials to the application as a
            connection string.
            See:
                https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal

        :param can_do_overwrites:
            REQUIRED; a boolean value;
            Flags if we should overwrite existing data when given a destination that
            already exists, or if we should fail and raise a `ResourceExistsError`.

        :param default_protocol:
            OPTIONAL; DEFAULT is set from container component of remote_path argument
            A string or bytes object;
            The name of the container in the destination blob storage we should use.
            If undefined, then we assume it is on the given remote_path argument.

        :param default_container_name:
            OPTIONAL; DEFAULT is set from container component of remote_path argument
            A string or bytes object;
            The name of the container in the destination blob storage we should use.
            If undefined, then we assume it is on the given remote_path argument.

        :param default_host_name:
            OPTIONAL; DEFAULT is set from host component of remote_path argument.
            A string or bytes object;
            The name of the host server.
            If undefined, then we assume it is on the given remote_path argument.

        :param default_interval:
            OPTIONAL; DEFAULT to "yearly"
            A string or bytes object;
            If undefined, then we assume it is on the given remote_path argument.

        :param default_media_type:
            OPTIONAL; DEFAULT to "mysql"
            A string or bytes object;
            if undefined, thenw e assume it is on the given remote_path argument.


        """
        path = _ensure_str(remote_path)
        path = path.strip(" /:").rstrip(".")
        parts = self._path2parts(path)
        if not path:
            protocol = default_protocol or ""
            if not protocol.endswith("://"):
                protocol += "://"
            host = default_host_name or ""
            if not host.endswith("/"):
                host += "/"
            container = default_container_name or ""
            if container and not container.endswith("/"):
                container += "/"
            interval = default_interval or ""
            if interval and not interval.endswith("/"):
                interval += "/"
            media_type = default_media_type or ""
            if media_type and not media_type.endswith("/"):
                media_type += "/"
            fname_prefix = default_fname_prefix or ""
            if fname_prefix and not fname_prefix.endswith("/"):
                fname_prefix += "/"
            path = protocol + host + container + interval + media_type + fname_prefix
        super(AzureBlob, self).__init__(path)
        connection_string = _ensure_str(connection_string)
        self._connection_string = connection_string
        self._flag_overwite_on_write = can_do_overwrites
        self._cpu_cap = cpu_cap
        self._max_mem_bytes = max_mem_bytes
        self._max_mem_pipe = min(MAX_PIPE_CHUNK_BYTES, max_mem_bytes)
        default_protocol = _ensure_str(default_protocol or parts[0]).strip(":/")
        default_host_name = _ensure_str(default_host_name or parts[1]).strip(":/")
        default_container_name = _ensure_str(default_container_name or parts[2]).strip(":/")
        default_interval = _ensure_str(default_interval or parts[3]).strip(":/")
        default_media_type = _ensure_str(default_media_type or parts[4]).strip(":/")
        default_fname_prefix = _ensure_str(default_fname_prefix or parts[5]).strip(":/")
        self._protocol = default_protocol
        self._host_name = default_host_name
        self._container_name = default_container_name
        self._interval = default_interval
        self._media_type = default_media_type
        self._fname_prefix = default_fname_prefix
        self._part_names = "protocol,host,container,interval,media_type,fname_prefix,fname".split(",")
        self._parts_list = [
            (name, parts[i] if i < len(parts) and parts[i] else "") for i, name in enumerate(self._part_names)
        ]
        self._default_parts: Dict[str, str] = {k: v if v != "" else None for k, v in self._parts_list}
        self._default_parts["interval"] = self._default_parts["interval"] or "yearly"
        self._default_parts["media_type"] = self._default_parts["media_type"] or "mysql"
        self._part_names = self._part_names[::-1]
        self._connection_manager: Optional[contextmanager] = None

    @property
    def connection_string(self):
        """An Azure specific authentication string
        for accessing the target backup destination host"""
        return self._connection_string

    @property
    def default_protocol(self):
        return self._protocol

    @property
    def default_host_name(self):
        """The default host server name directory that
        we default to if a relative path string omits the reference"""
        return self._host_name

    @property
    def default_container_name(self):
        """The default container (aka bucket) name that
        we default to if a relative path string omits the reference"""
        return self._container_name

    @property
    def default_interval(self):
        """The default backup interval directory that
        we default to if a relative path string omits the reference"""
        return self._interval

    @property
    def default_media_type(self):
        return self._media_type

    @property
    def default_fname_prefix(self):
        return self._fname_prefix

    @property
    def can_overwrite(self):
        return self._flag_overwite_on_write

    @property
    def max_bytes_per_pipe_message(self):
        return self._max_mem_pipe

    @property
    def max_system_memory_usage(self):
        return self._max_mem_bytes

    @property
    def connection_manager(self):
        if self._connection_manager is None:
            self._connection_manager = _client_ctx_mgr_wrapper(self._connection_string, client_generator)
        return self._connection_manager

    @staticmethod
    def _path2parts(path: str, split_fname: bool = False):
        """Breaks a path string into its sub-parts, and produces a tuple of those parts
        that is at least 6 elements long. We will insert None where a part is determined to be missing in order to
        ensure the minimum length of 6 elements."""

        def extract_protocol(_path: str):
            protocol, _, _path = _path.partition("://")
            if not _path:
                if protocol.startswith(".../"):
                    _path = protocol[4:]
                    protocol = "..."
                else:
                    _path = protocol
                    protocol = None
            else:
                protocol = protocol.strip(":/")
            return protocol, *partition_path(_path, 1)

        def partition_path(_path: str, depth: int):
            if not _path:
                if depth < 6:
                    return None, *partition_path(_path, depth + 1)
            elif depth < 5:
                part, _, _path = _path.partition("/")
                return part.strip(":/"), *partition_path(_path, depth + 1)
            elif split_fname:
                prefix, _, fname = _path.rpartition("/")
                return prefix, fname
            return _path.strip(":/"), None

        return extract_protocol(path)

    def _path_parse(self, path: str, split_fname: bool = False):
        """Called in multiple places where we need to decompose a path string in order to access specific parts by name."""
        if not path:
            return self.remote_path, {k: v for k, v in self._default_parts.items()}
        # noinspection PyTupleAssignmentBalance
        protocol, host, container, interval, media, prefix, *fname = self._path2parts(path, split_fname)
        fname: list
        protocol = protocol if protocol and protocol != "..." else self.default_protocol
        host = host if host and host != "..." else self.default_host_name
        container = container if container and container != "..." else self.default_container_name
        if container != self.default_container_name:
            interval = self.default_interval if interval and interval == "..." else interval if interval else ""
            media = self.default_media_type if media and media == "..." else media if media else ""
            prefix = self.default_fname_prefix if prefix and prefix == "..." else prefix if prefix else ""
        else:
            interval = interval if interval and interval != "..." else self.default_interval
            media = media if media and media != "..." else self.default_media_type
            prefix = prefix if prefix and prefix != "..." else self.default_fname_prefix
        if fname:
            _fname = list(fname)
            while _fname:
                fname = _fname.pop()
                if fname:
                    _fname = "/".join(_fname)
                    break
        else:
            # noinspection PyTypeChecker
            fname = None
        parts: str = "/".join((s for s in (host, container, interval, media, prefix, fname) if s))
        relative_depth = 0
        while parts and parts.startswith("../"):
            relative_depth += 1
            _, _, parts = parts.partition("/")
        base_parts = "/".join(tpl[1] for tpl in self._parts_list[1:-relative_depth])
        base_parts += "/" if base_parts else ""
        path = base_parts + parts.lstrip("/")
        _parts = path.split("/", 4)[::-1]
        shorten = len(self._part_names) - 1 - len(_parts)
        _parts2 = [None] * shorten
        _parts2 += _parts
        # noinspection PyTypeChecker
        ret = {k: v for k, v in zip(self._part_names[:-1], _parts2)}
        ret["protocol"] = protocol
        return path, ret

    def delete(self, path: AnyStr):
        """
        Delete object from the destination

        the general form for the path object should conform to the following:
            [azure:/]/[bucket or container name]/[server name]/[update interval]/[query language]/<file name>

            NOTE:   The protocol type (the left-most component of the example above) is technically optional,
                    as it should always be an azure storage type; but if passed we will check to confirm that it is
                    indeed for azure-blob storage, so including it ensures proper sanity checking.

        --  If path defines a new absolute path string then it must contain all parts defined above,
            with the option to omit those components wrapped in square brackets, E.G.:  [some component]

            where:
                [components inside square brackets] => optional
                <objects inside chevrons> => required

            such that:
                optional components that are not provided should be substituted with an ellipsis (the triple period => ...)

            E.G.:
                ...://foo/.../hourly/mysql/bar-that.foos.gz

            Note:
                Where optional path components are omitted, we assume that the context of the called AzureBlob instance
                should be used to fill in the gaps.

        --  If path is given as a relative path string then you may also use the ellipsis as defined for absolute paths,
            with the added option to use `..` for relative directory hierarchy referencing. The one caveat is that

            E.G.:
                    ../../daily/mysql/relative-foo.bar.gz
                or
                    ../../../some_different_host/.../mysql
                    where:
                        The `...` component signals that we wish to use the given default interval this object was
                        initialized with.

        :param path: A string or bytes object;
                     The path to the file (blob) to delete. Can be relative or absolute.
        """
        abs_path, path_dict = self._path_parse(path)
        container = path_dict["container"]
        fname = _assemble_fname(path_dict)
        if fname:
            label = BC_LABEL
            client_type = "blob"
            args = container, fname
        else:
            label = CC_LABEL
            client_type = "container"
            args = container,
        with self.connection_manager(*args) as client_iter:
            iter_type = next(client_iter)
            if iter_type != label:
                raise AzureClientIterationError(
                    f"Failed to properly identify deletion target given {path=}"
                    f"\n\texpected client type of {label} but got {iter_type}"
                )
            to_check = []
            del_call = "delete_" + client_type
            for client in client_iter:
                client: Union[BlobClient, ContainerClient]
                to_check.append(client)
                getattr(client, del_call)()
            for c in to_check:
                delay = .01
                max_delay = 2
                t0 = time.perf_counter()
                while (time.perf_counter() - t0) < 5:
                    try:
                        if client_type == "blob":
                            c: BlobClient
                            try:
                                bprop: BlobProperties = c.get_blob_properties()
                                if bprop.deleted:
                                    break
                            except AttributeError:
                                # when calls to get_blob_properties raises AttributeError, then the blob is no longer available and the deletion was successful
                                break
                        else:
                            c: ContainerClient
                            cprop: ContainerProperties = c.get_container_properties()
                            if cprop.deleted:
                                break
                        time.sleep(delay)
                        delay = min(max_delay, delay+delay)
                    except ResourceNotFoundError:
                        break

    def _blob_ospiper(self, path_parts_dict: Dict[str, str], pout: mpConn, chunk_size: int = None) -> None:
        def err_assembly():
            bad_path = "{protocol}://{parts}".format(
                protocol=self._part_names[0],
                parts="/".join((f"{{{s}}}" for s in self._part_names[1:] if path_parts_dict.get(s, None))),
            ).format(**path_parts_dict)
            return AzureClientIterationError(f"Unable to find downloadable content files on path : {bad_path}")

        # noinspection PyShadowingNames
        def configure_chunking(bsize: int, pipe_chunk_size: int):
            """

            :param bsize: total number of bytes to be downloaded for current blob
            :type bsize: int
            :param pipe_chunk_size: The maximum buffer size of our transfer pipe
            :type pipe_chunk_size: int
            :return: 4-tuple of ints indicating:
                        * the the number of memory chunks
                        * the size of those mem chunks
                        * if the pipe buffer is smaller than max allowed mem usage, then
                          this is the number of pipe chunks needed to fully transfer one
                          of the memory chunks.
                        * the size of the transfer chunks
            :rtype: tuple[int,int,int,int]
            """
            nonlocal self
            if bsize < self.max_system_memory_usage:
                mem_chunk_size = size
                num_mem_chunks = 1
            else:
                mem_chunk_size = self.max_system_memory_usage
                num_mem_chunks = (size + mem_chunk_size - 1) // mem_chunk_size
            if pipe_chunk_size < mem_chunk_size:
                _chunk_size = pipe_chunk_size
                num_chunks = (mem_chunk_size + _chunk_size - 1) // _chunk_size
            else:
                _chunk_size = mem_chunk_size
                num_chunks = 1
            return num_mem_chunks, mem_chunk_size, num_chunks, _chunk_size

        chunk_size = self.max_bytes_per_pipe_message if chunk_size is None else chunk_size
        max_threads = min(32, self._max_mem_bytes)
        with pout:
            with os.fdopen(pout.fileno(), "wb", buffering=chunk_size, closefd=False) as pipe_out:
                container = path_parts_dict.get("container", None)
                fname = path_parts_dict.pop("fname", None)
                prefix = _assemble_fname(path_parts_dict) or None
                with self.connection_manager(container, prefix, fname, recurse=True) as client_iter:
                    iter_type = next(client_iter)
                    if iter_type != BC_LABEL:
                        raise err_assembly()
                    for client in client_iter:
                        client: BlobClient
                        size = client.get_blob_properties().size
                        num_mem_chunks, mem_chunk_size, num_chunks, _chunk_size = configure_chunking(size, chunk_size)
                        with io.BytesIO(b"\x00" * mem_chunk_size) as bio:
                            for i in range(num_mem_chunks):
                                ipos = i * mem_chunk_size
                                dl: StorageStreamDownloader = client.download_blob(ipos, mem_chunk_size, max_concurrency=max_threads)
                                bio.seek(0)
                                bytes_read = dl.readinto(bio)
                                bio.seek(0)

                                for pos in range(0, bytes_read, _chunk_size):
                                    pipe_out.write(bio.read(_chunk_size))
                                rem = bytes_read % _chunk_size
                                if rem:
                                    pipe_out.write(bio.read(rem))

    @contextmanager
    def get_stream(self, copy: Union[str, MySQLCopy]):
        if copy is None:
            copy = self.remote_path
        path = copy.key if isinstance(copy, MySQLCopy) else copy
        _path = Path(path)
        has_fname = "." in _path.name and _path.name != "..."
        path, path_parts_dict = self._path_parse(path, has_fname)
        pipe_in, pipe_out = mp.Pipe(False)
        proc = mp.Process(target=self._blob_ospiper, args=(path_parts_dict, pipe_out))
        try:
            with pipe_in:
                proc.start()
                pipe_out.close()
                with os.fdopen(pipe_in.fileno(), "rb", closefd=False) as file_pipe_in:
                    yield file_pipe_in
        finally:
            # pipe_out.close()
            proc.join()
            proc.close()

    def read(self, filepath: str, bytes_per_chunk: Optional[int] = None) -> bytes:
        """
        Read content from destination at the end of given filepath.

        :param filepath:
                    REQUIRED; a str object;
                    Relative path to destination file that we will read from.
        :type filepath: str

        :param bytes_per_chunk:
                    OPTIONAL; DEFAULT = self.max_bytes_per_pipe_message; an int value;
                    This parameter dictates the max chunk size (in bytes) that should
                    be passed into the pipe for any single chunk.
        :type bytes_per_chunk: int

        :return: Content of the file.
        :rtype: bytes
        """
        with self.get_stream(filepath) as conn:
            conn: io.FileIO
            strt = time.perf_counter()
            datum = []
            while time.perf_counter() - strt < 2:
                try:
                    data = conn.read()
                    if data:
                        datum.append(data)
                        strt = time.perf_counter()
                except EOFError:
                    break
        return b"".join(datum)

    def save(self, handler, filepath):
        """
        Save a stream given as handler to filepath.

        :param handler: Incoming stream.
        :type handler: file
        :param filepath: Save stream as this name.
        :type filepath: str
        """
        with handler as f_src:
            self.write(f_src, filepath)

    def write(self, content: Union[AnyStr, io.BufferedIOBase], filepath: AnyStr):
        """
        Write ``content`` to a file.

        :param content: Content to write to the file.
        :type content: str, bytes, or subclass of BufferedIOBase object
        :param filepath: Relative path to file.
        :type filepath: str or bytes object
        """
        if isinstance(filepath, bytes):
            filepath = filepath.decode("utf-8")
        filepath, _, fname = filepath.rpartition("/")
        path, path_dict = self._path_parse(filepath)
        container = path_dict["container"] or self.default_container_name
        blob_name = _assemble_fname(path_dict)
        with self.connection_manager(container, prefix=blob_name, blob=fname) as client_iter:
            iter_type = next(client_iter)
            if iter_type == CC_LABEL:
                blob_name += "/" + fname
                client: ContainerClient = next(client_iter)
                if isinstance(content, io.BufferedReader):
                    with content:
                        client.upload_blob(blob_name, content.read(), overwrite=self.can_overwrite)
                else:
                    client.upload_blob(blob_name, content, overwrite=self.can_overwrite)
            elif iter_type != BC_LABEL:
                raise AzureClientIterationError(f"Failed to identify path to blob files given: {filepath}")
            else:
                # Unless filepath used wildcards, client_iter is only going to produce
                # a single client instance to upload to.
                bclient: BlobClient = next(client_iter)
                if isinstance(content, io.BufferedReader):
                    with content:
                        bclient.upload_blob(content.read(), overwrite=self.can_overwrite)
                else:
                    bclient.upload_blob(content, overwrite=self.can_overwrite)

    def _list_files(self, prefix: str = None, **kwargs):  # , recursive=False, files_only=False):
        """
        A descendant class must implement this method.
        It should return a list of files already filtered out by prefix.
        Some storage engines (e.g. Google Cloud Storage) allow that
        at the API level. The method should use storage level filtering
        to save on network transfers.

        if prefix is given it is assumed to supersede the default container/interval/media_type/custom-prefix/ parts of
        the path. To only replace select parts of that path segment, use the ... (ellipsis) to indicate which portions
        you wish to have remain default.
        """
        results = set()
        if prefix:
            if prefix == "..." or prefix.startswith(".../"):
                prefix = prefix.strip("/")
                path_template = f"{self._protocol}://{self.default_host_name}/{prefix}"
                _, path_dict = self._path_parse(path_template, True)
            else:
                container, _, prefix = prefix.partition("/")
                path_dict = {"container": container, "fname_prefix": prefix}
        else:
            prefix = None  # ensure we don't pass along an empty string
            path_dict = {"container": None}
        fname = path_dict.pop("fname", None) or None
        prefix = _assemble_fname(path_dict) or prefix or None
        cont_starts, _, _ = (path_dict.get("container", "") or "").partition("*")
        with BlobServiceClient.from_connection_string(self.connection_string) as service_client:
            service_client: BlobServiceClient
            # service_client.
            for container in service_client.list_containers(cont_starts or None):
                with service_client.get_container_client(container) as cclient:
                    cclient: ContainerClient
                    if fname:
                        for bprop in cclient.list_blobs(prefix):
                            bprop: BlobProperties
                            if fname in bprop.name:
                                with cclient.get_blob_client(bprop) as bclient:
                                    results.add(bclient.url)
                    else:
                        for bprop in cclient.list_blobs(prefix):
                            bprop: BlobProperties
                            with cclient.get_blob_client(bprop) as bclient:
                                results.add(bclient.url)
                    # if files_only:
                    #     if recursive:
                    #         for bprop in cclient.list_blobs(prefix):
                    #             bprop: BlobProperties
                    #             bname: str = bprop.name
                    #             if not fname or fname in bname.rpartition("/")[2]:
                    #                 with cclient.get_blob_client(bprop) as bclient:
                    #                     results.add(bclient.url)
                    #     else:
                    #         for bprop in cclient.walk_blobs(prefix):
                    #             bprop: BlobProperties
                    #             bname = bprop.name
                    #             dbg_break = 0
                    # elif recursive:
                    #     if not fname:
                    #         for bprop in cclient.list_blobs(prefix):
                    #             bprop: BlobProperties
                    #             with cclient.get_blob_client(bprop) as bclient:
                    #                 results.add(bclient.url)
                    #
                    #     else:
                    #         for bprop in cclient.walk_blobs(prefix):
                    #             if fname in bname.rpartition("/")[2]:
                    #                 with cclient.get_blob_client(bprop) as bclient:
                    #                     results.add(bclient.url)
        return results
