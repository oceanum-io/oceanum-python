import asyncio
import io
import os
import logging
import weakref
import random
from urllib.parse import urlparse
from decorator import decorator
from pathlib import Path

import aiohttp
import requests
from fsspec.asyn import (
    AsyncFileSystem,
    sync,
    sync_wrapper,
)
from fsspec.callbacks import _DEFAULT_CALLBACK
from fsspec.exceptions import FSTimeoutError
from fsspec.utils import isfilelike, nullcontext, tokenize
from fsspec.implementations.memory import MemoryFile

DEFAULT_CONFIG = {"OCEANUM_DOMAIN": "oceanum.io"}

def _get_storage_service_url(domain: str) -> str:
    """Construct storage service URL from domain."""
    return f"https://storage.{domain}/"

_DEFAULT_BATCH_SIZE = 16
_NOFILES_DEFAULT_BATCH_SIZE = 16
logger = logging.getLogger("fsspec.oceanum")


@decorator
async def retry_request(func, retries=6, *args, **kwargs):
    for retry in range(retries):
        try:
            if retry > 0:
                await asyncio.sleep(min(random.random() + 2 ** (retry - 1), 32))
            return await func(*args, **kwargs)
        except (
            aiohttp.client_exceptions.ClientError,
            FSTimeoutError,
            FileNotFoundError,
        ) as e:
            if isinstance(e, FileNotFoundError):
                logger.debug("Request returned 404")
                raise e
            if retry == retries - 1:
                logger.exception(f"{func.__name__} out of retries on exception: {e}")
                raise e


async def get_client(**kwargs):
    return aiohttp.ClientSession(**kwargs)


class FileSystem(AsyncFileSystem):
    """Datamesh storage filesystem.

    This follows the fsspec specification and can be used with dask.

    You can use this class directly, for example:
        fs = FileSystem(token="my_datamesh_token")
        fs.ls("/myfolder")

    or use fsspec convenience functions with protocol "oceanum". For example:
        of=fsspec.open("oceanum://myfolder/myfile.txt", token="my_datamesh_token")
    """

    def __init__(
        self,
        token:str|None=None,
        service:str|None=None,
        asynchronous:bool=False,
        loop:asyncio.AbstractEventLoop|None=None,
        timeout:int=3600,
        batch_size:int=_DEFAULT_BATCH_SIZE,
    ):
        """Storage filesystem constructor

        Args:
            token (string): Your datamesh access token. Defaults to os.environ.get("DATAMESH_TOKEN", None).
            service (string, optional): URL of datamesh service. If not provided, constructed from OCEANUM_DOMAIN environment variable (defaults to "oceanum.io").
            timeout (int, optional): Timeout for requests in seconds. Defaults to 3600.
            batch_size (int, optional): Number of concurrent requests. Defaults to 16.
        Raises:
            ValueError: Missing or invalid arguments
        """
        if service is None:
            domain = os.environ.get("OCEANUM_DOMAIN", DEFAULT_CONFIG["OCEANUM_DOMAIN"])
            service = _get_storage_service_url(domain)
        self._service = service
        self._token = token or os.environ.get("DATAMESH_TOKEN", None)
        url = urlparse(service)
        self._proto = url.scheme
        self._host = url.netloc
        self._base_url = f"{self._proto}://{self._host}/"
        self._init_auth_headers(self._token)
        super().__init__(
            self, asynchronous=asynchronous, loop=loop, batch_size=batch_size
        )
        self.get_client = get_client
        self.client_kwargs = {
            "headers": self._auth_headers,
            "timeout": aiohttp.ClientTimeout(total=timeout, sock_read=timeout),
        }
        self._session = None

    def _init_auth_headers(self, token: str| None):
        if token is not None:
            if token.startswith("Bearer "):
                self._auth_headers = {"Authorization": token}
            else:
                self._auth_headers = {
                    "X-DATAMESH-TOKEN": token,
                }
        else:
            raise ValueError(
                "A valid key must be supplied as a connection constructor argument or defined in environment variables as DATAMESH_TOKEN"
            )

    @property
    def fsid(self):
        return "oceanum"

    @staticmethod
    def close_session(loop, session):
        if loop is not None and loop.is_running():
            try:
                sync(loop, session.close, timeout=0.1)
                return
            except (TimeoutError, FSTimeoutError):
                pass
        connector = getattr(session, "_connector", None)
        if connector is not None:
            # close after loop is dead
            connector._close()

    async def set_session(self):
        if self._session is None:
            self._session = await self.get_client(loop=self.loop, **self.client_kwargs)
            if not self.asynchronous:
                weakref.finalize(self, self.close_session, self.loop, self._session)
        return self._session

    async def _ls(self,
        path="",
        detail=True,
        file_prefix=None,
        match_glob=None,
        limit=None,
    ):
        logger.debug(path)
        session = await self.set_session()
        spath = path.lstrip("/")
        params = {}

        if limit:
            params["limit"] = limit
        if file_prefix:
            params["file_prefix"] = file_prefix
        if match_glob:
            params["match_glob"] = match_glob

        async with session.get(self._base_url + spath, params=params or None) as r:
            try:
                self._raise_not_found_for_status(r, path)
            except FileNotFoundError:
                # The storage endpoint enforces trailing slash for directories, so test for that
                if path.endswith("/"):
                    raise FileNotFoundError(path)
                else:
                    return await self._ls(
                        path + "/",
                        detail=detail,
                        file_prefix=file_prefix,
                        match_glob=match_glob,
                        limit=limit,
                    )
            listing = await r.json()
            if not listing:
                raise FileNotFoundError(path)
        if detail:
            return [
                {
                    **u,
                    "type": "directory" if u.get("contentType") == "folder" else "file",
                }
                for u in listing
            ]
        else:
            return [u["name"] for u in listing]

    ls = sync_wrapper(_ls)

    def _raise_not_found_for_status(self, response, url):
        """
        Raises FileNotFoundError for 404s, otherwise uses raise_for_status.
        """
        if response.status == 404:
            raise FileNotFoundError(url)
        response.raise_for_status()

    async def _cat_file(self, path, **kwargs):
        logger.debug(path)
        session = await self.set_session()
        async with session.get(self._base_url + path.strip("/")) as r:
            out = await r.read()
            self._raise_not_found_for_status(r, path)
        return out

    @retry_request
    async def _get_file(
        self, rpath, lpath, chunk_size=5 * 2**20, callback=_DEFAULT_CALLBACK, **kwargs
    ):
        if os.path.isdir(lpath):
            return
        logger.debug(rpath)
        if rpath[-1] == "/":
            os.makedirs(lpath, exist_ok=True)
            return
        session = await self.set_session()
        async with session.get(self._base_url + rpath.lstrip("/")) as r:
            try:
                size = int(r.headers["content-length"])
            except (ValueError, KeyError):
                size = None

            callback.set_size(size)
            self._raise_not_found_for_status(r, rpath)
            if isfilelike(lpath):
                outfile = lpath
            else:
                outfile = open(lpath, "wb")

            try:
                chunk = True
                while chunk:
                    chunk = await r.content.read(chunk_size)
                    outfile.write(chunk)
                    callback.relative_update(len(chunk))
            finally:
                if not isfilelike(lpath):
                    outfile.close()

    async def _put_file(
        self,
        lpath,
        rpath,
        chunk_size=5 * 2**20,
        callback=_DEFAULT_CALLBACK,
        method="post",
        **kwargs,
    ):
        async def gen_chunks():
            # Support passing arbitrary file-like objects
            # and use them instead of streams.
            if isinstance(lpath, io.IOBase):
                context = nullcontext(lpath)
                use_seek = False  # might not support seeking
            else:
                context = open(lpath, "rb")
                use_seek = True

            with context as f:
                if use_seek:
                    callback.set_size(f.seek(0, 2))
                    f.seek(0)
                else:
                    callback.set_size(getattr(f, "size", None))

                chunk = f.read(chunk_size)
                while chunk:
                    yield chunk
                    callback.relative_update(len(chunk))
                    chunk = f.read(chunk_size)

        session = await self.set_session()

        method = method.lower()
        if method not in ("post", "put"):
            raise ValueError(
                f"method has to be either 'post' or 'put', not: {method!r}"
            )

        meth = getattr(session, method)
        async with meth(self._base_url + rpath.strip("/"), data=gen_chunks()) as resp:
            self._raise_not_found_for_status(resp, rpath)

    async def _exists(self, path, **kwargs):
        try:
            logger.debug(path)
            session = await self.set_session()
            r = await session.get(self._base_url + path.lstrip("/"))
            async with r:
                if r.status < 400:
                    return True
                # The storage endpoint enforces trailing slash for directories, so test for that
                if not path.endswith("/"):
                    r2 = await session.get(self._base_url + path.lstrip("/") + "/")
                    async with r2:
                        return r2.status < 400
                return False
        except (requests.HTTPError, aiohttp.ClientError):
            return False

    async def _isfile(self, path):
        try:
            info = await self._info(path)
            return info["type"] == "file"
        except:
            return False

    async def _isdir(self, path):
        try:
            info = await self._info(path)
            return info["type"] == "directory"
        except OSError:
            return False

    async def _open(
        self,
        path,
        mode="rb",
        chunk_size=5 * 2**8,
        **kwargs,
    ):
        if mode != "rb":
            raise NotImplementedError
        logger.debug(path)
        session = await self.set_session()
        async with session.get(self._base_url + path.strip("/")) as r:
            try:
                size = int(r.headers["content-length"])
            except (ValueError, KeyError):
                size = None

            self._raise_not_found_for_status(r, path)
            f = MemoryFile(None, None)
            try:
                chunk = True
                while chunk:
                    chunk = await r.content.read(chunk_size)
                    f.write(chunk)
            finally:
                f.seek(0)
            return f

    async def _info(self, path, **kwargs):
        """Get info of path"""
        info = {}
        session = await self.set_session()
        async with session.head(self._base_url + path.lstrip("/")) as r:
            self._raise_not_found_for_status(r, path)
        return {
            "name": path,
            "size": int(r.headers.get("content-length")),
            "contentType": r.headers.get("content-type"),
            "modified": r.headers.get("last-modified"),
            "type": (
                "directory" if r.headers.get("content-type") == "folder" else "file"
            ),
        }

    async def _mkdir(self, path, create_parents=True, **kwargs):
        logger.debug(path)
        if create_parents:
            await self._makedirs(path, exist_ok=True)
        else:
            await self._makedirs(path, exist_ok=False)

    async def _makedirs(self, path, exist_ok=False):
        logger.debug(path)
        if not exist_ok:
            check = await self._exists(path)
            if check:
                raise FileExistsError(path)
        session = await self.set_session()
        async with session.put(self._base_url + path.strip("/") + "/_") as r:
            self._raise_not_found_for_status(r, path)

    async def _cp_file(self, path1, path2, **kwargs):
        logger.debug(f"{path1} -> {path2}")
        session = await self.set_session()
        async with session.post(
            self._base_url + path2.lstrip("/"), headers={"x-copy-source": path1}
        ) as r:
            self._raise_not_found_for_status(r, path1)
            return r.status == 201

    async def _rm(self, path, recursive=True, **kwargs):
        """Remove file or directory.

        Parameters
        ----------
        path : str
            Path to remove
        recursive : bool
            If True, remove directories and their contents recursively.
            If False, only remove empty directories.
        """
        logger.debug(f"Removing {path}, recursive={recursive}")

        # Check if path exists first
        if not await self._exists(path):
            raise FileNotFoundError(f"Path {path} not found")

        # Check if it's a directory
        is_dir = await self._isdir(path)

        if is_dir:
            # Check directory contents
            try:
                contents = await self._ls(path, detail=True)
                has_contents = len(contents) > 0
            except FileNotFoundError:
                # Directory is empty or already deleted
                has_contents = False

            if has_contents and not recursive:
                # Non-recursive deletion of non-empty directory should fail
                raise OSError(f"Directory not empty: {path}")

            if recursive and has_contents:
                # For recursive directory deletion, remove contents first
                for item in contents:
                    item_path = item["name"]
                    if item["type"] == "directory":
                        await self._rm(item_path, recursive=True)
                    else:
                        await self._rm(item_path, recursive=False)

        # Remove the file or empty directory
        session = await self.set_session()
        clean_path = path.lstrip("/")

        # For directories, ensure trailing slash
        if is_dir and not clean_path.endswith("/"):
            clean_path += "/"

        async with session.delete(self._base_url + clean_path) as r:
            if r.status == 404:
                # Already deleted or doesn't exist
                return
            elif r.status >= 400:
                self._raise_not_found_for_status(r, path)

    def ukey(self, path):
        """Unique identifier"""
        return tokenize(path)


def ls(
    path: str,
    recursive: bool,
    detail: bool = False,
    token: str | None = None,
    service: str | None = None,
    **kwargs,
):
    """List contents in the oceanum storage (the root directory by default).

    Parameters
    ----------
    path: str
        Path to list.
    recursive: bool
        List subdirectories recursively.
    detail: bool
        Return detailed information about each content.
    token: str
        Oceanum datamesh token.
    service: str
        Oceanum storage service URL.

    Returns
    -------
    contents: list | dict
        List of contents or dictionary with detailed information about each content.

    """
    fs = FileSystem(token=token, service=service)
    try:
        maxdepth = None if recursive else 1
        paths = fs.find(path, maxdepth=maxdepth, withdirs=True, detail=detail,
                        **kwargs)
        if not paths:
            raise FileNotFoundError(f"Path {path} not found")
        return paths
    except aiohttp.client_exceptions.ClientError as err:
        raise aiohttp.client_exceptions.ClientError(
            f"Path {path} not found or not authorised (check datamesh token)"
        ) from err


def get(
    source: str,
    dest: str,
    recursive: bool = False,
    token: str | None = None,
    service: str | None = None,
):
    """Copy remote source to local dest, or multiple sources to directory.

    Parameters
    ----------
    source: str
        Path to get.
    dest: str
        Destination path.
    recursive: bool
        Get directories recursively.
    overwrite: bool
        Overwrite existing destination.
    token: str
        Oceanum datamesh token.
    service: str
        Oceanum storage service URL.

    Notes
    -----
    - Directory dest defined with a trailing slash must exist, consistent with gsutil.
    - Non-existing file dest path is allowed, consistent with gsutil (all required
      intermediate directories are created).

    """
    fs = FileSystem(token=token, service=service)
    is_source_dir = fs.isdir(source)

    # Ensure attempting to copy folder without recursive option does not fail silently
    if is_source_dir and not recursive:
        raise IsADirectoryError(f"--recursive is required to get directory {source}")

    # Deal with mimetype error when trying to copy file with recursive option
    if not is_source_dir and recursive:
        raise NotADirectoryError(f"Source {source} is a file, do not use --recursive")

    # Prevent from downloading into a folder that does not exist
    if not Path(dest).is_dir() and dest.endswith(os.path.sep):
        raise FileNotFoundError(f"Destination directory {dest} not found")

    # Expand destination file name to avoid IsADirectoryError
    if not is_source_dir and Path(dest).is_dir():
        dest = str(Path(dest) / Path(source).name)

    # Expand destination folder name if different than source's to keep path structure
    if is_source_dir and Path(dest) != Path(source):
        dest = str(Path(dest) / Path(source).name)

    # Downloading
    fs.get(source, dest, recursive=recursive)


def put(
    source: str,
    dest: str,
    recursive: bool = False,
    token: str | None = None,
    service: str | None = None,
):
    """Copy local source to remote dest, or multiple sources to directory.

    Parameters
    ----------
    source: str
        Path to get.
    dest: str
        Destination path.
    recursive: bool
        Get directories recursively.
    token: str
        Oceanum datamesh token.
    service: str
        Oceanum storage service URL.

    """
    fs = FileSystem(token=token, service=service)
    is_dest_dir = fs.isdir(dest)

    # Deal with ClientOsError when trying to copy non-existing source
    if not Path(source).exists():
        raise FileNotFoundError(f"Source {source} not found")

    # Ensure attempting to copy folder without recursive option does not fail silently
    if Path(source).is_dir() and not recursive:
        raise IsADirectoryError(f"--recursive is required to put directory {source}")

    # Raise if trying to upload a folder into an existing file
    if fs.exists(dest) and not is_dest_dir and Path(source).is_dir():
        raise FileExistsError(f"Destination {dest} is an existing file")

    # Downloading
    fs.put(source, dest, recursive=recursive)


def rm(
    path: str,
    recursive: bool = False,
    token: str | None = None,
    service: str | None = None,
):
    """Remove path file or directory.

    Parameters
    ----------
    path: str
        Path to remove.
    recursive: bool
        Remove directories recursively.
    token: str
        Oceanum datamesh token.
    service: str
        Oceanum storage service URL.

    """
    fs = FileSystem(token=token, service=service)
    try:
        return fs.rm(path, recursive=recursive)
    except aiohttp.client_exceptions.ClientError as err:
        raise aiohttp.client_exceptions.ClientError(
            f"Could not remove path {path} (check datamesh token)"
        ) from err


def exists(
    path: str,
    token: str | None = None,
    service: str | None = None,
) -> bool:
    """Check if path exists in oceanum storage.

    Parameters
    ----------
    path: str
        Path to check.
    token: str
        Oceanum datamesh token.
    service: str
        Oceanum storage service URL.

    Returns
    -------
    bool
        True if path exists, False otherwise.

    """
    fs = FileSystem(token=token, service=service)
    try:
        return fs.exists(path)
    except aiohttp.client_exceptions.ClientError as err:
        raise aiohttp.client_exceptions.ClientError(
            f"Could not check if path {path} exists (check datamesh token)"
        ) from err


def isfile(
    path: str,
    token: str | None = None,
    service: str | None = None,
) -> bool:
    """Check if path is a file in oceanum storage.

    Parameters
    ----------
    path: str
        Path to check.
    token: str
        Oceanum datamesh token.
    service: str
        Oceanum storage service URL.

    Returns
    -------
    bool
        True if path is a file, False otherwise.

    """
    fs = FileSystem(token=token, service=service)
    try:
        return fs.isfile(path)
    except aiohttp.client_exceptions.ClientError as err:
        raise aiohttp.client_exceptions.ClientError(
            f"Could not check if path {path} is a file (check datamesh token)"
        ) from err


def isdir(
    path: str,
    token: str | None = None,
    service: str | None = None,
) -> bool:
    """Check if path is a directory in oceanum storage.

    Parameters
    ----------
    path: str
        Path to check.
    token: str
        Oceanum datamesh token.
    service: str
        Oceanum storage service URL.

    Returns
    -------
    bool
        True if path is a directory, False otherwise.

    """
    fs = FileSystem(token=token, service=service)
    try:
        return fs.isdir(path)
    except aiohttp.client_exceptions.ClientError as err:
        raise aiohttp.client_exceptions.ClientError(
            f"Could not check if path {path} is a directory (check datamesh token)"
        ) from err
