from time import sleep
import requests
import numpy as np
from .exceptions import DatameshConnectError
import os


# Timeouts in seconds to establish connection to datamesh services
# for read types of operations
DATAMESH_CONNECT_TIMEOUT = os.getenv("DATAMESH_CONNECT_TIMEOUT", 3.05)
DATAMESH_CONNECT_TIMEOUT = (
    None if DATAMESH_CONNECT_TIMEOUT == "None" else float(DATAMESH_CONNECT_TIMEOUT)
)

# Timeout in seconds to read data from datamesh services
# for small json payloads type of operations
DATAMESH_READ_TIMEOUT = os.getenv("DATAMESH_READ_TIMEOUT", 10)
DATAMESH_READ_TIMEOUT = (
    None if DATAMESH_READ_TIMEOUT == "None" else float(DATAMESH_READ_TIMEOUT)
)

# Timeout in seconds for staging endpoint
DATAMESH_STAGE_READ_TIMEOUT = os.getenv("DATAMESH_STAGE_READ_TIMEOUT", 900)
DATAMESH_STAGE_READ_TIMEOUT = (
    None if DATAMESH_STAGE_READ_TIMEOUT == "None" else float(DATAMESH_STAGE_READ_TIMEOUT)
)

# Timeout in seconds for bulk download operations
DATAMESH_DOWNLOAD_TIMEOUT = os.getenv("DATAMESH_DOWNLOAD_TIMEOUT", 900)
DATAMESH_DOWNLOAD_TIMEOUT = (
    None if DATAMESH_DOWNLOAD_TIMEOUT == "None" else float(DATAMESH_DOWNLOAD_TIMEOUT)
)

# Timeout in seconds for bulk write operations
DATAMESH_WRITE_TIMEOUT = os.getenv("DATAMESH_WRITE_TIMEOUT", "None")
DATAMESH_WRITE_TIMEOUT = (
    None if DATAMESH_WRITE_TIMEOUT == "None" else float(DATAMESH_WRITE_TIMEOUT)
)

# Timeout in seconds for zarr chunk read operations
DATAMESH_CHUNK_READ_TIMEOUT = os.getenv("DATAMESH_CHUNK_READ_TIMEOUT", 60)
DATAMESH_CHUNK_READ_TIMEOUT = (
    None if DATAMESH_CHUNK_READ_TIMEOUT == "None" else float(DATAMESH_CHUNK_READ_TIMEOUT)
)

# Timeout in seconds for zarr chunk write operations
# much larger than for read seems to be required possibly because write acknowledgement
# occurs after the chunk has been fully written
DATAMESH_CHUNK_WRITE_TIMEOUT = os.getenv("DATAMESH_CHUNK_WRITE_TIMEOUT", 600)
DATAMESH_CHUNK_WRITE_TIMEOUT = (
    None if DATAMESH_CHUNK_WRITE_TIMEOUT == "None" else float(DATAMESH_CHUNK_WRITE_TIMEOUT)
)


def retried_request(
    url,
    method="GET",
    data=None,
    params=None,
    headers=None,
    retries=8,
    timeout=(DATAMESH_CONNECT_TIMEOUT, DATAMESH_READ_TIMEOUT),
    verify=True,
):
    """
    Retried request function with exponential backoff

    Parameters
    ----------
    url : str
        URL to request
    method : str, optional
        HTTP method, by default "GET"
    data : str, optional
        Request data, by default None
    headers : dict, optional
        Request headers, by default None
    retries : int, optional
        Number of retries, by default 8
    timeout : tupe(float, float), optional
        Request connect and read timeout in seconds, by default (3.05, 10)

    Returns
    -------
    requests.Response
        Response object

    Raises
    ------
    requests.RequestException
        If request fails

    """
    retried = 0
    while retried < retries:
        try:
            resp = requests.request(
                method=method,
                url=url,
                data=data,
                params=params,
                headers=headers,
                timeout=timeout,
                verify=verify,
            )
            # Bad Gateway results in waiting for 10 seconds
            # and retrying
            if resp.status_code == 502:
                sleep(30)
                raise requests.RequestException
        except (
            requests.RequestException,
            requests.ReadTimeout,
            requests.ConnectionError,
            requests.ConnectTimeout,
        ) as e:
            sleep(0.1 * 2**retried)
            retried += 1
            if retried == retries:
                raise DatameshConnectError(
                    f"Failed to connect to {url} after {retries} retries with error: {e}"
                )
        else:
            return resp
