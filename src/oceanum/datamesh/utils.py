from time import sleep
import requests
import numpy as np
from .exceptions import DatameshConnectError
import os

DATAMESH_READ_TIMEOUT = os.getenv("DATAMESH_READ_TIMEOUT", 10)
DATAMESH_READ_TIMEOUT = None if DATAMESH_READ_TIMEOUT == "None" else float(DATAMESH_READ_TIMEOUT)
DATAMESH_CONNECT_TIMEOUT = os.getenv("DATAMESH_CONNECT_TIMEOUT", 3.05)
DATAMESH_CONNECT_TIMEOUT = None if DATAMESH_CONNECT_TIMEOUT == "None" else float(DATAMESH_CONNECT_TIMEOUT)


def retried_request(url, method="GET", data=None, params=None, headers=None, retries=8, timeout=(DATAMESH_CONNECT_TIMEOUT, DATAMESH_READ_TIMEOUT)):
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
                method=method, url=url, data=data, params=params, headers=headers, timeout=timeout
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
                raise DatameshConnectError(f"Failed to connect to {url} after {retries} retries with error: {e}")
        else:
            return resp
