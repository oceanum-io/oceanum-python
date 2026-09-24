class DatameshError(Exception):
    """Base class for every error raised by the datamesh client.

    Exists so a caller can handle all datamesh failures with one `except`
    without enumerating subclasses, and so new error types can be added
    without breaking existing handlers. The `oceanum_exc` marker predates
    this class and is kept for code that checks it.
    """

    oceanum_exc = True

    #: Seconds the caller should wait before re-attempting, when the failure
    #: carries that information (see DatameshUnavailableError). None means the
    #: client has no opinion -- it does *not* mean "retry immediately".
    retry_after = None


class DatameshConnectError(DatameshError):
    """Could not reach a datamesh service, or the request failed in transport.

    Also raised for gateway-level failures that the client did not or could
    not retry. Where the distinction matters, catch the more specific
    DatameshUnavailableError first.
    """


class DatameshUnavailableError(DatameshConnectError):
    """A datamesh service was reachable but could not serve the request.

    Raised for gateway statuses (502/503/504). The request may succeed later,
    but *not* immediately -- `retry_after` is the minimum sensible wait.

    A caller that retries should honour `retry_after` rather than looping on
    its own schedule. Retrying sooner cannot help: these statuses mean a pod
    died, no pod is available, or the request exceeded what one worker can
    do. In the last case the query will fail again however long you wait,
    unless it is made smaller.

    Subclasses DatameshConnectError so existing handlers keep working.
    """

    def __init__(self, message, retry_after=None, status_code=None):
        super().__init__(message)
        self.retry_after = retry_after
        self.status_code = status_code


class DatameshQueryError(DatameshError):
    """The query itself was rejected -- malformed, or not satisfiable.

    Retrying an unchanged query will fail the same way.
    """


class DatameshWriteError(DatameshError):
    """A write to a datasource failed."""


class DatameshSessionError(DatameshError):
    """A datamesh session could not be created, used or finalised."""
