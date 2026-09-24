"""Connection._query / _stage_request gateway-failure policy.

This path had no test coverage: the re-attempt count on a 502, the refusal to
re-attempt a 503, and the exception raised were all unpinned, which is how the
2026-09-22 cascade reached production behaviour unnoticed.

The policy under test:

  502 on the query POST   -> one re-attempt after a long jittered wait
  503/504 on the query    -> no re-attempt; wait, then raise
  502 on the stage POST   -> one re-attempt (cheap, qhash-keyed server-side)
  anything else           -> untouched
"""

import pytest
import requests
from unittest.mock import patch, Mock, call

from oceanum.datamesh.exceptions import (
    DatameshConnectError,
    DatameshUnavailableError,
    DatameshQueryError,
)


def _response(status, headers=None, text="", json_body=None):
    resp = Mock(spec=requests.Response)
    resp.status_code = status
    resp.headers = headers or {}
    resp.text = text
    if json_body is None:
        resp.json.side_effect = ValueError("not json")
    else:
        resp.json.return_value = json_body
    return resp


@pytest.fixture
def conn():
    """A Connector that never touches the network.

    Connector.__init__ calls _check_info, which pings /info/ to report the
    gateway version; left unstubbed that is a real DNS lookup with 5 retries
    for every test in this file.
    """
    from oceanum.datamesh.connection import Connector

    with patch.object(Connector, "_check_info", return_value=None):
        # yield, not return, so the patch covers the whole test.
        yield Connector(token="dummy-not-a-real-token", service="https://gateway")


@pytest.fixture
def no_session():
    """Stub Session.acquire/close so _query needs no gateway."""
    with patch("oceanum.datamesh.connection.Session") as S:
        sess = Mock()
        sess.header = {}
        sess.close = Mock()
        S.acquire.return_value = sess
        yield S


# --------------------------------------------------------------------------
# _stage_request
# --------------------------------------------------------------------------


def test_stage_502_standalone_paces_then_raises(conn, no_session):
    """Called outside _query (load_datasource) there is no loop to defer the
    wait to, so _stage_request paces here and raises the public error.

    The re-attempt itself is _query's job now -- see
    test_stage_and_download_share_one_reattempt_budget -- because a per-hop
    budget let one query() make four staging POSTs and take four long waits.
    """
    with patch.object(conn, "_retried_request",
                      return_value=_response(502, text="Bad Gateway")) as rr, \
         patch("oceanum.datamesh.connection.time.sleep") as slept:
        with pytest.raises(DatameshUnavailableError) as exc:
            conn._stage_request(_query_obj(), no_session.acquire.return_value)
    assert rr.call_count == 1
    assert exc.value.status_code == 502
    # Paced, and with the long delay rather than a sub-second backoff.
    assert slept.call_count == 1 and slept.call_args[0][0] >= 10


def test_stage_502_defers_its_wait_when_called_from_query(conn, no_session):
    """With a retry index supplied, _stage_request must not sleep -- the wait
    belongs to _query, after the session is closed."""
    with patch.object(conn, "_retried_request", return_value=_response(502)), \
         patch("oceanum.datamesh.connection.time.sleep") as slept:
        with pytest.raises(Exception) as exc:
            conn._stage_request(_query_obj(), no_session.acquire.return_value, retry=0)
    assert slept.call_count == 0, "_stage_request must not sleep while a session is held"
    assert type(exc.value).__name__ == "_GatewayOutcome"


def test_stage_503_is_not_reattempted(conn, no_session):
    with patch.object(conn, "_retried_request",
                      return_value=_response(503, text="no available server")) as rr, \
         patch("oceanum.datamesh.connection.time.sleep") as slept:
        with pytest.raises(DatameshUnavailableError) as exc:
            conn._stage_request(_query_obj(), no_session.acquire.return_value)
    assert rr.call_count == 1
    assert exc.value.status_code == 503
    assert exc.value.retry_after >= 60
    # Still waits before raising -- that wait paces the caller's own loop.
    assert slept.call_args[0][0] >= 10


def test_stage_4xx_detail_is_preserved(conn, no_session):
    """The bare `except:` used to swallow DatameshQueryError raised inside the
    try, so every 4xx became an opaque DatameshConnectError and the server's
    `detail` was discarded."""
    with patch.object(conn, "_retried_request",
                      return_value=_response(400, json_body={"detail": "bad timefilter"})):
        with pytest.raises(DatameshQueryError, match="bad timefilter"):
            conn._stage_request(_query_obj(), no_session.acquire.return_value)


def test_stage_4xx_without_json_is_a_connect_error(conn, no_session):
    with patch.object(conn, "_retried_request",
                      return_value=_response(404, text="<html>nginx</html>")):
        with pytest.raises(DatameshConnectError, match="Datamesh server error"):
            conn._stage_request(_query_obj(), no_session.acquire.return_value)


def test_stage_failure_does_not_leak_the_session(conn, no_session):
    """Staging sits outside the try/finally that closes the session, so every
    one of its failure paths has to close it explicitly. A leaked session
    blocks writes to its datasource until it expires."""
    sess = no_session.acquire.return_value
    from oceanum.datamesh.query import Container

    for body, expected in (
        (_response(503, text="no server"), DatameshUnavailableError),
        (_response(400, json_body={"detail": "nope"}), DatameshQueryError),
        (_response(404, text="<html>"), DatameshConnectError),
    ):
        sess.close.reset_mock()
        with patch.object(conn, "_retried_request", return_value=body), \
             patch("oceanum.datamesh.connection.time.sleep"):
            with pytest.raises(expected):
                conn._query(QUERY)
        assert sess.close.call_count >= 1, f"session leaked on {body.status_code}"


def test_empty_stage_does_not_leak_the_session(conn, no_session):
    """`stage is None` (no data for the query) returns early -- also outside
    the try/finally."""
    sess = no_session.acquire.return_value
    sess.close.reset_mock()
    with patch.object(conn, "_stage_request", return_value=None):
        with pytest.warns(UserWarning, match="No data found"):
            assert conn._query(QUERY) is None
    assert sess.close.call_count == 1


def test_stage_and_download_share_one_reattempt_budget(conn, no_session):
    """A 502 at either hop consumes the same budget.

    With a per-hop budget, one query() could make four staging POSTs and take
    four long waits. The contract is one re-attempt per query() call.
    """
    stage_ok = _response(200, json_body={
        "query": QUERY, "qhash": "abc", "formats": ["application/x-netcdf4"],
        "size": 10, "dlen": 1, "coordmap": {}, "coordkeys": {},
        "container": "dataset", "sig": "deadbeef",
    })
    # stage 502 -> (re-attempt) stage ok -> download 502 -> budget spent, raise
    calls = [_response(502), stage_ok, _response(502)]
    with patch.object(conn, "_retried_request", side_effect=calls) as rr, \
         patch("oceanum.datamesh.connection.time.sleep") as slept:
        with pytest.raises(DatameshUnavailableError):
            conn._query(QUERY)
    assert rr.call_count == 3, f"expected 3 requests, got {rr.call_count}"
    assert slept.call_count == 2, "at most two waits per query() call"


# --------------------------------------------------------------------------
# _query
# --------------------------------------------------------------------------


QUERY = {"datasource": "test-datasource"}


def _query_obj():
    from oceanum.datamesh.query import Query

    return Query(**QUERY)


def _stage(size=10):
    return Mock(qhash="abc", size=size, dlen=1, container="Dataset", coordmap={})


def test_query_502_is_reattempted_once_then_raises(conn, no_session):
    """One re-attempt: the sibling is probably healthy and the work was lost,
    not completed. But not two -- and not after half a second."""
    from oceanum.datamesh.query import Container

    stage = _stage()
    stage.container = Container.Dataset
    with patch.object(conn, "_stage_request", return_value=stage), \
         patch.object(conn, "_retried_request",
                      return_value=_response(502, text="Bad Gateway")) as rr, \
         patch("oceanum.datamesh.connection.time.sleep") as slept:
        with pytest.raises(DatameshUnavailableError) as exc:
            conn._query(QUERY)
    assert rr.call_count == 2, "expected exactly one re-attempt of the query POST"
    assert exc.value.status_code == 502
    assert all(c[0][0] >= 10 for c in slept.call_args_list), "waits must be long"


def test_query_503_is_never_reattempted(conn, no_session):
    """The memory case arrives as a 503 saying 'Please try again'. Re-running
    the query that just exhausted a worker is what killed both replicas on
    2026-09-22."""
    from oceanum.datamesh.query import Container

    stage = _stage()
    stage.container = Container.Dataset
    with patch.object(conn, "_stage_request", return_value=stage), \
         patch.object(conn, "_retried_request",
                      return_value=_response(503, text="Worker memory limit exceeded")) as rr, \
         patch("oceanum.datamesh.connection.time.sleep") as slept:
        with pytest.raises(DatameshUnavailableError) as exc:
            conn._query(QUERY)
    assert rr.call_count == 1, "a 503 must never be re-attempted automatically"
    assert exc.value.retry_after >= 60
    assert "smaller" in str(exc.value)
    assert slept.call_count == 1 and slept.call_args[0][0] >= 10


def test_query_504_is_never_reattempted(conn, no_session):
    from oceanum.datamesh.query import Container

    stage = _stage()
    stage.container = Container.Dataset
    with patch.object(conn, "_stage_request", return_value=stage), \
         patch.object(conn, "_retried_request", return_value=_response(504)) as rr, \
         patch("oceanum.datamesh.connection.time.sleep"):
        with pytest.raises(DatameshUnavailableError):
            conn._query(QUERY)
    assert rr.call_count == 1


def test_query_500_is_not_a_gateway_failure(conn, no_session):
    """500 is the service answering, not the gateway failing: not retried, and
    not DatameshUnavailableError.

    It is also not DatameshQueryError. The query-engine renders every
    InternalQueryError as {"detail": ...} with a 500, so classifying a
    detail-bearing 500 as a rejected query told callers a transient server-side
    failure was terminal -- and flipped at least one pipeline from
    skip-this-grid-with-a-warning to crash.
    """
    from oceanum.datamesh.query import Container

    stage = _stage()
    stage.container = Container.Dataset
    with patch.object(conn, "_stage_request", return_value=stage), \
         patch.object(conn, "_retried_request",
                      return_value=_response(500, json_body={"detail": "boom"})) as rr, \
         patch("oceanum.datamesh.connection.time.sleep"):
        with pytest.raises(DatameshConnectError, match="boom") as exc:
            conn._query(QUERY)
    assert not isinstance(exc.value, DatameshQueryError), "a 500 is not a rejected query"
    assert not isinstance(exc.value, DatameshUnavailableError)
    assert rr.call_count == 1


def test_query_retries_can_be_disabled(conn, no_session, monkeypatch):
    """Opt-out rather than removal: an operator who would rather see the
    failure immediately can have that."""
    import oceanum.datamesh.connection as mod
    from oceanum.datamesh.query import Container

    monkeypatch.setattr(mod, "DATAMESH_QUERY_RETRIES", 1)
    stage = _stage()
    stage.container = Container.Dataset
    with patch.object(conn, "_stage_request", return_value=stage), \
         patch.object(conn, "_retried_request", return_value=_response(502)) as rr, \
         patch("oceanum.datamesh.connection.time.sleep"):
        with pytest.raises(DatameshUnavailableError):
            conn._query(QUERY)
    assert rr.call_count == 1, "DATAMESH_QUERY_RETRIES=1 means no re-attempt"


def test_query_session_is_closed_before_the_reattempt_wait(conn, no_session):
    """The wait must not hold the old session open.

    The re-attempt used to recurse inside the try whose finally closes the
    session, so a 30s wait would pin server-side session state for the whole
    re-attempt. _query now catches the retry signal outside that block.
    """
    from oceanum.datamesh.query import Container

    stage = _stage()
    stage.container = Container.Dataset
    order = []
    sess = no_session.acquire.return_value
    sess.close.side_effect = lambda: order.append("close")

    with patch.object(conn, "_stage_request", return_value=stage), \
         patch.object(conn, "_retried_request", return_value=_response(502)), \
         patch("oceanum.datamesh.connection.time.sleep",
               side_effect=lambda d: order.append("sleep")):
        with pytest.raises(DatameshUnavailableError):
            conn._query(QUERY)

    # Every wait must sit between a close and the next acquire. Asserting only
    # order[0] is not enough: the terminal wait before raising used to happen
    # inside the try/finally, so a correct-looking order[0] hid an open-session
    # sleep later in the same call.
    assert order == ["close", "sleep", "close", "sleep"], (
        f"a wait was taken while a session was open: {order}"
    )


def test_use_dask_with_caching_on_a_dataframe_does_not_crash(conn, no_session):
    """Regression: localcache was created under `cache_timeout and not use_dask`
    but locked under `cache_timeout` alone, so use_dask=True with a non-Dataset
    container reached localcache.lock() with the name unbound.

    Reachable without passing use_dask=True at all, because any query over
    DASK_QUERY_SIZE sets it further down.
    """
    from oceanum.datamesh.query import Container

    stage = _stage()
    stage.container = Container.DataFrame
    with patch.object(conn, "_stage_request", return_value=stage), \
         patch.object(conn, "_retried_request", return_value=_response(200)), \
         patch("oceanum.datamesh.connection.LocalCache") as LC:
        # Fails with NameError before the fix; the parquet read is what raises
        # now, which is fine -- the point is that the cache guard is reached.
        try:
            conn._query(QUERY, use_dask=True, cache_timeout=600)
        except NameError:
            raise AssertionError("localcache guard is still unbound")
        except Exception:
            pass
        # Caching is on, so the cache object must have been built and locked.
        assert LC.called, "LocalCache was never constructed despite cache_timeout"
        assert LC.return_value.lock.called, "the cache entry was never locked"


def test_no_caching_means_no_cache_object(conn, no_session):
    from oceanum.datamesh.query import Container

    stage = _stage()
    stage.container = Container.DataFrame
    with patch.object(conn, "_stage_request", return_value=stage), \
         patch.object(conn, "_retried_request", return_value=_response(200)), \
         patch("oceanum.datamesh.connection.LocalCache") as LC:
        try:
            conn._query(QUERY, cache_timeout=0)
        except Exception:
            pass
        assert not LC.called


# --------------------------------------------------------------------------
# Resource and presentation invariants for the gateway paths
# --------------------------------------------------------------------------


def test_internal_sentinel_does_not_appear_in_the_traceback(conn, no_session):
    """_GatewayOutcome is an implementation detail. Raising the terminal error
    from inside `except _GatewayOutcome` chained it, so users saw
    "_GatewayOutcome: gateway outcome after 27.3s / During handling of the above
    exception, another exception occurred"."""
    from oceanum.datamesh.query import Container

    stage = _stage()
    stage.container = Container.Dataset
    with patch.object(conn, "_stage_request", return_value=stage), \
         patch.object(conn, "_retried_request", return_value=_response(503)), \
         patch("oceanum.datamesh.connection.time.sleep"):
        with pytest.raises(DatameshUnavailableError) as exc:
            conn._query(QUERY)
    assert exc.value.__suppress_context__ is True, (
        "the internal sentinel is shown to the user as a chained exception"
    )


def test_load_datasource_closes_the_session_on_the_success_path(conn, no_session):
    """Only the zarr branch needs the session to outlive the call. The
    DataFrame/GeoDataFrame branches used to return without closing it, so it
    survived to atexit holding server-side state."""
    from oceanum.datamesh.query import Container

    sess = no_session.acquire.return_value
    sess.close.reset_mock()
    stage = _stage()
    stage.container = Container.DataFrame
    with patch.object(conn, "_stage_request", return_value=stage), \
         patch.object(conn, "_data_request", return_value="/tmp/x.pq"), \
         patch("oceanum.datamesh.connection.pandas.read_parquet", return_value="DF"):
        assert conn.load_datasource("dsx") == "DF"
    assert sess.close.call_count == 1, "load_datasource leaked its session"


def test_cache_lock_is_released_when_the_download_raises(conn, no_session):
    """lock() was released only on the paths that inspected a status code, so a
    transport failure or a parse error left it held for its full 60 s timeout --
    blocking the next query() for the same hash."""
    from oceanum.datamesh.query import Container

    stage = _stage()
    stage.container = Container.DataFrame
    lc = Mock()
    lc.get.return_value = None
    with patch.object(conn, "_stage_request", return_value=stage), \
         patch.object(conn, "_retried_request",
                      side_effect=DatameshConnectError("transport died")), \
         patch("oceanum.datamesh.connection.LocalCache", return_value=lc):
        with pytest.raises(DatameshConnectError):
            conn._query(QUERY, cache_timeout=600)
    assert lc.lock.call_count == lc.unlock.call_count, (
        f"cache lock left held: lock={lc.lock.call_count} unlock={lc.unlock.call_count}"
    )


def test_4xx_is_a_query_error_and_5xx_is_not(conn, no_session):
    """The dividing line: 4xx means the request was wrong, 5xx means the server
    failed. Both carry a `detail`, so only the status can tell them apart."""
    from oceanum.datamesh.query import Container

    stage = _stage()
    stage.container = Container.Dataset
    for status, expect_query_error in ((400, True), (404, True), (422, True), (500, False)):
        with patch.object(conn, "_stage_request", return_value=stage), \
             patch.object(conn, "_retried_request",
                          return_value=_response(status, json_body={"detail": "d"})), \
             patch("oceanum.datamesh.connection.time.sleep"):
            with pytest.raises(DatameshConnectError if not expect_query_error
                               else DatameshQueryError) as exc:
                conn._query(QUERY)
        assert isinstance(exc.value, DatameshQueryError) is expect_query_error, status
        # the server's detail survives either way
        assert "d" in str(exc.value), status


def test_stage_4xx_vs_5xx_split(conn, no_session):
    """Same split on the staging hop."""
    for status, expect_query_error in ((400, True), (500, False)):
        with patch.object(conn, "_retried_request",
                          return_value=_response(status, json_body={"detail": "why"})), \
             patch("oceanum.datamesh.connection.time.sleep"):
            with pytest.raises(DatameshConnectError if not expect_query_error
                               else DatameshQueryError) as exc:
                conn._stage_request(_query_obj(), no_session.acquire.return_value)
        assert isinstance(exc.value, DatameshQueryError) is expect_query_error, status
        assert "why" in str(exc.value), status
