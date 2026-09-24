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


def test_stage_502_is_reattempted_once(conn, no_session):
    stage_ok = _response(200, json_body={
        "query": QUERY, "qhash": "abc", "formats": ["application/x-netcdf4"],
        "size": 10, "dlen": 1, "coordmap": {}, "coordkeys": {},
        "container": "dataset", "sig": "deadbeef",
    })
    with patch.object(conn, "_retried_request",
                      side_effect=[_response(502, text="Bad Gateway"), stage_ok]) as rr, \
         patch("oceanum.datamesh.connection.time.sleep") as slept:
        stage = conn._stage_request(_query_obj(), no_session.acquire.return_value)
    assert rr.call_count == 2
    assert stage.qhash == "abc"
    # The wait must be the long one, not a sub-second backoff.
    assert slept.call_args[0][0] >= 10


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
    """500 is the service answering, not the gateway failing. It must not be
    retried and must not become DatameshUnavailableError."""
    from oceanum.datamesh.query import Container

    stage = _stage()
    stage.container = Container.Dataset
    with patch.object(conn, "_stage_request", return_value=stage), \
         patch.object(conn, "_retried_request",
                      return_value=_response(500, json_body={"detail": "boom"})) as rr, \
         patch("oceanum.datamesh.connection.time.sleep"):
        with pytest.raises(DatameshQueryError, match="boom"):
            conn._query(QUERY)
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

    # The first close must precede the first inter-attempt sleep.
    assert order[0] == "close", f"session still open during the wait: {order}"
