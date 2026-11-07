#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Notices about datasources from the oceanum.io admin service.

No request leaves the process: the admin service is requests.get patched in
oceanum.datamesh.notices, and the Connector's own Datamesh calls are patched out.
"""

import logging
import threading
import time
import warnings
from unittest.mock import MagicMock, patch

import pytest
import requests

from oceanum.datamesh import Connector, Notice, NoticeWarning, Query
from oceanum.datamesh import notices as notices_module
from oceanum.datamesh.notices import admin_service_url

TOKEN = "secret-datamesh-token"
ADMIN = "https://admin.oceanum.io"


def _notice(severity, nid=None, title=None):
    return {
        "id": nid or f"id-{severity}",
        "type": "deprecation",
        "severity": severity,
        "title": title or f"{severity} title",
        "body": "**markdown**",
        "publish_at": "2026-11-01T00:00:00Z",
        "expires_at": None,
    }


def _response(status=200, items=(), headers=None):
    resp = MagicMock(status_code=status, headers=headers or {})
    resp.json.return_value = (
        {"items": list(items)} if status == 200 else {"detail": "x"}
    )
    return resp


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    monkeypatch.delenv("OCEANUM_ADMIN_SERVICE", raising=False)
    monkeypatch.delenv("OCEANUM_NOTICES", raising=False)
    notices_module._clear()
    yield
    notices_module._clear()


@pytest.fixture
def admin_get():
    with patch("oceanum.datamesh.notices.requests.get") as get:
        get.return_value = _response(200, [])
        yield get


def _connector(token=TOKEN, service="https://datamesh.oceanum.io", **kwargs):
    with patch.object(Connector, "_check_info"):
        return Connector(token=token, service=service, **kwargs)


def _load(conn, datasource_id="my-datasource"):
    """load_datasource with its Datamesh calls patched out (no data found)."""
    with (
        patch("oceanum.datamesh.connection.Session.acquire"),
        patch.object(conn, "_stage_request", return_value=None),
    ):
        return conn.load_datasource(datasource_id)


def _notice_warnings(record):
    return [w for w in record if issubclass(w.category, NoticeWarning)]


# Credential


def test_datamesh_token_sent_as_x_datamesh_token(admin_get):
    _connector().get_notices("my-datasource")
    kwargs = admin_get.call_args.kwargs
    assert admin_get.call_args.args[0] == f"{ADMIN}/notices/active"
    assert kwargs["params"] == {"datasource": "my-datasource"}
    assert kwargs["headers"] == {"X-DATAMESH-TOKEN": TOKEN}
    assert kwargs["timeout"] <= 3
    assert kwargs["allow_redirects"] is False


def test_bearer_token_sent_as_authorization(admin_get):
    _connector(token="Bearer a.jwt.value").get_notices("my-datasource")
    assert admin_get.call_args.kwargs["headers"] == {
        "Authorization": "Bearer a.jwt.value"
    }


def test_user_header_not_sent(admin_get):
    _connector(user="someone").get_notices("my-datasource")
    assert admin_get.call_args.kwargs["headers"] == {"X-DATAMESH-TOKEN": TOKEN}


def test_verify_passed_on(admin_get):
    _connector(verify=False).get_notices("my-datasource")
    assert admin_get.call_args.kwargs["verify"] is False


# Admin service url


@pytest.mark.parametrize(
    "host,expected",
    [
        ("datamesh.oceanum.io", "https://admin.oceanum.io"),
        ("datamesh.oceanum.tech", "https://admin.oceanum.tech"),
        ("DATAMESH.oceanum.io", "https://admin.oceanum.io"),
        ("datamesh.oceanum.io:443", None),
        ("datamesh.oceanum.io:8443", None),
        ("datamesh.staging.oceanum.io", None),
        ("datamesh-v0.oceanum.io", None),
        ("127.0.0.1:8000", None),
        ("10.0.0.5", None),
        ("gateway.datamesh.oceanum.io", None),
        ("localhost:8000", None),
        ("datamesh.example.com", None),
        ("datamesh.oceanum.io.evil.com", None),
        ("", None),
    ],
)
def test_admin_url_derived_from_datamesh_host(host, expected):
    assert admin_service_url(host) == expected


def test_admin_url_from_env(monkeypatch, admin_get):
    monkeypatch.setenv("OCEANUM_ADMIN_SERVICE", "http://localhost:9000/")
    assert admin_service_url("datamesh.oceanum.io") == "http://localhost:9000"
    _connector(service="http://localhost:8000").get_notices("my-ds")
    assert admin_get.call_args.args[0] == "http://localhost:9000/notices/active"


def test_unknown_admin_service_is_skipped_silently(admin_get):
    conn = _connector(service="http://localhost:8000")
    assert conn.get_notices("my-ds") == []
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        _load(conn)
    assert not _notice_warnings(record)
    admin_get.assert_not_called()


# Answers and failures


def test_notices_parsed(admin_get):
    admin_get.return_value = _response(200, [_notice("outage"), _notice("info")])
    notices = _connector().get_notices("my-ds")
    assert [n.severity for n in notices] == ["outage", "info"]
    assert isinstance(notices[0], Notice)
    assert notices[0].body == "**markdown**"
    assert notices[0].publish_at.year == 2026
    assert notices[0].expires_at is None


def test_malformed_notices_ignored(admin_get):
    admin_get.return_value = _response(
        200, [{"id": "x"}, "junk", None, 7, _notice("change")]
    )
    assert [n.severity for n in _connector().get_notices("my-ds")] == ["change"]


@pytest.mark.parametrize(
    "status", [204, 301, 302, 307, 308, 401, 404, 422, 429, 500, 502, 503, 504]
)
def test_error_statuses_are_no_notices(admin_get, status):
    admin_get.return_value = _response(status)
    assert _connector().get_notices("my-ds") == []


@pytest.mark.parametrize(
    "error",
    [
        requests.Timeout("slow"),
        requests.ConnectionError("down"),
        requests.TooManyRedirects("loop"),
        ValueError("boom"),
    ],
)
def test_request_errors_are_no_notices(admin_get, error):
    admin_get.side_effect = error
    assert _connector().get_notices("my-ds") == []


def test_unreadable_body_is_no_notices(admin_get):
    resp = _response(200)
    resp.json.side_effect = ValueError("not json")
    admin_get.return_value = resp
    assert _connector().get_notices("my-ds") == []


def test_errors_never_reach_load(admin_get):
    admin_get.side_effect = RuntimeError("unexpected")
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        assert _load(_connector()) is None
    assert not _notice_warnings(record)


def test_credential_never_logged(admin_get, caplog):
    admin_get.side_effect = requests.ConnectionError(f"failed with {TOKEN}")
    with caplog.at_level(logging.DEBUG, logger="oceanum.datamesh.notices"):
        _connector().get_notices("my-ds")
    assert caplog.records
    assert TOKEN not in caplog.text


@pytest.mark.parametrize(
    "answer",
    [
        _response(429, headers={"Retry-After": "30"}),
        _response(503, headers={"Retry-After": "5"}),
        requests.Timeout("slow"),
        requests.ConnectionError("down"),
    ],
    ids=["429", "503", "timeout", "connection-error"],
)
def test_backs_off_after_rate_limit_or_outage(admin_get, answer):
    if isinstance(answer, Exception):
        admin_get.side_effect = answer
    else:
        admin_get.return_value = answer
    conn = _connector()
    conn.get_notices("ds-1")
    assert conn.get_notices("ds-2") == []
    assert admin_get.call_count == 1


def test_backoff_uses_retry_after(admin_get):
    admin_get.return_value = _response(429, headers={"Retry-After": "30"})
    before = time.monotonic()
    _connector().get_notices("my-ds")
    until = notices_module._get_state().backoff[ADMIN]
    assert 29 <= until - before <= 31


# Caching


def test_one_request_per_datasource(admin_get):
    conn = _connector()
    conn.get_notices("ds-1")
    conn.get_notices("ds-1")
    _load(conn, "ds-1")
    _load(_connector(), "ds-1")
    assert admin_get.call_count == 1
    conn.get_notices("ds-2")
    assert admin_get.call_count == 2


def test_concurrent_requests_share_one_fetch(admin_get):
    release = threading.Event()

    def slow_get(*args, **kwargs):
        release.wait(5)
        return _response(200, [_notice("info")])

    admin_get.side_effect = slow_get
    conn = _connector()
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(conn.get_notices("my-ds")))
        for _ in range(4)
    ]
    for t in threads:
        t.start()
    release.set()
    for t in threads:
        t.join(5)
    assert admin_get.call_count == 1
    assert [len(r) for r in results] == [1, 1, 1, 1]


def test_cache_expires(admin_get, monkeypatch):
    conn = _connector()
    conn.get_notices("my-ds")
    monkeypatch.setattr(notices_module, "NOTICES_CACHE_TTL", 0.0)
    notices_module._clear()
    conn.get_notices("my-ds")
    conn.get_notices("my-ds")
    assert admin_get.call_count == 3


def test_cache_bounded(admin_get, monkeypatch):
    monkeypatch.setattr(notices_module, "NOTICES_CACHE_SIZE", 3)
    conn = _connector()
    for i in range(5):
        conn.get_notices(f"ds-{i}")
    assert len(notices_module._get_state().cache) == 3


def test_cache_per_credential(admin_get):
    _connector(token="token-a").get_notices("my-ds")
    _connector(token="token-b").get_notices("my-ds")
    assert admin_get.call_count == 2


# Shown on load


def test_warnings_by_severity(admin_get, caplog):
    admin_get.return_value = _response(
        200,
        [
            _notice("outage", title="Feed down"),
            _notice("action_required", title="Moving to v2"),
            _notice("change", title="New variable"),
            _notice("info", title="FYI"),
        ],
    )
    with (
        warnings.catch_warnings(record=True) as record,
        caplog.at_level(logging.INFO, logger="oceanum.datamesh.notices"),
    ):
        warnings.simplefilter("always")
        _load(_connector(), "my-datasource")
    messages = [str(w.message) for w in _notice_warnings(record)]
    assert len(messages) == 2
    assert "Feed down" in messages[0] and "outage" in messages[0]
    assert "Moving to v2" in messages[1] and "action required" in messages[1]
    assert all("my-datasource" in m for m in messages)
    assert all(issubclass(NoticeWarning, UserWarning) for _ in messages)
    info = [r for r in caplog.records if r.levelno == logging.INFO]
    assert len(info) == 2
    assert "New variable" in info[0].getMessage()
    assert "FYI" in info[1].getMessage()
    assert all("\n" not in r.getMessage() for r in info)


def test_unknown_severity_is_logged_not_warned(admin_get, caplog):
    admin_get.return_value = _response(200, [_notice("brand_new")])
    with (
        warnings.catch_warnings(record=True) as record,
        caplog.at_level(logging.INFO, logger="oceanum.datamesh.notices"),
    ):
        warnings.simplefilter("always")
        _load(_connector())
    assert not _notice_warnings(record)
    assert "brand new" in caplog.text


def test_warning_points_at_caller(admin_get):
    admin_get.return_value = _response(200, [_notice("outage")])
    conn = _connector()
    with (
        patch("oceanum.datamesh.connection.Session.acquire"),
        patch.object(conn, "_stage_request", return_value=None),
        warnings.catch_warnings(record=True) as record,
    ):
        warnings.simplefilter("always")
        conn.load_datasource("my-ds")
    assert _notice_warnings(record)[0].filename == __file__


def test_each_notice_shown_once_per_process(admin_get):
    admin_get.return_value = _response(200, [_notice("outage")])
    conn = _connector()
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        _load(conn)
        _load(conn)
        notices_module._get_state().cache.clear()
        _load(conn)
    assert admin_get.call_count == 2
    assert len(_notice_warnings(record)) == 1


def test_get_notices_does_not_count_as_shown(admin_get):
    admin_get.return_value = _response(200, [_notice("outage")])
    conn = _connector()
    conn.get_notices("my-datasource")
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        _load(conn)
    assert len(_notice_warnings(record)) == 1


def test_nothing_shown_when_the_operation_fails(admin_get):
    admin_get.return_value = _response(200, [_notice("outage")])
    conn = _connector()
    with (
        patch("oceanum.datamesh.connection.Session.acquire"),
        patch.object(
            conn, "_stage_request", side_effect=RuntimeError("datamesh failed")
        ),
        warnings.catch_warnings(record=True) as record,
    ):
        warnings.simplefilter("always")
        with pytest.raises(RuntimeError, match="datamesh failed"):
            conn.load_datasource("my-ds")
    assert not _notice_warnings(record)


def test_slow_admin_does_not_delay_load(admin_get, monkeypatch):
    monkeypatch.setattr("oceanum.datamesh.connection.NOTICES_TIMEOUT", 0.2)
    release = threading.Event()

    def hanging_get(*args, **kwargs):
        release.wait(10)
        return _response(200, [_notice("outage")])

    admin_get.side_effect = hanging_get
    conn = _connector()
    try:
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            start = time.monotonic()
            _load(conn)
            elapsed = time.monotonic() - start
        assert elapsed < 1.0
        assert not _notice_warnings(record)
    finally:
        release.set()


def test_query_entry_points(admin_get):
    admin_get.return_value = _response(200, [_notice("outage")])
    conn = _connector()
    with patch.object(conn, "_query", return_value=None):
        conn.query(Query(datasource="ds-query-object"))
        conn.query({"datasource": "ds-query-dict"})
        conn.query(datasource="ds-query-keys")
    asked = [c.kwargs["params"]["datasource"] for c in admin_get.call_args_list]
    assert asked == ["ds-query-object", "ds-query-dict", "ds-query-keys"]


def test_query_async(admin_get):
    import asyncio

    conn = _connector()
    with patch.object(conn, "_query", return_value=None):
        asyncio.run(conn.query_async({"datasource": "ds-async"}))
    assert admin_get.call_args.kwargs["params"] == {"datasource": "ds-async"}


def test_get_datasource(admin_get):
    admin_get.return_value = _response(200, [_notice("action_required")])
    conn = _connector()
    meta = {"id": "my-ds", "name": "my-ds", "driver": "null", "geom": None}
    with (
        patch.object(conn, "_get_datasource_metadata", return_value=meta),
        warnings.catch_warnings(record=True) as record,
    ):
        warnings.simplefilter("always")
        conn.get_datasource("my-ds")
    assert len(_notice_warnings(record)) == 1


# Opt-out


@pytest.mark.parametrize("value", ["0", "false", "no", "off", "OFF"])
def test_opt_out_env(admin_get, monkeypatch, value):
    monkeypatch.setenv("OCEANUM_NOTICES", value)
    _load(_connector())
    admin_get.assert_not_called()


def test_opt_out_kwarg(admin_get):
    _load(_connector(notices=False))
    admin_get.assert_not_called()


def test_kwarg_overrides_env(admin_get, monkeypatch):
    monkeypatch.setenv("OCEANUM_NOTICES", "0")
    _load(_connector(notices=True))
    admin_get.assert_called_once()


def test_get_notices_works_when_opted_out(admin_get):
    admin_get.return_value = _response(200, [_notice("info")])
    assert len(_connector(notices=False).get_notices("my-ds")) == 1


def test_thread_failure_is_no_notices(admin_get):
    conn = _connector()
    with patch.object(
        threading.Thread, "start", side_effect=RuntimeError("no threads")
    ):
        assert conn.get_notices("my-ds") == []
    start = time.monotonic()
    assert conn.get_notices("my-ds") == []
    assert time.monotonic() - start < 0.5
    admin_get.assert_not_called()


def test_redirect_not_followed(admin_get):
    """A redirect would carry X-DATAMESH-TOKEN to wherever it points."""
    resp = _response(302, headers={"Location": "https://elsewhere.example.com/"})
    admin_get.return_value = resp
    assert _connector().get_notices("my-ds") == []
    assert admin_get.call_count == 1
    assert admin_get.call_args.kwargs["allow_redirects"] is False


def test_server_text_sanitised(admin_get, caplog):
    evil = "Feed\x1b[2J down\nINFO fake line\r\x00\u2028" + "x" * 500
    admin_get.return_value = _response(
        200,
        [
            _notice("outage", nid="a", title=evil),
            _notice("info", nid="b", title=evil),
        ],
    )
    with (
        warnings.catch_warnings(record=True) as record,
        caplog.at_level(logging.INFO, logger="oceanum.datamesh.notices"),
    ):
        warnings.simplefilter("always")
        _load(_connector())
    shown = [str(w.message) for w in _notice_warnings(record)]
    shown += [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert len(shown) == 2
    for text in shown:
        assert "Feed [2J down INFO fake line" in text
        assert not any(c in text for c in "\x1b\n\r\x00\u2028")
        assert len(text) < 400


def test_returned_notices_are_copies(admin_get):
    admin_get.return_value = _response(200, [_notice("outage", title="Original")])
    conn = _connector()
    first = conn.get_notices("my-ds")
    first[0].title = "Changed"
    first.clear()
    again = conn.get_notices("my-ds")
    assert [n.title for n in again] == ["Original"]
    assert admin_get.call_count == 1


def test_failure_does_not_suppress_later_warnings(admin_get):
    admin_get.return_value = _response(500)
    conn = _connector()
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        _load(conn)
        assert not _notice_warnings(record)
        # The cached failure expires.
        notices_module._get_state().cache.clear()
        admin_get.return_value = _response(200, [_notice("outage")])
        _load(conn)
    assert len(_notice_warnings(record)) == 1


def test_shown_once_per_datasource(admin_get):
    admin_get.return_value = _response(200, [_notice("outage", nid="shared")])
    conn = _connector()
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        _load(conn, "ds-one")
        _load(conn, "ds-two")
        _load(conn, "ds-one")
    messages = [str(w.message) for w in _notice_warnings(record)]
    assert len(messages) == 2
    assert "ds-one" in messages[0] and "ds-two" in messages[1]


@pytest.mark.parametrize(
    "value,expected",
    [
        ("https://admin.example.com/", "https://admin.example.com"),
        ("https://admin.example.com:8443/base", "https://admin.example.com:8443/base"),
        ("http://localhost:9000", "http://localhost:9000"),
        ("http://127.0.0.1:9000/", "http://127.0.0.1:9000"),
        ("http://admin.oceanum.io", None),
        ("http://10.0.0.5:9000", None),
        ("ftp://admin.example.com", None),
        ("admin.example.com", None),
        ("https://user:pw@admin.example.com", None),
        ("https://admin.example.com/?x=1", None),
        ("https://", None),
        ("http://[::1", None),
    ],
)
def test_admin_url_env_validated(monkeypatch, value, expected):
    monkeypatch.setenv("OCEANUM_ADMIN_SERVICE", value)
    assert admin_service_url("datamesh.oceanum.io") == expected


def test_invalid_env_never_falls_back_to_production(admin_get, monkeypatch):
    monkeypatch.setenv("OCEANUM_ADMIN_SERVICE", "http://admin.example.com")
    conn = _connector()
    assert conn.get_notices("my-ds") == []
    _load(conn)
    admin_get.assert_not_called()


@pytest.mark.parametrize(
    "service",
    [
        "http://localhost:8000",
        "https://datamesh.staging.oceanum.io",
        "https://10.0.0.5",
        "https://datamesh.customer.example.com",
    ],
)
def test_other_datamesh_hosts_make_no_request(admin_get, service):
    conn = _connector(service=service)
    assert conn.get_notices("my-ds") == []
    _load(conn)
    admin_get.assert_not_called()


def test_answer_limits_enforced(admin_get):
    items = [
        dict(
            _notice("info", nid=f"n{i:02d}"),
            publish_at=f"2026-10-{i + 1:02d}T00:00:00Z",
            title="t" * 500,
            body="b" * 5000,
        )
        for i in (
            3,
            17,
            0,
            24,
            9,
            12,
            1,
            22,
            5,
            14,
            19,
            7,
            2,
            21,
            11,
            16,
            4,
            23,
            8,
            13,
            18,
            6,
            10,
            20,
            15,
        )
    ]
    admin_get.return_value = _response(200, items)
    notices = _connector().get_notices("my-ds")
    assert len(notices) == 20
    assert [n.id for n in notices] == [f"n{i:02d}" for i in range(24, 4, -1)]
    assert all(len(n.title) == 200 for n in notices)
    assert all(len(n.body) == 2000 for n in notices)


def test_overall_deadline(admin_get, monkeypatch):
    monkeypatch.setattr(notices_module, "NOTICES_TIMEOUT", 0.2)
    release = threading.Event()

    def hanging_get(*args, **kwargs):
        release.wait(10)
        return _response(200, [_notice("outage")])

    admin_get.side_effect = hanging_get
    try:
        start = time.monotonic()
        assert _connector().get_notices("my-ds") == []
        assert time.monotonic() - start < 1.0
    finally:
        release.set()


def test_datasource_id_normalised(admin_get):
    conn = _connector()
    conn.get_notices("  my-ds\n")
    conn.get_notices("my-ds")
    assert admin_get.call_count == 1
    assert admin_get.call_args.kwargs["params"] == {"datasource": "my-ds"}
    assert conn.get_notices("   ") == []
    assert conn.get_notices(None) == []
    assert admin_get.call_count == 1
