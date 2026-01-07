import asyncio
import json
from typing import Any, Dict, List

import pytest

from api_engine import ApiEngine, EndpointConfig, TraceSink


class DummyTransport:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []
        self._responses: Dict[str, List[Dict[str, Any]]] = {}
        self._sync_errors: Dict[str, List[BaseException]] = {}
        self._async_errors: Dict[str, List[BaseException]] = {}

    def queue_response(self, url: str, response: Dict[str, Any]) -> None:
        self._responses.setdefault(url, []).append(response)

    def queue_sync_error(self, url: str, exc: BaseException) -> None:
        self._sync_errors.setdefault(url, []).append(exc)

    def queue_async_error(self, url: str, exc: BaseException) -> None:
        self._async_errors.setdefault(url, []).append(exc)

    def sync_request(self, *, method: str, url: str, headers: Dict[str, str], json_body: Any, timeout: float) -> Dict[str, Any]:
        self.calls.append({"kind": "sync", "method": method, "url": url, "headers": headers, "body": json_body, "timeout": timeout})
        if self._sync_errors.get(url):
            raise self._sync_errors[url].pop(0)
        return self._responses[url].pop(0)

    async def async_request(self, *, method: str, url: str, headers: Dict[str, str], json_body: Any, timeout: float) -> Dict[str, Any]:
        self.calls.append({"kind": "async", "method": method, "url": url, "headers": headers, "body": json_body, "timeout": timeout})
        if self._async_errors.get(url):
            raise self._async_errors[url].pop(0)
        return self._responses[url].pop(0)


def make_engine() -> ApiEngine:
    tracer = TraceSink()
    transport = DummyTransport()
    endpoints = {
        "ok": EndpointConfig(name="ok", method="GET", url="/ok", retries=0),
        "retry": EndpointConfig(name="retry", method="GET", url="/retry", retries=2, retry_statuses=[500]),
    }
    return ApiEngine(endpoints=endpoints, transport=transport, tracer=tracer, default_headers={"X-Default": "1"})


def test_unknown_endpoint_returns_category_and_traces() -> None:
    engine = make_engine()
    result = engine.call_sync({}, "missing", "{}")

    assert result["ok"] is False
    assert result["error"] == "unknown_endpoint"

    types = [e["type"] for e in engine.tracer.events]
    assert types == ["resolve.start", "resolve.error"]


def test_bad_payload_returns_error_and_does_not_call_transport() -> None:
    engine = make_engine()
    result = engine.call_sync({}, "ok", "{not-json}")

    assert result["ok"] is False
    assert result["error"] == "bad_payload"

    assert engine.tracer.events[0]["type"] == "resolve.start"
    assert engine.tracer.events[-1]["type"] == "payload.error"

    # transport should never be called
    assert not engine._transport.calls


def test_headers_merge_precedence() -> None:
    engine = make_engine()
    engine._endpoints["ok"].headers["X-Endpoint"] = "E"

    engine._transport.queue_response("/ok", {"status": 200, "body": json.dumps({"a": 1})})
    ctx = {"headers": {"X-Default": "ctx-override", "X-Req": "R"}}

    result = engine.call_sync(ctx, "ok", "{}")
    assert result["ok"] is True

    call = engine._transport.calls[0]
    # default < endpoint < ctx
    assert call["headers"]["X-Default"] == "ctx-override"
    assert call["headers"]["X-Endpoint"] == "E"
    assert call["headers"]["X-Req"] == "R"


def test_retry_and_timeout_mapping_sync() -> None:
    engine = make_engine()
    endpoint = engine._endpoints["retry"]

    # first: timeout, then 500, then 500 (no more retries)
    engine._transport.queue_sync_error("/retry", TimeoutError())
    engine._transport.queue_response("/retry", {"status": 500, "body": "{}"})
    engine._transport.queue_response("/retry", {"status": 500, "body": "{}"})

    result = engine.call_sync({}, "retry", "{}")

    assert result["ok"] is False
    # final error from status mapping
    assert result["error"] == "upstream_error:500"

    types = [e["type"] for e in engine.tracer.events]
    # should include resolve, request.start/end, request.retry etc.
    assert "request.retry" in types


@pytest.mark.asyncio
async def test_retry_and_timeout_mapping_async() -> None:
    engine = make_engine()
    endpoint = engine._endpoints["retry"]

    # first: timeout, then 500, then 500 (no more retries)
    engine._transport.queue_async_error("/retry", asyncio.TimeoutError())
    engine._transport.queue_response("/retry", {"status": 500, "body": "{}"})
    engine._transport.queue_response("/retry", {"status": 500, "body": "{}"})

    result = await engine.call_async({}, "retry", "{}")

    assert result["ok"] is False
    # final error from status mapping
    assert result["error"] == "upstream_error:500"

    types = [e["type"] for e in engine.tracer.events]
    assert "request.retry" in types


@pytest.mark.asyncio
async def test_async_behaves_like_sync_success() -> None:
    engine = make_engine()
    engine._transport.queue_response("/ok", {"status": 200, "body": json.dumps({"v": 1})})
    engine._transport.queue_response("/ok", {"status": 200, "body": json.dumps({"v": 1})})

    sync_result = engine.call_sync({}, "ok", "{}")
    async_result = await engine.call_async({}, "ok", "{}")

    assert sync_result == async_result


def test_trace_payload_is_copied() -> None:
    tracer = TraceSink()
    payload = {"endpoint": "x", "meta": {"a": 1}}
    tracer.emit("test", payload)

    # mutate original
    payload["endpoint"] = "y"
    payload["meta"]["a"] = 2

    stored = tracer.events[0]
    assert stored["endpoint"] == "x"
    assert stored["meta"]["a"] == 1