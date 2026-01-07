import asyncio
import copy
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class EndpointConfig:
    name: str
    method: str
    url: str
    timeout: float = 5.0
    retries: int = 0
    retry_statuses: List[int] = field(default_factory=lambda: [500, 502, 503])
    headers: Dict[str, str] = field(default_factory=dict)


class TraceSink:
    """Very small trace sink used by the engine.

    Stores a list of events as (event_type, payload_copy) tuples.
    Payloads are copied on emit so later mutation of the input
    does not affect stored events.
    """

    def __init__(self) -> None:
        self.events: List[Dict[str, Any]] = []

    def emit(self, event_type: str, payload: Dict[str, Any]) -> None:
        data = copy.deepcopy(payload)
        data["type"] = event_type
        self.events.append(data)


class ApiEngine:
    """Simple API orchestration layer.

    This implementation is intentionally a bit repetitive between
    call_sync and call_async so that it can be refactored later
    without changing behavior.
    """

    def __init__(
        self,
        endpoints: Dict[str, EndpointConfig],
        transport: Any,
        tracer: Optional[TraceSink] = None,
        default_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self._endpoints = endpoints
        self._transport = transport
        self._tracer = tracer or TraceSink()
        self._default_headers = default_headers or {}

    @property
    def tracer(self) -> TraceSink:
        return self._tracer

    # Result shape is intentionally simple but stable.
    def call_sync(self, ctx: Dict[str, Any], endpoint_name: str, raw_payload: str) -> Dict[str, Any]:
        tracer = self._tracer
        tracer.emit("resolve.start", {"endpoint": endpoint_name})

        endpoint = self._endpoints.get(endpoint_name)
        if endpoint is None:
            tracer.emit("resolve.error", {"endpoint": endpoint_name, "error": "unknown_endpoint"})
            return {
                "ok": False,
                "status": None,
                "data": None,
                "error": "unknown_endpoint",
            }

        tracer.emit("resolve.done", {"endpoint": endpoint_name})

        # Parse payload
        try:
            parsed = json.loads(raw_payload) if raw_payload is not None else None
        except json.JSONDecodeError as exc:
            tracer.emit("payload.error", {"endpoint": endpoint_name, "error": "bad_payload", "message": str(exc)})
            return {
                "ok": False,
                "status": None,
                "data": None,
                "error": "bad_payload",
            }

        # Build headers with precedence: engine defaults < endpoint headers < ctx overrides
        ctx_headers = ctx.get("headers") or {}
        headers: Dict[str, str] = {}
        headers.update(self._default_headers)
        headers.update(endpoint.headers)
        headers.update(ctx_headers)

        tracer.emit(
            "request.build",
            {
                "endpoint": endpoint_name,
                "method": endpoint.method,
                "url": endpoint.url,
                "headers": headers,
            },
        )

        attempt = 0
        last_error: Optional[str] = None
        while True:
            attempt += 1
            tracer.emit(
                "request.start",
                {
                    "endpoint": endpoint_name,
                    "attempt": attempt,
                },
            )
            try:
                response = self._transport.sync_request(
                    method=endpoint.method,
                    url=endpoint.url,
                    headers=headers,
                    json_body=parsed,
                    timeout=endpoint.timeout,
                )
            except TimeoutError:
                tracer.emit(
                    "request.error",
                    {
                        "endpoint": endpoint_name,
                        "attempt": attempt,
                        "category": "timeout",
                    },
                )
                last_error = "timeout"
                if attempt > endpoint.retries:
                    return {
                        "ok": False,
                        "status": None,
                        "data": None,
                        "error": "timeout",
                    }
                continue
            except Exception as exc:  # noqa: BLE001
                tracer.emit(
                    "request.error",
                    {
                        "endpoint": endpoint_name,
                        "attempt": attempt,
                        "category": "network_error",
                        "message": str(exc),
                    },
                )
                last_error = "network_error"
                return {
                    "ok": False,
                    "status": None,
                    "data": None,
                    "error": last_error,
                }

            status = response.get("status")
            body = response.get("body")
            tracer.emit(
                "request.end",
                {
                    "endpoint": endpoint_name,
                    "attempt": attempt,
                    "status": status,
                },
            )

            if status in endpoint.retry_statuses and attempt <= endpoint.retries:
                tracer.emit(
                    "request.retry",
                    {
                        "endpoint": endpoint_name,
                        "attempt": attempt,
                        "status": status,
                    },
                )
                last_error = f"http_{status}"
                continue

            # Normalize response
            try:
                data = json.loads(body) if isinstance(body, str) and body else body
            except json.JSONDecodeError as exc:  # malformed JSON from upstream
                tracer.emit(
                    "response.error",
                    {
                        "endpoint": endpoint_name,
                        "status": status,
                        "category": "bad_response_json",
                        "message": str(exc),
                    },
                )
                return {
                    "ok": False,
                    "status": status,
                    "data": None,
                    "error": "bad_response_json",
                }

            if 200 <= status < 300:
                tracer.emit(
                    "response.ok",
                    {
                        "endpoint": endpoint_name,
                        "status": status,
                    },
                )
                return {
                    "ok": True,
                    "status": status,
                    "data": data,
                    "error": None,
                }

            # non‑2xx final error
            tracer.emit(
                "response.error",
                {
                    "endpoint": endpoint_name,
                    "status": status,
                    "category": "upstream_error",
                },
            )
            return {
                "ok": False,
                "status": status,
                "data": data,
                "error": f"upstream_error:{status}",
            }

    async def call_async(self, ctx: Dict[str, Any], endpoint_name: str, raw_payload: str) -> Dict[str, Any]:
        tracer = self._tracer
        tracer.emit("resolve.start", {"endpoint": endpoint_name})

        endpoint = self._endpoints.get(endpoint_name)
        if endpoint is None:
            tracer.emit("resolve.error", {"endpoint": endpoint_name, "error": "unknown_endpoint"})
            return {
                "ok": False,
                "status": None,
                "data": None,
                "error": "unknown_endpoint",
            }

        tracer.emit("resolve.done", {"endpoint": endpoint_name})

        try:
            parsed = json.loads(raw_payload) if raw_payload is not None else None
        except json.JSONDecodeError as exc:
            tracer.emit("payload.error", {"endpoint": endpoint_name, "error": "bad_payload", "message": str(exc)})
            return {
                "ok": False,
                "status": None,
                "data": None,
                "error": "bad_payload",
            }

        ctx_headers = ctx.get("headers") or {}
        headers: Dict[str, str] = {}
        headers.update(self._default_headers)
        headers.update(endpoint.headers)
        headers.update(ctx_headers)

        tracer.emit(
            "request.build",
            {
                "endpoint": endpoint_name,
                "method": endpoint.method,
                "url": endpoint.url,
                "headers": headers,
            },
        )

        attempt = 0
        last_error: Optional[str] = None
        while True:
            attempt += 1
            tracer.emit(
                "request.start",
                {
                    "endpoint": endpoint_name,
                    "attempt": attempt,
                },
            )
            try:
                response = await self._transport.async_request(
                    method=endpoint.method,
                    url=endpoint.url,
                    headers=headers,
                    json_body=parsed,
                    timeout=endpoint.timeout,
                )
            except asyncio.TimeoutError:
                tracer.emit(
                    "request.error",
                    {
                        "endpoint": endpoint_name,
                        "attempt": attempt,
                        "category": "timeout",
                    },
                )
                last_error = "timeout"
                if attempt > endpoint.retries:
                    return {
                        "ok": False,
                        "status": None,
                        "data": None,
                        "error": "timeout",
                    }
                continue
            except Exception as exc:  # noqa: BLE001
                tracer.emit(
                    "request.error",
                    {
                        "endpoint": endpoint_name,
                        "attempt": attempt,
                        "category": "network_error",
                        "message": str(exc),
                    },
                )
                last_error = "network_error"
                return {
                    "ok": False,
                    "status": None,
                    "data": None,
                    "error": last_error,
                }

            status = response.get("status")
            body = response.get("body")
            tracer.emit(
                "request.end",
                {
                    "endpoint": endpoint_name,
                    "attempt": attempt,
                    "status": status,
                },
            )

            if status in endpoint.retry_statuses and attempt <= endpoint.retries:
                tracer.emit(
                    "request.retry",
                    {
                        "endpoint": endpoint_name,
                        "attempt": attempt,
                        "status": status,
                    },
                )
                last_error = f"http_{status}"
                continue

            try:
                data = json.loads(body) if isinstance(body, str) and body else body
            except json.JSONDecodeError as exc:
                tracer.emit(
                    "response.error",
                    {
                        "endpoint": endpoint_name,
                        "status": status,
                        "category": "bad_response_json",
                        "message": str(exc),
                    },
                )
                return {
                    "ok": False,
                    "status": status,
                    "data": None,
                    "error": "bad_response_json",
                }

            if 200 <= status < 300:
                tracer.emit(
                    "response.ok",
                    {
                        "endpoint": endpoint_name,
                        "status": status,
                    },
                )
                return {
                    "ok": True,
                    "status": status,
                    "data": data,
                    "error": None,
                }

            tracer.emit(
                "response.error",
                {
                    "endpoint": endpoint_name,
                    "status": status,
                    "category": "upstream_error",
                },
            )
            return {
                "ok": False,
                "status": status,
                "data": data,
                "error": f"upstream_error:{status}",
            }