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


class _RequestContext:
    """Internal class that holds state for a single request operation.

    Centralizes parsed endpoint, headers, and payload to avoid
    re-computing or re-passing them through the call stack.
    """

    def __init__(
        self,
        endpoint_name: str,
        endpoint: EndpointConfig,
        parsed_payload: Any,
        headers: Dict[str, str],
    ) -> None:
        self.endpoint_name = endpoint_name
        self.endpoint = endpoint
        self.parsed_payload = parsed_payload
        self.headers = headers


class ApiEngine:
    """Simple API orchestration layer.

    This implementation centralizes common request/response logic
    to reduce duplication between sync and async paths.
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

    def _resolve_endpoint(self, endpoint_name: str) -> Optional[EndpointConfig]:
        """Resolve endpoint config by name. Emits trace events."""
        self._tracer.emit("resolve.start", {"endpoint": endpoint_name})
        endpoint = self._endpoints.get(endpoint_name)
        if endpoint is None:
            self._tracer.emit("resolve.error", {"endpoint": endpoint_name, "error": "unknown_endpoint"})
            return None
        self._tracer.emit("resolve.done", {"endpoint": endpoint_name})
        return endpoint

    def _parse_payload(self, endpoint_name: str, raw_payload: str) -> tuple[Optional[Any], Optional[Dict[str, Any]]]:
        """Parse raw_payload as JSON. Returns (parsed_data, error_response).

        If parsing succeeds, error_response is None.
        If parsing fails, returns (None, error_response) with the error already traced.
        """
        try:
            parsed = json.loads(raw_payload) if raw_payload is not None else None
            return parsed, None
        except json.JSONDecodeError as exc:
            self._tracer.emit(
                "payload.error",
                {"endpoint": endpoint_name, "error": "bad_payload", "message": str(exc)},
            )
            return None, {
                "ok": False,
                "status": None,
                "data": None,
                "error": "bad_payload",
            }

    def _build_headers(self, ctx: Dict[str, Any], endpoint: EndpointConfig) -> Dict[str, str]:
        """Build merged headers: defaults < endpoint < ctx overrides."""
        ctx_headers = ctx.get("headers") or {}
        headers: Dict[str, str] = {}
        headers.update(self._default_headers)
        headers.update(endpoint.headers)
        headers.update(ctx_headers)
        return headers

    def _emit_request_build(self, endpoint_name: str, endpoint: EndpointConfig, headers: Dict[str, str]) -> None:
        """Emit request.build trace event."""
        self._tracer.emit(
            "request.build",
            {
                "endpoint": endpoint_name,
                "method": endpoint.method,
                "url": endpoint.url,
                "headers": headers,
            },
        )

    def _prepare_call(self, ctx: Dict[str, Any], endpoint_name: str, raw_payload: str) -> tuple[Optional[_RequestContext], Optional[Dict[str, Any]]]:
        """Prepare common request context. Returns (request_ctx, error_response).

        If successful, error_response is None.
        If failed, request_ctx is None and error_response is the error dict.
        """
        endpoint = self._resolve_endpoint(endpoint_name)
        if endpoint is None:
            return None, {
                "ok": False,
                "status": None,
                "data": None,
                "error": "unknown_endpoint",
            }

        parsed, parse_error = self._parse_payload(endpoint_name, raw_payload)
        if parse_error is not None:
            return None, parse_error

        headers = self._build_headers(ctx, endpoint)
        self._emit_request_build(endpoint_name, endpoint, headers)

        request_ctx = _RequestContext(endpoint_name, endpoint, parsed, headers)
        return request_ctx, None

    def _normalize_response_body(self, body: Any) -> tuple[Any, Optional[str]]:
        """Normalize response body (parse JSON if needed).

        Returns (data, error_string).
        If successful, error_string is None.
        If JSON parsing fails, returns (None, 'bad_response_json').
        """
        try:
            data = json.loads(body) if isinstance(body, str) and body else body
            return data, None
        except json.JSONDecodeError:
            return None, "bad_response_json"

    def _handle_success_response(
        self, endpoint_name: str, status: int, data: Any
    ) -> Dict[str, Any]:
        """Handle successful (2xx) response."""
        self._tracer.emit(
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

    def _handle_error_response(
        self, endpoint_name: str, status: int, data: Any, error_type: str
    ) -> Dict[str, Any]:
        """Handle error response."""
        self._tracer.emit(
            "response.error",
            {
                "endpoint": endpoint_name,
                "status": status,
                "category": error_type,
            },
        )
        return {
            "ok": False,
            "status": status,
            "data": data,
            "error": f"{error_type}:{status}",
        }

    # Result shape is intentionally simple but stable.
    def call_sync(self, ctx: Dict[str, Any], endpoint_name: str, raw_payload: str) -> Dict[str, Any]:
        request_ctx, error = self._prepare_call(ctx, endpoint_name, raw_payload)
        if error is not None:
            return error

        # Execute request with retry logic
        return self._execute_sync_with_retry(request_ctx)

    def _execute_sync_with_retry(self, request_ctx: _RequestContext) -> Dict[str, Any]:
        """Execute sync request with retry logic."""
        attempt = 0
        while True:
            attempt += 1
            self._tracer.emit(
                "request.start",
                {
                    "endpoint": request_ctx.endpoint_name,
                    "attempt": attempt,
                },
            )
            try:
                response = self._transport.sync_request(
                    method=request_ctx.endpoint.method,
                    url=request_ctx.endpoint.url,
                    headers=request_ctx.headers,
                    json_body=request_ctx.parsed_payload,
                    timeout=request_ctx.endpoint.timeout,
                )
            except TimeoutError:
                self._tracer.emit(
                    "request.error",
                    {
                        "endpoint": request_ctx.endpoint_name,
                        "attempt": attempt,
                        "category": "timeout",
                    },
                )
                if attempt > request_ctx.endpoint.retries:
                    return {
                        "ok": False,
                        "status": None,
                        "data": None,
                        "error": "timeout",
                    }
                continue
            except Exception as exc:  # noqa: BLE001
                self._tracer.emit(
                    "request.error",
                    {
                        "endpoint": request_ctx.endpoint_name,
                        "attempt": attempt,
                        "category": "network_error",
                        "message": str(exc),
                    },
                )
                return {
                    "ok": False,
                    "status": None,
                    "data": None,
                    "error": "network_error",
                }

            status = response.get("status")
            body = response.get("body")
            self._tracer.emit(
                "request.end",
                {
                    "endpoint": request_ctx.endpoint_name,
                    "attempt": attempt,
                    "status": status,
                },
            )

            # Check if we should retry
            if status in request_ctx.endpoint.retry_statuses and attempt <= request_ctx.endpoint.retries:
                self._tracer.emit(
                    "request.retry",
                    {
                        "endpoint": request_ctx.endpoint_name,
                        "attempt": attempt,
                        "status": status,
                    },
                )
                continue

            # Normalize and return response
            return self._normalize_and_return_response(request_ctx.endpoint_name, status, body)

    def _normalize_and_return_response(self, endpoint_name: str, status: int, body: Any) -> Dict[str, Any]:
        """Normalize response body and return appropriate result based on status."""
        data, normalize_error = self._normalize_response_body(body)
        
        if normalize_error:
            # JSON decode error from upstream
            self._tracer.emit(
                "response.error",
                {
                    "endpoint": endpoint_name,
                    "status": status,
                    "category": normalize_error,
                },
            )
            return {
                "ok": False,
                "status": status,
                "data": None,
                "error": normalize_error,
            }

        if 200 <= status < 300:
            return self._handle_success_response(endpoint_name, status, data)
        else:
            # non-2xx final error
            return self._handle_error_response(endpoint_name, status, data, "upstream_error")

    async def call_async(self, ctx: Dict[str, Any], endpoint_name: str, raw_payload: str) -> Dict[str, Any]:
        request_ctx, error = self._prepare_call(ctx, endpoint_name, raw_payload)
        if error is not None:
            return error

        # Execute request with retry logic
        return await self._execute_async_with_retry(request_ctx)

    async def _execute_async_with_retry(self, request_ctx: _RequestContext) -> Dict[str, Any]:
        """Execute async request with retry logic."""
        attempt = 0
        while True:
            attempt += 1
            self._tracer.emit(
                "request.start",
                {
                    "endpoint": request_ctx.endpoint_name,
                    "attempt": attempt,
                },
            )
            try:
                response = await self._transport.async_request(
                    method=request_ctx.endpoint.method,
                    url=request_ctx.endpoint.url,
                    headers=request_ctx.headers,
                    json_body=request_ctx.parsed_payload,
                    timeout=request_ctx.endpoint.timeout,
                )
            except asyncio.TimeoutError:
                self._tracer.emit(
                    "request.error",
                    {
                        "endpoint": request_ctx.endpoint_name,
                        "attempt": attempt,
                        "category": "timeout",
                    },
                )
                if attempt > request_ctx.endpoint.retries:
                    return {
                        "ok": False,
                        "status": None,
                        "data": None,
                        "error": "timeout",
                    }
                continue
            except Exception as exc:  # noqa: BLE001
                self._tracer.emit(
                    "request.error",
                    {
                        "endpoint": request_ctx.endpoint_name,
                        "attempt": attempt,
                        "category": "network_error",
                        "message": str(exc),
                    },
                )
                return {
                    "ok": False,
                    "status": None,
                    "data": None,
                    "error": "network_error",
                }

            status = response.get("status")
            body = response.get("body")
            self._tracer.emit(
                "request.end",
                {
                    "endpoint": request_ctx.endpoint_name,
                    "attempt": attempt,
                    "status": status,
                },
            )

            # Check if we should retry
            if status in request_ctx.endpoint.retry_statuses and attempt <= request_ctx.endpoint.retries:
                self._tracer.emit(
                    "request.retry",
                    {
                        "endpoint": request_ctx.endpoint_name,
                        "attempt": attempt,
                        "status": status,
                    },
                )
                continue

            # Normalize and return response
            return self._normalize_and_return_response(request_ctx.endpoint_name, status, body)