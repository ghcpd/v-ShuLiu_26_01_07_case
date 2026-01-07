import asyncio
import copy
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


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

    This implementation centralizes common logic across sync and async paths
    to reduce duplication while maintaining identical observable behavior.
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
        """Resolve endpoint configuration and emit trace events."""
        return self._endpoints.get(endpoint_name)

    def _parse_payload(self, raw_payload: Optional[str]) -> Tuple[Any, Optional[str]]:
        """Parse JSON payload. Returns (parsed_data, error_if_any)."""
        try:
            parsed = json.loads(raw_payload) if raw_payload is not None else None
            return parsed, None
        except json.JSONDecodeError as exc:
            return None, "bad_payload"

    def _build_headers(self, ctx: Dict[str, Any], endpoint: EndpointConfig) -> Dict[str, str]:
        """Merge headers: engine defaults < endpoint headers < ctx overrides."""
        ctx_headers = ctx.get("headers") or {}
        headers: Dict[str, str] = {}
        headers.update(self._default_headers)
        headers.update(endpoint.headers)
        headers.update(ctx_headers)
        return headers

    def _normalize_response_body(self, body: Any) -> Tuple[Any, Optional[str]]:
        """Parse response body JSON if needed. Returns (data, error_if_any)."""
        try:
            data = json.loads(body) if isinstance(body, str) and body else body
            return data, None
        except json.JSONDecodeError:
            return None, "bad_response_json"

    def _build_error_response(
        self,
        ok: bool,
        status: Optional[int],
        data: Any,
        error: str,
    ) -> Dict[str, Any]:
        """Build standard response object."""
        return {
            "ok": ok,
            "status": status,
            "data": data,
            "error": error if not ok else None,
        }

    def _map_http_status(self, status: int) -> str:
        """Map HTTP status to error category."""
        if 200 <= status < 300:
            return "ok"
        return f"upstream_error:{status}"

    def call_sync(self, ctx: Dict[str, Any], endpoint_name: str, raw_payload: str) -> Dict[str, Any]:
        """Synchronous API call with retry and error mapping."""
        tracer = self._tracer
        tracer.emit("resolve.start", {"endpoint": endpoint_name})

        endpoint = self._resolve_endpoint(endpoint_name)
        if endpoint is None:
            tracer.emit("resolve.error", {"endpoint": endpoint_name, "error": "unknown_endpoint"})
            return self._build_error_response(False, None, None, "unknown_endpoint")

        tracer.emit("resolve.done", {"endpoint": endpoint_name})

        # Parse payload
        parsed, payload_error = self._parse_payload(raw_payload)
        if payload_error:
            tracer.emit("payload.error", {"endpoint": endpoint_name, "error": payload_error, "message": ""})
            return self._build_error_response(False, None, None, payload_error)

        # Build headers
        headers = self._build_headers(ctx, endpoint)
        tracer.emit(
            "request.build",
            {
                "endpoint": endpoint_name,
                "method": endpoint.method,
                "url": endpoint.url,
                "headers": headers,
            },
        )

        # Retry loop using sync transport
        return self._dispatch_sync(endpoint_name, endpoint, parsed, headers)

    def _dispatch_sync(
        self,
        endpoint_name: str,
        endpoint: EndpointConfig,
        parsed: Any,
        headers: Dict[str, str],
    ) -> Dict[str, Any]:
        """Execute sync request with retry logic."""
        tracer = self._tracer
        attempt = 0
        while True:
            attempt += 1
            tracer.emit(
                "request.start",
                {
                    "endpoint": endpoint_name,
                    "attempt": attempt,
                },
            )

            # Send request
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
                if attempt > endpoint.retries:
                    return self._build_error_response(False, None, None, "timeout")
                continue
            except Exception:  # noqa: BLE001
                tracer.emit(
                    "request.error",
                    {
                        "endpoint": endpoint_name,
                        "attempt": attempt,
                        "category": "network_error",
                        "message": "",
                    },
                )
                return self._build_error_response(False, None, None, "network_error")

            # Handle response
            result = self._handle_response(endpoint_name, endpoint, response, attempt)
            if result is not None:
                return result
            # result is None means retry; continue loop

    def _handle_response(
        self,
        endpoint_name: str,
        endpoint: EndpointConfig,
        response: Dict[str, Any],
        attempt: int,
    ) -> Optional[Dict[str, Any]]:
        """Process response, check retry conditions, and normalize data.
        
        Returns None if retry should occur, otherwise returns the final response.
        """
        tracer = self._tracer
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

        # Check if we should retry
        if status in endpoint.retry_statuses and attempt <= endpoint.retries:
            tracer.emit(
                "request.retry",
                {
                    "endpoint": endpoint_name,
                    "attempt": attempt,
                    "status": status,
                },
            )
            return None  # Signal to retry

        # Normalize response body
        data, body_error = self._normalize_response_body(body)
        if body_error:
            tracer.emit(
                "response.error",
                {
                    "endpoint": endpoint_name,
                    "status": status,
                    "category": body_error,
                    "message": "",
                },
            )
            return self._build_error_response(False, status, None, body_error)

        # Check status code
        status_category = self._map_http_status(status)
        if status_category == "ok":
            tracer.emit(
                "response.ok",
                {
                    "endpoint": endpoint_name,
                    "status": status,
                },
            )
            return self._build_error_response(True, status, data, "ok")

        # Error response
        tracer.emit(
            "response.error",
            {
                "endpoint": endpoint_name,
                "status": status,
                "category": "upstream_error",
            },
        )
        return self._build_error_response(False, status, data, status_category)

    async def call_async(self, ctx: Dict[str, Any], endpoint_name: str, raw_payload: str) -> Dict[str, Any]:
        """Asynchronous API call with retry and error mapping."""
        tracer = self._tracer
        tracer.emit("resolve.start", {"endpoint": endpoint_name})

        endpoint = self._resolve_endpoint(endpoint_name)
        if endpoint is None:
            tracer.emit("resolve.error", {"endpoint": endpoint_name, "error": "unknown_endpoint"})
            return self._build_error_response(False, None, None, "unknown_endpoint")

        tracer.emit("resolve.done", {"endpoint": endpoint_name})

        # Parse payload
        parsed, payload_error = self._parse_payload(raw_payload)
        if payload_error:
            tracer.emit("payload.error", {"endpoint": endpoint_name, "error": payload_error, "message": ""})
            return self._build_error_response(False, None, None, payload_error)

        # Build headers
        headers = self._build_headers(ctx, endpoint)
        tracer.emit(
            "request.build",
            {
                "endpoint": endpoint_name,
                "method": endpoint.method,
                "url": endpoint.url,
                "headers": headers,
            },
        )

        # Retry loop using async transport
        return await self._dispatch_async(endpoint_name, endpoint, parsed, headers)

    async def _dispatch_async(
        self,
        endpoint_name: str,
        endpoint: EndpointConfig,
        parsed: Any,
        headers: Dict[str, str],
    ) -> Dict[str, Any]:
        """Execute async request with retry logic."""
        tracer = self._tracer
        attempt = 0
        while True:
            attempt += 1
            tracer.emit(
                "request.start",
                {
                    "endpoint": endpoint_name,
                    "attempt": attempt,
                },
            )

            # Send request
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
                if attempt > endpoint.retries:
                    return self._build_error_response(False, None, None, "timeout")
                continue
            except Exception:  # noqa: BLE001
                tracer.emit(
                    "request.error",
                    {
                        "endpoint": endpoint_name,
                        "attempt": attempt,
                        "category": "network_error",
                        "message": "",
                    },
                )
                return self._build_error_response(False, None, None, "network_error")

            # Handle response
            result = self._handle_response(endpoint_name, endpoint, response, attempt)
            if result is not None:
                return result
            # result is None means retry; continue loop