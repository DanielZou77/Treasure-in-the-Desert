from __future__ import annotations

import json
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from Data_Center.models import ApiCheckResult, ProviderError


def http_get_json(
    base_url: str,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 25,
) -> Any:
    query = urlencode(params or {}, doseq=True)
    url = f"{base_url}?{query}" if query else base_url
    request = Request(
        url,
        headers={
            "User-Agent": "QuantTradingDataCenter/0.1",
            **(headers or {}),
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        raise ProviderError(f"HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise ProviderError(f"Network error: {exc.reason}") from exc

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise ProviderError(f"Invalid JSON response: {body[:300]}") from exc


def provider_check(callable_fetch: Callable[[], Any], validator: Callable[[Any], None]) -> ApiCheckResult:
    try:
        data = callable_fetch()
        validator(data)
        return ApiCheckResult(True, "API key looks usable.")
    except Exception as exc:
        return ApiCheckResult(False, str(exc))
