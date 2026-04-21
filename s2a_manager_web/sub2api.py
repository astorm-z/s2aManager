from __future__ import annotations

import json
import ssl
import time
from json import JSONDecodeError
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

from .errors import APIError, AppError
from .settings import Sub2ApiSettings


REQUEST_RETRY_LIMIT = 3
ADMIN_LIST_PAGE_SIZE_CAP = 100


def normalize_api_base(url: str) -> str:
    value = str(url or "").strip().rstrip("/")
    if not value:
        raise AppError("sub2api base_url 不能为空")
    if value.endswith("/api/v1"):
        return value
    if "/api/v1/" in value:
        return value.split("/api/v1/", 1)[0] + "/api/v1"
    return value + "/api/v1"


def extract_error(status: int, raw_body: str) -> APIError:
    try:
        payload = json.loads(raw_body) if raw_body else {}
    except JSONDecodeError:
        payload = None

    if isinstance(payload, dict):
        if "code" in payload:
            return APIError(
                status=status,
                code=payload.get("code"),
                message=str(payload.get("message") or "Unknown error"),
                details=payload,
            )
        return APIError(status=status, code=status, message=raw_body or "Request failed", details=payload)
    return APIError(status=status, code=status, message=raw_body or "Request failed")


def is_transient_ssl_eof_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "unexpected_eof_while_reading" in message or "eof occurred in violation of protocol" in message


def parse_int_field(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def clamp_admin_list_page_size(page_size: Any) -> int:
    parsed = parse_int_field(page_size, 0)
    if parsed <= 0:
        return ADMIN_LIST_PAGE_SIZE_CAP
    return min(parsed, ADMIN_LIST_PAGE_SIZE_CAP)


class AdminAPIClient:
    def __init__(self, settings: Sub2ApiSettings) -> None:
        self.api_base = normalize_api_base(settings.base_url)
        self.admin_api_key = settings.admin_api_key.strip()
        self.timeout = float(settings.timeout or 30.0)
        self.insecure = bool(settings.insecure)
        self.user_agent = "S2A-Manager-Web/0.1.0"

    def request(self, method: str, path: str, payload: Any = None) -> Any:
        request_attempt = 0
        while True:
            request_attempt += 1
            headers = {
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            }
            if self.admin_api_key:
                headers["x-api-key"] = self.admin_api_key

            data: bytes | None = None
            if payload is not None:
                headers["Content-Type"] = "application/json"
                data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

            request = Request(
                url=urljoin(self.api_base + "/", path.lstrip("/")),
                data=data,
                headers=headers,
                method=method.upper(),
            )
            context = None
            if self.insecure and request.full_url.startswith("https://"):
                context = ssl._create_unverified_context()

            try:
                with urlopen(request, timeout=self.timeout, context=context) as response:
                    raw_body = response.read().decode("utf-8", errors="replace")
                    return self._unwrap_success(raw_body, response.status)
            except HTTPError as exc:
                raw_body = exc.read().decode("utf-8", errors="replace")
                raise extract_error(exc.code, raw_body) from exc
            except URLError as exc:
                if request_attempt < REQUEST_RETRY_LIMIT and is_transient_ssl_eof_error(exc):
                    time.sleep(0.35 * request_attempt)
                    continue
                raise AppError(f"请求 sub2api 失败: {exc}") from exc

    @staticmethod
    def _unwrap_success(raw_body: str, status: int) -> Any:
        if not raw_body.strip():
            return {"status": status}

        try:
            payload = json.loads(raw_body)
        except JSONDecodeError as exc:
            raise AppError(f"sub2api 返回了非 JSON 响应: {raw_body[:200]}") from exc

        if isinstance(payload, dict) and "code" in payload:
            if payload.get("code") != 0:
                raise APIError(
                    status=status,
                    code=payload.get("code"),
                    message=str(payload.get("message") or "Unknown error"),
                    details=payload,
                )
            return payload.get("data")
        return payload

    def list_accounts_page(
        self,
        *,
        page: int,
        page_size: int,
        platform: str | None,
        account_type: str | None,
        status: str | None,
        search: str | None,
        group_id: int | str | None = None,
    ) -> dict[str, Any]:
        effective_page_size = clamp_admin_list_page_size(page_size)
        query: dict[str, Any] = {"page": page, "page_size": effective_page_size}
        if platform:
            query["platform"] = platform
        if account_type:
            query["type"] = account_type
        if status:
            query["status"] = status
        if search:
            query["search"] = search
        if isinstance(group_id, int) and group_id > 0:
            query["group"] = group_id
        elif isinstance(group_id, str) and group_id.strip().lower() == "ungrouped":
            query["group"] = "ungrouped"

        payload = self.request("GET", f"/admin/accounts?{urlencode(query)}")
        if not isinstance(payload, dict):
            raise AppError("`/admin/accounts` 返回格式异常：期望对象")
        if not isinstance(payload.get("items"), list):
            raise AppError("`/admin/accounts` 返回格式异常：缺少数组字段 `items`")
        return payload

    def list_groups(self) -> list[dict[str, Any]]:
        payload = self.request("GET", "/admin/groups/all")
        if not isinstance(payload, list):
            raise AppError("`/admin/groups/all` 返回格式异常：期望数组")
        parsed: list[dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            group_id = item.get("id")
            if not isinstance(group_id, int) or group_id <= 0:
                continue
            parsed.append(
                {
                    "id": group_id,
                    "name": str(item.get("name") or f"group-{group_id}"),
                    "platform": str(item.get("platform") or "unknown"),
                    "status": str(item.get("status") or "unknown"),
                    "is_exclusive": bool(item.get("is_exclusive")),
                }
            )
        parsed.sort(key=lambda x: (x["platform"], x["name"], x["id"]))
        return parsed

    def list_proxies(self) -> list[dict[str, Any]]:
        payload = self.request("GET", "/admin/proxies/all?with_count=true")
        if not isinstance(payload, list):
            raise AppError("`/admin/proxies/all?with_count=true` 返回格式异常：期望数组")
        parsed = [item for item in payload if isinstance(item, dict)]
        parsed.sort(key=lambda item: (str(item.get("host") or ""), parse_int_field(item.get("port"), 0), int(item.get("id") or 0)))
        return parsed

    def bulk_update_accounts(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self.request("POST", "/admin/accounts/bulk-update", payload)
        if not isinstance(result, dict):
            raise AppError("`/admin/accounts/bulk-update` 返回格式异常")
        return result

    def update_account(self, account_id: int, payload: dict[str, Any]) -> Any:
        return self.request("PUT", f"/admin/accounts/{account_id}", payload)

    def delete_account(self, account_id: int) -> Any:
        return self.request("DELETE", f"/admin/accounts/{account_id}")

    def import_accounts_data(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self.request("POST", "/admin/accounts/data", payload)
        if not isinstance(result, dict):
            raise AppError("`/admin/accounts/data` 返回格式异常")
        return result
