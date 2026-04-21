from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from json import JSONDecodeError
from pathlib import Path
from typing import Any

import yaml

from .errors import AppError
from .sub2api import ADMIN_LIST_PAGE_SIZE_CAP, AdminAPIClient, clamp_admin_list_page_size, parse_int_field

DATA_TYPE = "sub2api-data"
LEGACY_DATA_TYPE = "sub2api-bundle"
DATA_VERSION = 1
VALID_PROXY_PROTOCOLS = {"http", "https", "socks5", "socks5h"}
VALID_PROXY_STATUSES = {"active", "inactive"}
VALID_ACCOUNT_IMPORT_TYPES = {"oauth", "setup-token", "apikey", "upstream"}
KNOWN_ACCOUNT_PLATFORMS = {"anthropic", "openai", "gemini", "antigravity", "sora"}
ACCOUNT_LIST_GROUP_UNGROUPED = "ungrouped"
CREDENTIAL_FALLBACK_KEYS = (
    "token",
    "access_token",
    "refresh_token",
    "session_token",
    "id_token",
    "api_key",
    "apikey",
    "setup_token",
    "cookie",
    "cookies",
    "email",
    "password",
    "chatgpt_account_id",
    "chatgpt_user_id",
    "organization_id",
    "project_id",
    "headers",
    "endpoint",
    "base_url",
    "region",
)
BULK_SUPPORTED_KEYS = {
    "name",
    "notes",
    "type",
    "proxy_id",
    "concurrency",
    "priority",
    "rate_multiplier",
    "load_factor",
    "status",
    "schedulable",
    "group_ids",
    "credentials",
    "extra",
    "expires_at",
    "auto_pause_on_expired",
    "confirm_mixed_channel_risk",
}


def non_empty(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def unique_ids(values: list[int]) -> list[int]:
    seen: set[int] = set()
    result: list[int] = []
    for value in values:
        if value <= 0:
            raise AppError(f"ID 必须为正整数: {value}")
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def ensure_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise AppError(f"{label} 必须是 JSON 对象")
    return dict(value)


def json_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def json_number(value: Any) -> float | int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value


def utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_proxy_key(protocol: str, host: str, port: int, username: str = "", password: str = "") -> str:
    return f"{protocol.strip()}|{host.strip()}|{port}|{username.strip()}|{password.strip()}"


def normalize_proxy_status(status: str) -> str:
    normalized = status.strip().lower()
    if normalized == "disabled":
        return "inactive"
    return normalized


def format_validation_message(result: dict[str, Any], *, source_label: str | None = None) -> str:
    errors = [str(item) for item in result.get("errors") or []]
    if not errors:
        warnings = result.get("warnings") or []
        if warnings:
            prefix = f"{source_label}：" if source_label else ""
            return prefix + f"发现 {len(warnings)} 条提醒，请先查看检查结果。"
        return f"{source_label or '当前文件'} 格式检查通过"

    preview = "；".join(errors[:3])
    if len(errors) > 3:
        preview += f"；其余 {len(errors) - 3} 条请先展开检查结果查看"
    prefix = f"{source_label}：" if source_label else ""
    return prefix + preview


def build_account_proxy_display(account: dict[str, Any]) -> str | None:
    proxy = account.get("proxy")
    if isinstance(proxy, dict):
        proxy_id = parse_int_field(proxy.get("id"), 0)
        name = non_empty(str(proxy.get("name") or ""))
        protocol = non_empty(str(proxy.get("protocol") or ""))
        host = non_empty(str(proxy.get("host") or ""))
        port = parse_int_field(proxy.get("port"), 0)
        prefix = f"[{proxy_id}] " if proxy_id > 0 else ""
        if name and protocol and host and port > 0:
            return f"{prefix}{name} ({protocol}://{host}:{port})"
        if protocol and host and port > 0:
            return f"{prefix}{protocol}://{host}:{port}"
        if host and port > 0:
            return f"{prefix}{host}:{port}"
        if name:
            return f"{prefix}{name}"

    proxy_key = non_empty(str(account.get("proxy_key") or ""))
    if proxy_key:
        return proxy_key

    proxy_id = parse_int_field(account.get("proxy_id"), 0)
    if proxy_id > 0:
        return f"[{proxy_id}]"
    return None


def build_account_group_display_lines(account: dict[str, Any]) -> list[str]:
    lines: list[str] = []

    groups = account.get("groups")
    if isinstance(groups, list):
        for group in groups:
            if not isinstance(group, dict):
                continue
            group_id = parse_int_field(group.get("id"), 0)
            group_name = non_empty(str(group.get("name") or ""))
            if group_id > 0 and group_name:
                lines.append(f"[{group_id}]{group_name}")
            elif group_id > 0:
                lines.append(f"[{group_id}]")
            elif group_name:
                lines.append(group_name)
        if lines:
            return lines

    group_ids = account.get("group_ids")
    if isinstance(group_ids, list):
        for group_id in group_ids:
            if isinstance(group_id, int) and group_id > 0:
                lines.append(f"[{group_id}]")
    return lines


def normalize_account_for_view(account: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(account)
    group_display_lines = build_account_group_display_lines(account)
    normalized["group_display_lines"] = group_display_lines
    normalized["group_display_title"] = "\n".join(group_display_lines)
    normalized["proxy_display"] = build_account_proxy_display(account)
    return normalized


def normalize_optional_string_field(
    value: Any,
    *,
    location: str,
    field: str,
    errors: list[str],
    allow_none: bool = True,
) -> str | None:
    if value is None:
        if allow_none:
            return None
        errors.append(f"{location}.{field} 必须是字符串")
        return None
    if not isinstance(value, str):
        errors.append(f"{location}.{field} 必须是字符串")
        return None
    return value.strip()


def derive_credentials_from_raw_account(item: dict[str, Any], location: str, warnings: list[str]) -> dict[str, Any] | None:
    if "credentials" in item:
        raw_credentials = item.get("credentials")
        if not isinstance(raw_credentials, dict) or not raw_credentials:
            return None
        return dict(raw_credentials)

    if "credential" in item:
        raw_credentials = item.get("credential")
        if isinstance(raw_credentials, dict) and raw_credentials:
            warnings.append(f"{location}: 已自动把 `credential` 改写成 `credentials`")
            return dict(raw_credentials)
        return None

    credentials: dict[str, Any] = {}
    for key in CREDENTIAL_FALLBACK_KEYS:
        if key not in item:
            continue
        value = item.get(key)
        if value is None:
            continue
        target_key = "api_key" if key == "apikey" else key
        credentials[target_key] = value

    if credentials:
        warnings.append(f"{location}: 未找到 `credentials`，已自动从顶层常见字段生成")
        return credentials
    return None


def validate_data_proxy_item(item: Any, index: int) -> dict[str, Any]:
    location = f"proxies[{index}]"
    errors: list[str] = []
    warnings: list[str] = []
    normalized: dict[str, Any] = {}

    if not isinstance(item, dict):
        return {"errors": [f"{location} 必须是 JSON 对象"], "warnings": [], "normalized": None, "effective_proxy_key": None}

    protocol = normalize_optional_string_field(item.get("protocol"), location=location, field="protocol", errors=errors, allow_none=False)
    host = normalize_optional_string_field(item.get("host"), location=location, field="host", errors=errors, allow_none=False)
    port = json_int(item.get("port"))
    if port is None:
        errors.append(f"{location}.port 必须是整数")
    elif port <= 0 or port > 65535:
        errors.append(f"{location}.port 必须在 1 到 65535 之间")

    normalized_protocol = ""
    if protocol is not None:
        normalized_protocol = protocol.lower()
        if normalized_protocol not in VALID_PROXY_PROTOCOLS:
            errors.append(f"{location}.protocol 只支持 {', '.join(sorted(VALID_PROXY_PROTOCOLS))}")
        normalized["protocol"] = normalized_protocol

    if host is not None:
        if not host:
            errors.append(f"{location}.host 不能为空")
        normalized["host"] = host

    if port is not None:
        normalized["port"] = port

    for field in ("name", "username", "password"):
        if field not in item:
            continue
        text = normalize_optional_string_field(item.get(field), location=location, field=field, errors=errors, allow_none=True)
        if text is not None:
            normalized[field] = text

    effective_proxy_key: str | None = None
    raw_proxy_key = item.get("proxy_key")
    if raw_proxy_key is not None:
        proxy_key = normalize_optional_string_field(raw_proxy_key, location=location, field="proxy_key", errors=errors, allow_none=True)
        if proxy_key:
            effective_proxy_key = proxy_key
            normalized["proxy_key"] = proxy_key

    raw_status = item.get("status")
    if raw_status is not None:
        status = normalize_optional_string_field(raw_status, location=location, field="status", errors=errors, allow_none=True)
        if status:
            normalized_status = normalize_proxy_status(status)
            if normalized_status not in VALID_PROXY_STATUSES:
                errors.append(f"{location}.status 只支持 active / inactive")
            else:
                normalized["status"] = normalized_status

    if effective_proxy_key is None and protocol and host and port:
        effective_proxy_key = build_proxy_key(
            normalized_protocol or protocol.strip().lower(),
            host,
            port,
            str(normalized.get("username") or ""),
            str(normalized.get("password") or ""),
        )
        normalized["proxy_key"] = effective_proxy_key
        warnings.append(f"{location}: 未提供 `proxy_key`，已按协议/地址/端口自动生成")

    return {
        "errors": errors,
        "warnings": warnings,
        "normalized": normalized if not errors else None,
        "effective_proxy_key": effective_proxy_key,
    }


def _validate_group_ids(value: Any, *, location: str, errors: list[str]) -> list[int] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        errors.append(f"{location}.group_ids 必须是整数数组")
        return None
    parsed: list[int] = []
    for index, item in enumerate(value):
        if not isinstance(item, int) or item <= 0:
            errors.append(f"{location}.group_ids[{index}] 必须是正整数")
            continue
        parsed.append(item)
    return unique_ids(parsed) if parsed else []


def validate_data_account_item(item: Any, index: int, file_proxy_keys: set[str]) -> dict[str, Any]:
    location = f"accounts[{index}]"
    errors: list[str] = []
    warnings: list[str] = []
    normalized: dict[str, Any] = {}

    if not isinstance(item, dict):
        return {"errors": [f"{location} 必须是 JSON 对象"], "warnings": [], "normalized": None}

    name = normalize_optional_string_field(item.get("name"), location=location, field="name", errors=errors, allow_none=False)
    if name is not None:
        if not name:
            errors.append(f"{location}.name 不能为空")
        normalized["name"] = name

    platform = normalize_optional_string_field(item.get("platform"), location=location, field="platform", errors=errors, allow_none=False)
    if platform is not None:
        normalized_platform = platform.lower()
        if not normalized_platform:
            errors.append(f"{location}.platform 不能为空")
        else:
            if normalized_platform not in KNOWN_ACCOUNT_PLATFORMS:
                warnings.append(f"{location}.platform=`{normalized_platform}` 不是当前程序已知平台，导入时将交由服务端最终判断")
            normalized["platform"] = normalized_platform

    account_type = normalize_optional_string_field(item.get("type"), location=location, field="type", errors=errors, allow_none=False)
    if account_type is not None:
        normalized_type = account_type.lower()
        if not normalized_type:
            errors.append(f"{location}.type 不能为空")
        elif normalized_type not in VALID_ACCOUNT_IMPORT_TYPES:
            allowed = ", ".join(sorted(VALID_ACCOUNT_IMPORT_TYPES))
            errors.append(f"{location}.type 只支持 {allowed}；当前导入接口不接受 bedrock")
        else:
            normalized["type"] = normalized_type

    credentials = derive_credentials_from_raw_account(item, location, warnings)
    if not isinstance(credentials, dict) or not credentials:
        errors.append(f"{location}.credentials 必须是非空 JSON 对象")
    else:
        normalized["credentials"] = credentials

    if "notes" in item:
        notes = normalize_optional_string_field(item.get("notes"), location=location, field="notes", errors=errors, allow_none=True)
        normalized["notes"] = notes

    if "extra" in item:
        extra = item.get("extra")
        if extra is None:
            normalized["extra"] = None
        elif not isinstance(extra, dict):
            errors.append(f"{location}.extra 必须是 JSON 对象")
        else:
            normalized["extra"] = dict(extra)

    raw_proxy_key = item.get("proxy_key")
    if raw_proxy_key is not None:
        proxy_key = normalize_optional_string_field(raw_proxy_key, location=location, field="proxy_key", errors=errors, allow_none=True)
        if proxy_key:
            normalized["proxy_key"] = proxy_key
            if proxy_key not in file_proxy_keys:
                warnings.append(f"{location}.proxy_key 在当前文件代理列表里找不到；只有站点中已存在同 key 代理时导入才会成功")

    concurrency_value = json_int(item.get("concurrency", 0))
    if concurrency_value is None:
        errors.append(f"{location}.concurrency 必须是整数")
    elif concurrency_value < 0:
        errors.append(f"{location}.concurrency 不能为负数")
    else:
        normalized["concurrency"] = concurrency_value

    priority_value = json_int(item.get("priority", 0))
    if priority_value is None:
        errors.append(f"{location}.priority 必须是整数")
    elif priority_value < 0:
        errors.append(f"{location}.priority 不能为负数")
    else:
        normalized["priority"] = priority_value

    if "rate_multiplier" in item:
        rate_multiplier = item.get("rate_multiplier")
        if rate_multiplier is None:
            normalized["rate_multiplier"] = None
        else:
            rate_value = json_number(rate_multiplier)
            if rate_value is None:
                errors.append(f"{location}.rate_multiplier 必须是数字")
            elif rate_value < 0:
                errors.append(f"{location}.rate_multiplier 不能为负数")
            else:
                normalized["rate_multiplier"] = rate_value

    if "expires_at" in item:
        expires_at = item.get("expires_at")
        if expires_at is None:
            normalized["expires_at"] = None
        else:
            expires_value = json_int(expires_at)
            if expires_value is None:
                errors.append(f"{location}.expires_at 必须是整数时间戳")
            else:
                normalized["expires_at"] = expires_value

    if "auto_pause_on_expired" in item:
        auto_pause = item.get("auto_pause_on_expired")
        if auto_pause is not None and not isinstance(auto_pause, bool):
            errors.append(f"{location}.auto_pause_on_expired 必须是布尔值")
        else:
            normalized["auto_pause_on_expired"] = auto_pause

    if "status" in item:
        status = normalize_optional_string_field(item.get("status"), location=location, field="status", errors=errors, allow_none=True)
        if status:
            if status.lower() not in {"active", "inactive", "error"}:
                errors.append(f"{location}.status 只支持 active / inactive / error")
            else:
                normalized["status"] = status.lower()

    if "schedulable" in item:
        schedulable = item.get("schedulable")
        if not isinstance(schedulable, bool):
            errors.append(f"{location}.schedulable 必须是布尔值")
        else:
            normalized["schedulable"] = schedulable

    if "group_ids" in item:
        group_ids = _validate_group_ids(item.get("group_ids"), location=location, errors=errors)
        if group_ids is not None:
            normalized["group_ids"] = group_ids

    if "load_factor" in item:
        load_factor = item.get("load_factor")
        if load_factor is None:
            normalized["load_factor"] = None
        elif not isinstance(load_factor, int):
            errors.append(f"{location}.load_factor 必须是整数")
        elif load_factor < 0:
            errors.append(f"{location}.load_factor 不能为负数")
        else:
            normalized["load_factor"] = load_factor

    return {"errors": errors, "warnings": warnings, "normalized": normalized if not errors else None}


def validate_accounts_data_payload(raw_payload: Any) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    proxy_keys_in_file: set[str] = set()

    if not isinstance(raw_payload, dict):
        return {
            "ok": False,
            "errors": ["`data` 必须是 JSON 对象"],
            "warnings": [],
            "normalized": None,
            "proxy_count": 0,
            "account_count": 0,
            "proxy_keys_in_file": set(),
        }

    payload = dict(raw_payload)
    normalized: dict[str, Any] = dict(payload)

    if "type" in payload:
        payload_type = payload.get("type")
        if not isinstance(payload_type, str):
            errors.append("`data.type` 必须是字符串")
        else:
            normalized_type = payload_type.strip()
            if normalized_type and normalized_type not in {DATA_TYPE, LEGACY_DATA_TYPE}:
                errors.append(f"`data.type` 只支持 `{DATA_TYPE}` 或 `{LEGACY_DATA_TYPE}`")
            else:
                normalized["type"] = normalized_type
    else:
        warnings.append("未提供 `data.type`，后端仍可导入，但这不属于标准导出文件")

    if "version" in payload:
        version = json_int(payload.get("version"))
        if version is None:
            errors.append("`data.version` 必须是整数")
        elif version != DATA_VERSION:
            errors.append(f"`data.version` 目前只支持 {DATA_VERSION}")
        else:
            normalized["version"] = version
    else:
        warnings.append("未提供 `data.version`，后端仍可导入，但这不属于标准导出文件")

    if "exported_at" in payload:
        exported_at = payload.get("exported_at")
        if not isinstance(exported_at, str):
            errors.append("`data.exported_at` 必须是字符串")
        else:
            normalized["exported_at"] = exported_at.strip()
    else:
        warnings.append("未提供 `data.exported_at`，建议先转成标准导入格式")

    proxies = payload.get("proxies")
    if proxies is None:
        errors.append("缺少 `data.proxies` 数组")
        proxies = []
    elif not isinstance(proxies, list):
        errors.append("`data.proxies` 必须是数组")
        proxies = []

    accounts = payload.get("accounts")
    if accounts is None:
        errors.append("缺少 `data.accounts` 数组")
        accounts = []
    elif not isinstance(accounts, list):
        errors.append("`data.accounts` 必须是数组")
        accounts = []

    normalized_proxies: list[dict[str, Any]] = []
    for index, proxy_item in enumerate(proxies):
        proxy_result = validate_data_proxy_item(proxy_item, index)
        errors.extend(proxy_result["errors"])
        warnings.extend(proxy_result["warnings"])
        normalized_proxy = proxy_result.get("normalized")
        effective_proxy_key = proxy_result.get("effective_proxy_key")
        if normalized_proxy is not None:
            normalized_proxies.append(normalized_proxy)
        if isinstance(effective_proxy_key, str) and effective_proxy_key:
            proxy_keys_in_file.add(effective_proxy_key)

    normalized_accounts: list[dict[str, Any]] = []
    for index, account_item in enumerate(accounts):
        account_result = validate_data_account_item(account_item, index, proxy_keys_in_file)
        errors.extend(account_result["errors"])
        warnings.extend(account_result["warnings"])
        normalized_account = account_result.get("normalized")
        if normalized_account is not None:
            normalized_accounts.append(normalized_account)

    normalized["proxies"] = normalized_proxies
    normalized["accounts"] = normalized_accounts

    return {
        "ok": not errors,
        "errors": errors,
        "warnings": warnings,
        "normalized": normalized if not errors else None,
        "proxy_count": len(normalized_proxies),
        "account_count": len(normalized_accounts),
        "proxy_keys_in_file": proxy_keys_in_file,
    }


def validate_accounts_import_payload(raw: Any, *, skip_default_group_bind: bool) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    request_payload: dict[str, Any] | None = None
    raw_data: Any = None
    effective_skip = skip_default_group_bind

    if not isinstance(raw, dict):
        errors.append("账号导入文件必须是 JSON/YAML/TOML 对象；支持直接放 DataPayload，或放到 `data` 字段里")
    else:
        if "data" in raw:
            raw_data = raw.get("data")
            skip_value = raw.get("skip_default_group_bind")
            if "skip_default_group_bind" in raw:
                if skip_value is None:
                    pass
                elif not isinstance(skip_value, bool):
                    errors.append("`skip_default_group_bind` 必须是布尔值")
                else:
                    effective_skip = skip_value
        else:
            raw_data = raw

    data_result = validate_accounts_data_payload(raw_data)
    errors.extend(data_result["errors"])
    warnings.extend(data_result["warnings"])

    normalized_data = data_result.get("normalized")
    if normalized_data is not None:
        request_payload = {"data": normalized_data, "skip_default_group_bind": effective_skip}

    return {
        "ok": not errors,
        "errors": errors,
        "warnings": warnings,
        "request_payload": request_payload,
        "data_payload": normalized_data,
        "account_count": data_result["account_count"],
        "proxy_count": data_result["proxy_count"],
        "skip_default_group_bind": effective_skip,
    }


def build_standard_accounts_data_payload(data_payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(data_payload)
    payload["type"] = DATA_TYPE
    payload["version"] = DATA_VERSION
    if not isinstance(payload.get("exported_at"), str) or not payload.get("exported_at", "").strip():
        payload["exported_at"] = utc_now_rfc3339()
    payload["proxies"] = [dict(item) for item in payload.get("proxies") or [] if isinstance(item, dict)]
    payload["accounts"] = [dict(item) for item in payload.get("accounts") or [] if isinstance(item, dict)]

    for proxy in payload["proxies"]:
        if "name" not in proxy or not str(proxy.get("name") or "").strip():
            proxy["name"] = "imported-proxy"
        if "status" not in proxy or not str(proxy.get("status") or "").strip():
            proxy["status"] = "active"

    for account in payload["accounts"]:
        if "concurrency" not in account or json_int(account.get("concurrency")) is None:
            account["concurrency"] = 10
        if "priority" not in account or json_int(account.get("priority")) is None:
            account["priority"] = 1

    return payload


def try_convert_auth_snapshot_account(raw: Any, *, source_name: str) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    access_token = non_empty(str(raw.get("access_token") or ""))
    refresh_token = non_empty(str(raw.get("refresh_token") or ""))
    email = non_empty(str(raw.get("email") or ""))
    source_type = non_empty(str(raw.get("type") or ""))
    account_id = non_empty(str(raw.get("account_id") or ""))
    id_token = non_empty(str(raw.get("id_token") or ""))
    if not access_token or not email:
        return None

    credentials: dict[str, Any] = {
        "access_token": access_token,
        "email": email,
    }
    if refresh_token:
        credentials["refresh_token"] = refresh_token
    if id_token:
        credentials["id_token"] = id_token
    if account_id:
        credentials["chatgpt_account_id"] = account_id
    expired = non_empty(str(raw.get("expired") or ""))
    if expired:
        credentials["expires_at"] = expired

    extra: dict[str, Any] = {
        "source_format": "auth_snapshot",
        "source_name": source_name,
    }
    if source_type:
        extra["source_type"] = source_type
    if "disabled" in raw:
        extra["disabled"] = bool(raw.get("disabled"))
    last_refresh = non_empty(str(raw.get("last_refresh") or ""))
    if last_refresh:
        extra["last_refresh"] = last_refresh

    normalized_account = {
        "name": email,
        "platform": "openai",
        "type": "oauth",
        "credentials": credentials,
        "extra": extra,
        "concurrency": 10,
        "priority": 1,
    }
    standardized = build_standard_accounts_data_payload(
        {
            "exported_at": utc_now_rfc3339(),
            "proxies": [],
            "accounts": [normalized_account],
        }
    )
    warnings = [f"{source_name}: 已识别为 auths 单账号快照，自动转换为 openai/oauth 账号"]
    if not refresh_token:
        warnings.append(f"{source_name}: refresh_token 为空，已按 access_token 模式继续转换")
    return {
        "ok": True,
        "errors": [],
        "warnings": warnings,
        "data_payload": standardized,
        "account_count": 1,
        "proxy_count": 0,
        "mode": "auth-snapshot",
    }


def convert_simple_accounts_json(raw: Any, *, source_name: str = "当前文件") -> dict[str, Any]:
    standard_validation = validate_accounts_import_payload(raw, skip_default_group_bind=True)
    if standard_validation["ok"] and isinstance(standard_validation.get("data_payload"), dict):
        standardized = build_standard_accounts_data_payload(standard_validation["data_payload"])
        return {
            "ok": True,
            "errors": [],
            "warnings": list(standard_validation["warnings"]),
            "data_payload": standardized,
            "account_count": len(standardized.get("accounts") or []),
            "proxy_count": len(standardized.get("proxies") or []),
            "mode": "standardized-existing",
        }

    auth_snapshot_conversion = try_convert_auth_snapshot_account(raw, source_name=source_name)
    if auth_snapshot_conversion is not None:
        return auth_snapshot_conversion

    warnings: list[str] = []
    errors: list[str] = []
    raw_accounts: Any = None
    raw_proxies: Any = []
    source_candidate = raw.get("data") if isinstance(raw, dict) and isinstance(raw.get("data"), dict) else raw

    if isinstance(source_candidate, list):
        raw_accounts = source_candidate
    elif isinstance(source_candidate, dict):
        if "accounts" in source_candidate:
            raw_accounts = source_candidate.get("accounts")
            raw_proxies = source_candidate.get("proxies", [])
        elif {"name", "platform", "type"} & set(source_candidate.keys()):
            raw_accounts = [source_candidate]
            raw_proxies = []
        else:
            errors.append(f"{source_name}: 无法识别账号列表。请提供数组、`accounts` 数组，或单个账号对象")
    else:
        errors.append(f"{source_name}: 只能转换 JSON/YAML/TOML 对象或数组")

    if errors:
        return {"ok": False, "errors": errors, "warnings": warnings, "data_payload": None, "account_count": 0, "proxy_count": 0, "mode": "simple"}
    if not isinstance(raw_accounts, list):
        return {"ok": False, "errors": [f"{source_name}: `accounts` 必须是数组"], "warnings": warnings, "data_payload": None, "account_count": 0, "proxy_count": 0, "mode": "simple"}
    if raw_proxies is None:
        raw_proxies = []
    if not isinstance(raw_proxies, list):
        return {"ok": False, "errors": [f"{source_name}: `proxies` 必须是数组"], "warnings": warnings, "data_payload": None, "account_count": 0, "proxy_count": 0, "mode": "simple"}

    proxy_keys_in_file: set[str] = set()
    normalized_proxies: list[dict[str, Any]] = []
    for index, proxy_item in enumerate(raw_proxies):
        proxy_result = validate_data_proxy_item(proxy_item, index)
        errors.extend(proxy_result["errors"])
        warnings.extend(proxy_result["warnings"])
        normalized_proxy = proxy_result.get("normalized")
        effective_proxy_key = proxy_result.get("effective_proxy_key")
        if normalized_proxy is not None:
            normalized_proxies.append(normalized_proxy)
        if isinstance(effective_proxy_key, str) and effective_proxy_key:
            proxy_keys_in_file.add(effective_proxy_key)

    normalized_accounts: list[dict[str, Any]] = []
    for index, account_item in enumerate(raw_accounts):
        account_result = validate_data_account_item(account_item, index, proxy_keys_in_file)
        errors.extend(account_result["errors"])
        warnings.extend(account_result["warnings"])
        normalized_account = account_result.get("normalized")
        if normalized_account is not None:
            normalized_accounts.append(normalized_account)

    if errors:
        return {
            "ok": False,
            "errors": errors,
            "warnings": warnings,
            "data_payload": None,
            "account_count": len(normalized_accounts),
            "proxy_count": len(normalized_proxies),
            "mode": "simple",
        }

    standardized = build_standard_accounts_data_payload(
        {
            "exported_at": utc_now_rfc3339(),
            "proxies": normalized_proxies,
            "accounts": normalized_accounts,
        }
    )
    return {
        "ok": True,
        "errors": [],
        "warnings": warnings,
        "data_payload": standardized,
        "account_count": len(normalized_accounts),
        "proxy_count": len(normalized_proxies),
        "mode": "simple",
    }


def parse_uploaded_payload(filename: str, content: bytes) -> Any:
    suffix = Path(filename or "").suffix.lower()
    text = content.decode("utf-8-sig", errors="replace")
    attempts: list[str] = []

    if suffix in {".json", ""}:
        attempts.append("json")
    if suffix in {".yaml", ".yml"}:
        attempts.append("yaml")
    if suffix == ".toml":
        attempts.append("toml")
    attempts.extend([mode for mode in ("json", "yaml", "toml") if mode not in attempts])

    errors: list[str] = []
    for mode in attempts:
        try:
            if mode == "json":
                return json.loads(text)
            if mode == "yaml":
                return yaml.safe_load(text)
            if mode == "toml":
                import tomllib

                return tomllib.loads(text)
        except (JSONDecodeError, yaml.YAMLError, ValueError, TypeError) as exc:
            errors.append(f"{mode}: {exc}")
    raise AppError(f"无法解析上传文件 `{filename}`。已尝试 JSON/YAML/TOML：{' | '.join(errors)}")


def deep_merge_dict(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in updates.items():
        current = result.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            result[key] = deep_merge_dict(current, value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def build_import_overrides(
    *,
    group_id: int | None,
    proxy_key: str | None,
    status: str | None,
    schedulable: bool | None,
    notes: str | None,
    concurrency: int | None,
    priority: int | None,
    load_factor: int | None,
    rate_multiplier: float | None,
    expires_at: int | None,
    auto_pause_on_expired: bool | None,
    credentials: dict[str, Any] | None,
    extra: dict[str, Any] | None,
    manual_updates: dict[str, Any] | None,
) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    if group_id is not None:
        overrides["group_ids"] = [group_id]
    if proxy_key:
        overrides["proxy_key"] = proxy_key
    if status:
        overrides["status"] = status
    if schedulable is not None:
        overrides["schedulable"] = schedulable
    if notes is not None:
        overrides["notes"] = notes
    if concurrency is not None:
        overrides["concurrency"] = concurrency
    if priority is not None:
        overrides["priority"] = priority
    if load_factor is not None:
        overrides["load_factor"] = load_factor
    if rate_multiplier is not None:
        overrides["rate_multiplier"] = rate_multiplier
    if expires_at is not None:
        overrides["expires_at"] = expires_at
    if auto_pause_on_expired is not None:
        overrides["auto_pause_on_expired"] = auto_pause_on_expired
    if credentials:
        overrides["credentials"] = credentials
    if extra:
        overrides["extra"] = extra
    if manual_updates:
        overrides = deep_merge_dict(overrides, manual_updates)
    return overrides


def apply_import_overrides(data_payload: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    payload = build_standard_accounts_data_payload(data_payload)
    if not overrides:
        return payload
    merged_accounts: list[dict[str, Any]] = []
    for account in payload.get("accounts") or []:
        if not isinstance(account, dict):
            continue
        next_account = dict(account)
        for field in ("credentials", "extra"):
            if field in overrides and isinstance(overrides[field], dict):
                next_account[field] = deep_merge_dict(dict(next_account.get(field) or {}), overrides[field])
        for key, value in overrides.items():
            if key in {"credentials", "extra"} and isinstance(value, dict):
                continue
            next_account[key] = copy.deepcopy(value)
        merged_accounts.append(next_account)
    payload["accounts"] = merged_accounts
    return payload


def inspect_import_source(
    raw: Any,
    *,
    source_name: str,
    skip_default_group_bind: bool,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    validation = validate_accounts_import_payload(raw, skip_default_group_bind=skip_default_group_bind)
    warnings = list(validation["warnings"])
    errors = list(validation["errors"])
    mode = "standard"
    data_payload: dict[str, Any] | None = None

    if validation["ok"] and isinstance(validation.get("data_payload"), dict):
        data_payload = build_standard_accounts_data_payload(validation["data_payload"])
        mode = "standard"
    else:
        converted = convert_simple_accounts_json(raw, source_name=source_name)
        if converted["ok"] and isinstance(converted.get("data_payload"), dict):
            data_payload = build_standard_accounts_data_payload(converted["data_payload"])
            mode = str(converted.get("mode") or "simple")
            warnings = converted["warnings"]
            errors = []
        else:
            combined_errors = errors or []
            if converted.get("errors"):
                combined_errors.extend([str(item) for item in converted["errors"]])
            return {
                "ok": False,
                "errors": combined_errors,
                "warnings": warnings + list(converted.get("warnings") or []),
                "mode": "invalid",
                "request_payload": None,
                "data_payload": None,
                "account_count": 0,
                "proxy_count": 0,
            }

    final_payload = apply_import_overrides(data_payload, overrides or {})
    final_validation = validate_accounts_import_payload(
        {"data": final_payload, "skip_default_group_bind": skip_default_group_bind},
        skip_default_group_bind=skip_default_group_bind,
    )
    if not final_validation["ok"] or not isinstance(final_validation.get("request_payload"), dict):
        return {
            "ok": False,
            "errors": final_validation["errors"],
            "warnings": warnings + list(final_validation.get("warnings") or []),
            "mode": mode,
            "request_payload": None,
            "data_payload": None,
            "account_count": 0,
            "proxy_count": 0,
        }

    request_payload = final_validation["request_payload"]
    return {
        "ok": True,
        "errors": [],
        "warnings": warnings + list(final_validation.get("warnings") or []),
        "mode": mode,
        "request_payload": request_payload,
        "data_payload": final_payload,
        "account_count": len(final_payload.get("accounts") or []),
        "proxy_count": len(final_payload.get("proxies") or []),
        "skip_default_group_bind": skip_default_group_bind,
    }


@dataclass
class AccountFilters:
    account_ids: list[int] | None
    platform: str | None
    account_type: str | None
    account_status: str | None
    search: str | None
    name_contains: str | None
    group_ids: list[int] | None
    ungrouped_only: bool
    max_accounts: int | None
    scan_page_size: int
    batch_size: int


def to_positive_int(value: Any, label: str) -> int:
    if not isinstance(value, int):
        raise AppError(f"{label} 必须是整数")
    if value <= 0:
        raise AppError(f"{label} 必须为正整数")
    return value


def to_int_list(values: Any, label: str) -> list[int]:
    if not isinstance(values, list):
        raise AppError(f"{label} 必须是整数数组")
    return [to_positive_int(v, label) for v in values]


def account_matches_local_filters(account: dict[str, Any], filters: AccountFilters) -> bool:
    account_id = account.get("id")
    if not isinstance(account_id, int) or account_id <= 0:
        return False

    if filters.account_ids and account_id not in set(filters.account_ids):
        return False

    group_ids_raw = account.get("group_ids")
    group_ids = group_ids_raw if isinstance(group_ids_raw, list) else []
    group_id_set = {gid for gid in group_ids if isinstance(gid, int) and gid > 0}

    if filters.ungrouped_only and group_id_set:
        return False
    if filters.group_ids and group_id_set.isdisjoint(filters.group_ids):
        return False
    if filters.name_contains:
        name = str(account.get("name") or "")
        if filters.name_contains.lower() not in name.lower():
            return False
    return True


def _server_group_filter(filters: AccountFilters) -> int | str | None:
    if filters.ungrouped_only:
        return ACCOUNT_LIST_GROUP_UNGROUPED
    if filters.group_ids and len(filters.group_ids) == 1:
        group_id = filters.group_ids[0]
        if group_id > 0:
            return group_id
    return None


def _requires_full_scan(filters: AccountFilters) -> bool:
    if filters.account_ids:
        return True
    if filters.name_contains:
        return True
    if filters.group_ids and len(filters.group_ids) > 1:
        return True
    if filters.ungrouped_only:
        return True
    return False


def list_accounts_for_view(
    client: AdminAPIClient,
    *,
    filters: AccountFilters,
    page: int,
    page_size: int,
) -> dict[str, Any]:
    effective_page_size = clamp_admin_list_page_size(page_size)
    if not _requires_full_scan(filters):
        payload = client.list_accounts_page(
            page=max(page, 1),
            page_size=effective_page_size,
            platform=filters.platform,
            account_type=filters.account_type,
            status=filters.account_status,
            search=filters.search,
            group_id=_server_group_filter(filters),
        )
        return {
            "items": [
                normalize_account_for_view(item)
                for item in (payload.get("items") or [])
                if isinstance(item, dict)
            ],
            "total": parse_int_field(payload.get("total"), 0),
            "page": max(page, 1),
            "page_size": effective_page_size,
            "mode": "server-page",
        }

    page_no = 1
    matched: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    scan_page_size = min(max(filters.scan_page_size, 1), ADMIN_LIST_PAGE_SIZE_CAP)
    while True:
        payload = client.list_accounts_page(
            page=page_no,
            page_size=scan_page_size,
            platform=filters.platform,
            account_type=filters.account_type,
            status=filters.account_status,
            search=filters.search,
            group_id=_server_group_filter(filters),
        )
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            break
        for item in items:
            if not isinstance(item, dict):
                continue
            if not account_matches_local_filters(item, filters):
                continue
            account_id = item.get("id")
            if not isinstance(account_id, int) or account_id in seen_ids:
                continue
            seen_ids.add(account_id)
            matched.append(normalize_account_for_view(item))
        total = parse_int_field(payload.get("total"), 0)
        if (total > 0 and page_no * scan_page_size >= total) or len(items) < scan_page_size:
            break
        page_no += 1

    matched.sort(key=lambda item: (str(item.get("platform") or ""), str(item.get("name") or ""), int(item.get("id") or 0)))
    total = len(matched)
    start = max(page - 1, 0) * effective_page_size
    end = start + effective_page_size
    return {
        "items": matched[start:end],
        "total": total,
        "page": max(page, 1),
        "page_size": effective_page_size,
        "mode": "full-scan",
    }


def collect_target_account_ids(client: AdminAPIClient, filters: AccountFilters) -> list[int]:
    if filters.account_ids:
        ids = unique_ids(filters.account_ids)
        return ids[: filters.max_accounts] if filters.max_accounts else ids

    page = 1
    seen: set[int] = set()
    target_ids: list[int] = []
    effective_page_size = clamp_admin_list_page_size(filters.scan_page_size)
    fetched_count = 0

    while True:
        page_data = client.list_accounts_page(
            page=page,
            page_size=effective_page_size,
            platform=filters.platform,
            account_type=filters.account_type,
            status=filters.account_status,
            search=filters.search,
            group_id=_server_group_filter(filters),
        )
        items = page_data.get("items")
        if not isinstance(items, list) or not items:
            break

        fetched_count += len(items)
        total = parse_int_field(page_data.get("total"), 0)
        for raw in items:
            if not isinstance(raw, dict):
                continue
            if not account_matches_local_filters(raw, filters):
                continue
            account_id = raw.get("id")
            if not isinstance(account_id, int) or account_id in seen:
                continue
            seen.add(account_id)
            target_ids.append(account_id)
            if filters.max_accounts and len(target_ids) >= filters.max_accounts:
                return target_ids

        if (total > 0 and fetched_count >= total) or len(items) < effective_page_size:
            break
        page += 1

    return target_ids


def chunked_ids(ids: list[int], size: int) -> list[list[int]]:
    if size <= 0:
        raise AppError("批量大小必须为正整数")
    return [ids[i : i + size] for i in range(0, len(ids), size)]


def bulk_update_all_accounts(
    client: AdminAPIClient,
    *,
    filters: AccountFilters,
    updates: dict[str, Any],
    dry_run: bool,
) -> dict[str, Any]:
    if "account_ids" in updates:
        raise AppError("更新字段中不允许包含 account_ids")
    updates = ensure_dict(updates, "批量修改内容")
    if not updates:
        raise AppError("请至少填写一个更新字段")

    single_update_only_keys = sorted(key for key in updates.keys() if key not in BULK_SUPPORTED_KEYS)
    target_ids = collect_target_account_ids(client, filters)
    if dry_run:
        return {
            "matched": len(target_ids),
            "sample_ids": target_ids[:20],
            "updates": updates,
            "update_mode": "single" if single_update_only_keys else "bulk",
            "single_update_only_keys": single_update_only_keys,
        }

    if not target_ids:
        return {
            "matched": 0,
            "updated_success": 0,
            "updated_failed": 0,
            "message": "没有匹配到任何账号",
        }

    updated_success = 0
    updated_failed = 0
    success_ids: list[int] = []
    failed_ids: list[int] = []
    results: list[dict[str, Any]] = []

    if single_update_only_keys:
        for account_id in target_ids:
            try:
                client.update_account(account_id, updates)
                updated_success += 1
                success_ids.append(account_id)
                results.append({"account_id": account_id, "success": True})
            except Exception as exc:
                updated_failed += 1
                failed_ids.append(account_id)
                results.append({"account_id": account_id, "success": False, "error": str(exc)})
        update_mode = "single"
        batches = len(target_ids)
    else:
        batches_payload = chunked_ids(target_ids, max(filters.batch_size, 1))
        for batch in batches_payload:
            payload = dict(updates)
            payload["account_ids"] = batch
            result = client.bulk_update_accounts(payload)
            updated_success += parse_int_field(result.get("success"), 0)
            updated_failed += parse_int_field(result.get("failed"), 0)
            success_ids.extend(to_int_list(result.get("success_ids") or [], "`success_ids`") if isinstance(result.get("success_ids"), list) else [])
            failed_ids.extend(to_int_list(result.get("failed_ids") or [], "`failed_ids`") if isinstance(result.get("failed_ids"), list) else [])
            if isinstance(result.get("results"), list):
                results.extend([row for row in result["results"] if isinstance(row, dict)])
        update_mode = "bulk"
        batches = len(batches_payload)

    return {
        "matched": len(target_ids),
        "batches": batches,
        "updated_success": updated_success,
        "updated_failed": updated_failed,
        "update_mode": update_mode,
        "single_update_only_keys": single_update_only_keys,
        "success_ids": success_ids,
        "failed_ids": failed_ids,
        "results": results,
    }


def bulk_delete_all_accounts(
    client: AdminAPIClient,
    *,
    filters: AccountFilters,
    dry_run: bool,
) -> dict[str, Any]:
    target_ids = collect_target_account_ids(client, filters)
    if dry_run:
        return {
            "matched": len(target_ids),
            "sample_ids": target_ids[:20],
            "delete_mode": "single",
        }

    if not target_ids:
        return {
            "matched": 0,
            "deleted_success": 0,
            "deleted_failed": 0,
            "delete_mode": "single",
            "message": "没有匹配到任何账号",
            "success_ids": [],
            "failed_ids": [],
            "results": [],
        }

    deleted_success = 0
    deleted_failed = 0
    success_ids: list[int] = []
    failed_ids: list[int] = []
    results: list[dict[str, Any]] = []

    for account_id in target_ids:
        try:
            client.delete_account(account_id)
            deleted_success += 1
            success_ids.append(account_id)
            results.append({"account_id": account_id, "success": True})
        except Exception as exc:
            deleted_failed += 1
            failed_ids.append(account_id)
            results.append({"account_id": account_id, "success": False, "error": str(exc)})

    return {
        "matched": len(target_ids),
        "deleted_success": deleted_success,
        "deleted_failed": deleted_failed,
        "delete_mode": "single",
        "success_ids": success_ids,
        "failed_ids": failed_ids,
        "results": results,
    }


def proxy_key_from_proxy(proxy: dict[str, Any]) -> str | None:
    protocol = non_empty(str(proxy.get("protocol") or ""))
    host = non_empty(str(proxy.get("host") or ""))
    port = parse_int_field(proxy.get("port"), 0)
    if not protocol or not host or port <= 0:
        return non_empty(str(proxy.get("proxy_key") or ""))
    return build_proxy_key(protocol, host, port, str(proxy.get("username") or ""), str(proxy.get("password") or ""))


def preview_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2)
