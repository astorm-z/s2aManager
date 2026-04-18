from __future__ import annotations

import os
import secrets
from dataclasses import asdict, dataclass
from datetime import timedelta, timezone, tzinfo
from functools import lru_cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml

from .errors import ConfigError


@dataclass
class AppSettings:
    host: str = "127.0.0.1"
    port: int = 8000
    session_cookie_name: str = "s2a_manager_session"
    session_secret: str = ""
    web_password: str = ""
    timezone: str = "Asia/Shanghai"


@dataclass
class Sub2ApiSettings:
    base_url: str = "http://127.0.0.1:8080"
    admin_api_key: str = ""
    timeout: float = 30.0
    insecure: bool = False


@dataclass
class SiteSettings(Sub2ApiSettings):
    key: str = "default"
    name: str = "默认站点"


@dataclass
class UISettings:
    default_list_page_size: int = 20
    default_bulk_scan_page_size: int = 100
    default_bulk_batch_size: int = 100
    import_skip_default_group_bind: bool = True


@dataclass
class Settings:
    app: AppSettings
    sites: list[SiteSettings]
    default_site_key: str
    ui: UISettings


FALLBACK_TIMEZONES: dict[str, tzinfo] = {
    "Asia/Shanghai": timezone(timedelta(hours=8), name="Asia/Shanghai"),
    "UTC": timezone.utc,
    "Etc/UTC": timezone.utc,
}


def resolve_timezone(value: str) -> tzinfo:
    timezone_name = str(value or "").strip() or "Asia/Shanghai"
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        fallback = FALLBACK_TIMEZONES.get(timezone_name)
        if fallback is not None:
            return fallback
        raise


def validate_timezone_name(value: str) -> str:
    timezone_name = str(value or "").strip() or "Asia/Shanghai"
    try:
        resolve_timezone(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ConfigError(
            f"配置文件字段无效: `app.timezone`=`{timezone_name}` 无法解析。"
            "如果你在 Windows 上运行且未安装 tzdata，请先执行 `uv sync` 安装依赖，"
            "或暂时改为内置兜底支持的 `Asia/Shanghai` / `UTC`。"
        ) from exc
    return timezone_name


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def get_config_path() -> Path:
    return get_project_root() / "config.yaml"


def build_default_settings() -> Settings:
    return Settings(
        app=AppSettings(session_secret=secrets.token_hex(32)),
        sites=[SiteSettings()],
        default_site_key="default",
        ui=UISettings(),
    )


def _dump_settings_to_yaml(settings: Settings) -> str:
    payload = asdict(settings)
    return yaml.safe_dump(payload, allow_unicode=True, sort_keys=False)


def ensure_config_file() -> Path:
    path = get_config_path()
    if path.exists():
        return path
    path.write_text(_dump_settings_to_yaml(build_default_settings()), encoding="utf-8")
    return path


def _merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        current = result.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            result[key] = _merge_dict(current, value)
        else:
            result[key] = value
    return result


def _normalize_raw_config(raw: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(raw)
    if "sites" not in normalized:
        legacy_sub2api = normalized.get("sub2api")
        if legacy_sub2api is None:
            normalized["sites"] = [asdict(SiteSettings())]
            normalized["default_site_key"] = "default"
            return normalized
        if not isinstance(legacy_sub2api, dict):
            raise ConfigError("配置文件格式错误: `sub2api` 必须是对象")
        site_payload = dict(legacy_sub2api)
        site_payload.setdefault("key", "default")
        site_payload.setdefault("name", "默认站点")
        normalized["sites"] = [site_payload]
        normalized.setdefault("default_site_key", "default")
    return normalized


def _build_sites(raw_sites: Any) -> list[SiteSettings]:
    if not isinstance(raw_sites, list):
        raise ConfigError("配置文件格式错误: `sites` 必须是数组")
    if not raw_sites:
        raise ConfigError("配置文件格式错误: `sites` 至少要配置一个站点")

    sites: list[SiteSettings] = []
    seen_keys: set[str] = set()
    for index, raw_site in enumerate(raw_sites):
        if not isinstance(raw_site, dict):
            raise ConfigError(f"配置文件格式错误: `sites[{index}]` 必须是对象")
        try:
            site = SiteSettings(**raw_site)
        except TypeError as exc:
            raise ConfigError(f"配置文件格式错误: `sites[{index}]` 字段无效: {exc}") from exc

        site.key = site.key.strip()
        site.name = site.name.strip()
        if not site.key:
            raise ConfigError(f"配置文件格式错误: `sites[{index}].key` 不能为空")
        if site.key in seen_keys:
            raise ConfigError(f"配置文件格式错误: 站点 key 重复: `{site.key}`")
        if not site.name:
            site.name = site.key
        seen_keys.add(site.key)
        sites.append(site)
    return sites


def load_settings() -> Settings:
    path = ensure_config_file()
    defaults = asdict(build_default_settings())
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"配置文件不是合法 YAML: {path}: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"读取配置文件失败: {path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise ConfigError(f"配置文件格式错误: {path} 顶层必须是对象")

    normalized_raw = _normalize_raw_config(raw)
    merged = _merge_dict(defaults, normalized_raw)
    try:
        app_settings = AppSettings(**merged.get("app", {}))
        ui_settings = UISettings(**merged.get("ui", {}))
    except TypeError as exc:
        raise ConfigError(f"配置文件字段无效: {exc}") from exc

    sites = _build_sites(merged.get("sites", []))
    site_keys = {site.key for site in sites}
    default_site_key = str(merged.get("default_site_key") or "").strip() or sites[0].key
    if default_site_key not in site_keys:
        raise ConfigError(f"配置文件格式错误: `default_site_key`=`{default_site_key}` 不存在于 `sites` 中")

    settings = Settings(
        app=app_settings,
        sites=sites,
        default_site_key=default_site_key,
        ui=ui_settings,
    )
    settings.app.timezone = validate_timezone_name(settings.app.timezone)
    if not settings.app.session_secret.strip():
        settings.app.session_secret = secrets.token_hex(32)
        path.write_text(_dump_settings_to_yaml(settings), encoding="utf-8")
    return settings


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()


def get_app_timezone() -> tzinfo:
    return resolve_timezone(get_settings().app.timezone)


def get_web_password() -> str:
    password = os.environ.get("S2A_MANAGER_WEB_PASSWORD", "").strip()
    if password:
        return password

    config_password = get_settings().app.web_password.strip()
    if config_password:
        return config_password

    if not password:
        raise ConfigError(
            "缺少网站登录密码。请设置环境变量 `S2A_MANAGER_WEB_PASSWORD`，"
            "或在 `config.yaml` 的 `app.web_password` 中填写。"
        )
    return password
