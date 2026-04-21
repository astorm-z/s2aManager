from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from fastapi import FastAPI, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .errors import AppError
from .logic import (
    ACCOUNT_LIST_GROUP_UNGROUPED,
    AccountFilters,
    build_import_overrides,
    bulk_delete_all_accounts,
    bulk_update_all_accounts,
    inspect_import_source,
    list_accounts_for_view,
    non_empty,
    parse_uploaded_payload,
    preview_json,
    proxy_key_from_proxy,
)
from .scheduled_tasks import (
    SCHEDULE_MODE_DELAY,
    SCHEDULE_MODE_IMMEDIATE,
    SCHEDULE_MODE_RUN_AT,
    TASK_STATUSES,
    ScheduledTaskRepository,
    ScheduledTaskRunner,
    build_task_view,
    parse_local_datetime_input,
)
from .settings import SiteSettings, get_app_timezone, get_project_root, get_settings, get_web_password
from .sub2api import AdminAPIClient, clamp_admin_list_page_size

KEEP_OPTION = "__KEEP__"
CLEAR_OPTION = "__CLEAR__"
SET_OPTION = "__SET__"
ENABLE_OPTION = "__ENABLE__"
DISABLE_OPTION = "__DISABLE__"

settings = get_settings()
app_timezone = get_app_timezone()
get_web_password()
scheduled_task_repository = ScheduledTaskRepository(get_project_root() / "data" / "scheduler.db")

app = FastAPI(title="S2A Manager Web")
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.app.session_secret,
    session_cookie=settings.app.session_cookie_name,
    same_site="lax",
    https_only=False,
)
app.mount("/static", StaticFiles(directory=str(get_project_root() / "s2a_manager_web" / "static")), name="static")
templates = Jinja2Templates(directory=str(get_project_root() / "s2a_manager_web" / "templates"))


@app.on_event("startup")
async def startup_event() -> None:
    scheduled_task_runner.start()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    scheduled_task_runner.stop()


def get_default_site() -> SiteSettings:
    return next((site for site in settings.sites if site.key == settings.default_site_key), settings.sites[0])


def get_site_by_key(site_key: str) -> SiteSettings:
    for site in settings.sites:
        if site.key == site_key:
            return site
    raise AppError(f"站点 `{site_key}` 不存在，请检查配置")


def resolve_site_key(raw_value: Any | None) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return get_default_site().key
    return get_site_by_key(value).key


def get_current_site(request: Request, payload: Any | None = None) -> SiteSettings:
    candidate = None
    if payload is not None and hasattr(payload, "get"):
        candidate = payload.get("site_key")
    if candidate is None:
        candidate = request.query_params.get("site_key")
    if candidate is None:
        candidate = request.session.get("site_key")
    site_key = resolve_site_key(candidate)
    request.session["site_key"] = site_key
    return get_site_by_key(site_key)


def get_client(site_key: str | None = None) -> AdminAPIClient:
    return AdminAPIClient(get_site_by_key(resolve_site_key(site_key)))


def execute_scheduled_task(task: dict[str, Any]) -> dict[str, Any]:
    site_key = resolve_site_key(task.get("site_key"))
    filters_payload = task.get("filters")
    updates_payload = task.get("updates")
    if not isinstance(filters_payload, dict):
        raise AppError("延时任务缺少筛选条件")
    if not isinstance(updates_payload, dict):
        raise AppError("延时任务缺少更新内容")
    filters = AccountFilters(**filters_payload)
    return bulk_update_all_accounts(get_client(site_key), filters=filters, updates=updates_payload, dry_run=False)


scheduled_task_runner = ScheduledTaskRunner(scheduled_task_repository, execute_scheduled_task)


def is_authenticated(request: Request) -> bool:
    return bool(request.session.get("authenticated"))


def require_auth(request: Request) -> None:
    if not is_authenticated(request):
        raise AppError("登录已失效，请重新登录。")


def render(request: Request, template_name: str, context: dict[str, Any], status_code: int = 200) -> HTMLResponse:
    current_site = context.get("current_site")
    if not isinstance(current_site, SiteSettings):
        current_site = get_default_site()
    base_context = {
        "request": request,
        "settings": settings,
        "app_timezone_name": settings.app.timezone,
        "site_options": settings.sites,
        "current_site": current_site,
        "selected_site_key": current_site.key,
        "active_tab": "manage",
    }
    base_context.update(context)
    return templates.TemplateResponse(request, template_name, base_context, status_code=status_code)


def parse_optional_positive_int(raw: str | None, label: str) -> int | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise AppError(f"{label} 必须是整数") from exc
    if parsed <= 0:
        raise AppError(f"{label} 必须为正整数")
    return parsed


def parse_optional_int(raw: str | None, label: str) -> int | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise AppError(f"{label} 必须是整数") from exc


def parse_optional_float(raw: str | None, label: str, *, min_value: float | None = None) -> float | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        parsed = float(value)
    except ValueError as exc:
        raise AppError(f"{label} 必须是数字") from exc
    if min_value is not None and parsed < min_value:
        raise AppError(f"{label} 不能小于 {min_value}")
    return parsed


def parse_optional_bool_choice(value: str | None, label: str) -> tuple[bool, bool | None]:
    selected = str(value or "").strip()
    if not selected or selected == KEEP_OPTION:
        return False, None
    if selected == ENABLE_OPTION:
        return True, True
    if selected in {DISABLE_OPTION, CLEAR_OPTION}:
        return True, False
    raise AppError(f"{label} 选项无效")


def parse_optional_timestamp(raw: str | None, label: str) -> int | None:
    value = str(raw or "").strip()
    if not value:
        return None
    if re.fullmatch(r"\d+", value):
        return int(value)
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise AppError(f"{label} 必须是 Unix 秒时间戳或 ISO 时间，例如 2026-03-20T18:30:00+08:00") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def parse_json_text(raw: str | None, label: str, *, require_dict: bool = False) -> Any:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AppError(f"{label} 不是合法 JSON: {exc}") from exc
    if require_dict and not isinstance(parsed, dict):
        raise AppError(f"{label} 必须是 JSON 对象")
    return parsed


def parse_id_text(raw: str | None, label: str) -> list[int] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    parts = [item for item in re.split(r"[\s,，;；]+", text) if item]
    try:
        values = [int(item) for item in parts]
    except ValueError as exc:
        raise AppError(f"{label} 只能包含整数 ID") from exc
    result: list[int] = []
    seen: set[int] = set()
    for value in values:
        if value <= 0:
            raise AppError(f"{label} 只能包含正整数 ID")
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def get_form_list(form: Any, key: str) -> list[str]:
    if hasattr(form, "getlist"):
        values = form.getlist(key)
    else:
        raw = form.get(key)
        if raw is None:
            values = []
        elif isinstance(raw, list):
            values = raw
        else:
            values = [raw]
    return [str(item).strip() for item in values if str(item).strip()]


def parse_group_id_list(values: list[str], label: str) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    for raw in values:
        try:
            group_id = int(raw)
        except ValueError as exc:
            raise AppError(f"{label} 选项无效") from exc
        if group_id <= 0:
            raise AppError(f"{label} 选项无效")
        if group_id in seen:
            continue
        seen.add(group_id)
        result.append(group_id)
    return result


def select_group_ids(raw: str | None) -> list[int] | None:
    value = str(raw or "").strip()
    if not value:
        return None
    if value == ACCOUNT_LIST_GROUP_UNGROUPED:
        return None
    try:
        group_id = int(value)
    except ValueError as exc:
        raise AppError("分组选项无效") from exc
    return [group_id]


def build_filters_from_form(form: Any) -> tuple[AccountFilters, dict[str, Any]]:
    table_page = parse_optional_positive_int(form.get("table_page"), "当前页") or 1
    table_page_size = parse_optional_positive_int(form.get("table_page_size"), "列表分页大小") or settings.ui.default_list_page_size
    ungrouped_only = str(form.get("ungrouped_only") or "") == "on"
    selected_group_ids = None if ungrouped_only else select_group_ids(form.get("group_id"))
    filters = AccountFilters(
        account_ids=parse_id_text(form.get("account_ids"), "指定账号 ID"),
        platform=non_empty(form.get("platform")),
        account_type=non_empty(form.get("account_type")),
        account_status=non_empty(form.get("account_status")),
        search=non_empty(form.get("search")),
        name_contains=non_empty(form.get("name_contains")),
        group_ids=selected_group_ids,
        ungrouped_only=ungrouped_only,
        max_accounts=parse_optional_positive_int(form.get("max_accounts"), "最多处理数量"),
        scan_page_size=parse_optional_positive_int(form.get("scan_page_size"), "批量扫描分页大小") or settings.ui.default_bulk_scan_page_size,
        batch_size=parse_optional_positive_int(form.get("batch_size"), "批量提交大小") or settings.ui.default_bulk_batch_size,
    )
    state = {
        "account_ids": form.get("account_ids", ""),
        "platform": form.get("platform", ""),
        "account_type": form.get("account_type", ""),
        "account_status": form.get("account_status", ""),
        "search": form.get("search", ""),
        "name_contains": form.get("name_contains", ""),
        "group_id": form.get("group_id", ""),
        "ungrouped_only": ungrouped_only,
        "max_accounts": form.get("max_accounts", ""),
        "scan_page_size": str(filters.scan_page_size),
        "batch_size": str(filters.batch_size),
        "table_page": table_page,
        "table_page_size": clamp_admin_list_page_size(table_page_size),
    }
    return filters, state


def build_manage_updates(form: Any) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    group_action = str(form.get("update_group_action") or "").strip()
    if group_action and group_action != KEEP_OPTION:
        if group_action == CLEAR_OPTION:
            updates["group_ids"] = []
        elif group_action == SET_OPTION:
            group_ids = parse_group_id_list(get_form_list(form, "update_group_ids"), "目标分组")
            if not group_ids:
                raise AppError("请选择至少一个目标分组")
            updates["group_ids"] = group_ids
        else:
            raise AppError("目标分组选项无效")

    proxy_update = str(form.get("update_proxy_id") or "").strip()
    if proxy_update:
        if proxy_update == CLEAR_OPTION:
            updates["proxy_id"] = 0
        else:
            updates["proxy_id"] = int(proxy_update)

    update_status = non_empty(form.get("update_status"))
    if update_status:
        updates["status"] = update_status

    has_schedulable, schedulable_value = parse_optional_bool_choice(form.get("update_schedulable"), "允许调度")
    if has_schedulable:
        updates["schedulable"] = bool(schedulable_value)

    update_name = non_empty(form.get("update_name"))
    if update_name is not None:
        updates["name"] = update_name

    notes_mode = str(form.get("update_notes_mode") or "").strip()
    notes_text = str(form.get("update_notes") or "").strip()
    if notes_mode == CLEAR_OPTION:
        updates["notes"] = None
    elif notes_text:
        updates["notes"] = notes_text

    update_type = non_empty(form.get("update_type"))
    if update_type:
        updates["type"] = update_type

    update_concurrency = parse_optional_positive_int(form.get("update_concurrency"), "并发数")
    if update_concurrency is not None:
        updates["concurrency"] = update_concurrency

    load_factor_mode = str(form.get("update_load_factor_mode") or "").strip()
    load_factor_raw = str(form.get("update_load_factor") or "").strip()
    if load_factor_mode == CLEAR_OPTION:
        updates["load_factor"] = None
    elif load_factor_raw:
        load_factor = parse_optional_int(load_factor_raw, "负载因子")
        if load_factor is None:
            raise AppError("负载因子不能为空")
        updates["load_factor"] = load_factor

    update_priority = parse_optional_int(form.get("update_priority"), "优先级")
    if update_priority is not None:
        updates["priority"] = update_priority

    rate_multiplier = parse_optional_float(form.get("update_rate_multiplier"), "速率倍率", min_value=0.0)
    if rate_multiplier is not None:
        updates["rate_multiplier"] = rate_multiplier

    expires_at_mode = str(form.get("update_expires_at_mode") or "").strip()
    expires_at_raw = str(form.get("update_expires_at") or "").strip()
    if expires_at_mode == CLEAR_OPTION:
        updates["expires_at"] = None
    elif expires_at_raw:
        updates["expires_at"] = parse_optional_timestamp(expires_at_raw, "过期时间")

    has_auto_pause, auto_pause_value = parse_optional_bool_choice(form.get("update_auto_pause"), "到期自动暂停")
    if has_auto_pause:
        updates["auto_pause_on_expired"] = bool(auto_pause_value)

    if str(form.get("confirm_mixed_channel_risk") or "") == "on":
        updates["confirm_mixed_channel_risk"] = True

    credentials_payload = parse_json_text(form.get("update_credentials_json"), "账号 credentials JSON", require_dict=True)
    if credentials_payload is not None:
        updates["credentials"] = credentials_payload

    extra_payload = parse_json_text(form.get("update_extra_json"), "账号 extra JSON", require_dict=True)
    if extra_payload is not None:
        updates["extra"] = extra_payload

    manual_updates = parse_json_text(form.get("manual_updates_json"), "底层补充更新 JSON", require_dict=True)
    if manual_updates is not None:
        updates.update(manual_updates)

    if "account_ids" in updates:
        raise AppError("更新字段中不允许包含 account_ids")
    if not updates:
        raise AppError("请至少填写一个更新字段")
    return updates


def parse_schedule_payload(form: Any) -> dict[str, Any]:
    schedule_mode = str(form.get("schedule_mode") or SCHEDULE_MODE_IMMEDIATE).strip() or SCHEDULE_MODE_IMMEDIATE
    if schedule_mode not in {SCHEDULE_MODE_IMMEDIATE, SCHEDULE_MODE_DELAY, SCHEDULE_MODE_RUN_AT}:
        raise AppError("执行方式无效")
    if schedule_mode == SCHEDULE_MODE_IMMEDIATE:
        return {
            "schedule_mode": schedule_mode,
            "delay_minutes": None,
            "run_at_utc": None,
            "run_at_display": "立即执行",
        }
    if schedule_mode == SCHEDULE_MODE_DELAY:
        delay_minutes = parse_optional_positive_int(form.get("delay_minutes"), "延时时间（分钟）")
        if delay_minutes is None:
            raise AppError("请填写延时时间（分钟）")
        run_at_utc = int(datetime.now(app_timezone).timestamp()) + delay_minutes * 60
        run_at_display = datetime.fromtimestamp(run_at_utc, tz=timezone.utc).astimezone(app_timezone).strftime("%Y-%m-%d %H:%M:%S")
        return {
            "schedule_mode": schedule_mode,
            "delay_minutes": delay_minutes,
            "run_at_utc": run_at_utc,
            "run_at_display": run_at_display,
        }

    run_at_local = parse_local_datetime_input(str(form.get("run_at_local") or ""), app_timezone)
    now_local = datetime.now(app_timezone)
    if run_at_local <= now_local:
        raise AppError(f"指定执行时间必须晚于当前时间（{now_local.strftime('%Y-%m-%d %H:%M:%S')} {settings.app.timezone}）")
    return {
        "schedule_mode": schedule_mode,
        "delay_minutes": None,
        "run_at_utc": int(run_at_local.timestamp()),
        "run_at_display": run_at_local.strftime("%Y-%m-%d %H:%M:%S"),
    }


def normalize_task_status_filter(raw_value: Any | None) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    if value not in TASK_STATUSES:
        raise AppError("任务状态筛选无效")
    return value


def build_scheduled_tasks_context(site_key: str, status_filter: str = "", *, message: str | None = None, error: str | None = None) -> dict[str, Any]:
    tasks = [
        build_task_view(task, app_timezone)
        for task in scheduled_task_repository.list_tasks(site_key=site_key, status=status_filter or None)
    ]
    return {
        "tasks": tasks,
        "status_filter": status_filter,
        "status_options": [("", "全部"), ("pending", "待执行"), ("running", "执行中"), ("success", "成功"), ("failed", "失败"), ("cancelled", "已取消")],
        "message": message,
        "error": error,
    }


def render_scheduled_tasks_partial(
    request: Request,
    *,
    current_site: SiteSettings,
    status_filter: str = "",
    message: str | None = None,
    error: str | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    context = build_scheduled_tasks_context(current_site.key, status_filter, message=message, error=error)
    context["current_site"] = current_site
    return render(request, "partials/scheduled_tasks.html", context, status_code=status_code)


def ensure_task_belongs_to_site(task_id: int, site_key: str) -> dict[str, Any]:
    task = scheduled_task_repository.get_task(task_id)
    if task is None or resolve_site_key(task.get("site_key")) != resolve_site_key(site_key):
        raise AppError("延时任务不存在，或不属于当前站点")
    return task


def default_manage_form_state() -> dict[str, Any]:
    return {
        "account_ids": "",
        "platform": "",
        "account_type": "",
        "account_status": "",
        "search": "",
        "name_contains": "",
        "group_id": "",
        "ungrouped_only": False,
        "max_accounts": "",
        "scan_page_size": str(settings.ui.default_bulk_scan_page_size),
        "batch_size": str(settings.ui.default_bulk_batch_size),
        "table_page": 1,
        "table_page_size": settings.ui.default_list_page_size,
        "update_group_action": KEEP_OPTION,
        "update_group_ids": [],
        "update_proxy_id": "",
        "update_status": "",
        "update_schedulable": KEEP_OPTION,
        "update_name": "",
        "update_notes_mode": KEEP_OPTION,
        "update_notes": "",
        "update_type": "",
        "update_concurrency": "",
        "update_load_factor_mode": KEEP_OPTION,
        "update_load_factor": "",
        "update_priority": "",
        "update_rate_multiplier": "",
        "update_expires_at_mode": KEEP_OPTION,
        "update_expires_at": "",
        "update_auto_pause": KEEP_OPTION,
        "confirm_mixed_channel_risk": False,
        "update_credentials_json": "",
        "update_extra_json": "",
        "manual_updates_json": "",
        "schedule_mode": SCHEDULE_MODE_IMMEDIATE,
        "delay_minutes": "",
        "run_at_local": "",
    }


def default_import_form_state() -> dict[str, Any]:
    return {
        "import_group_id": "",
        "import_proxy_id": "",
        "import_status": "",
        "import_schedulable": KEEP_OPTION,
        "import_notes": "",
        "import_concurrency": "",
        "import_priority": "",
        "import_load_factor": "",
        "import_rate_multiplier": "",
        "import_expires_at": "",
        "import_auto_pause": KEEP_OPTION,
        "import_skip_default_group_bind": settings.ui.import_skip_default_group_bind,
        "import_credentials_json": "",
        "import_extra_json": "",
        "import_manual_updates_json": "",
    }


def load_reference_options(client: AdminAPIClient) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    groups: list[dict[str, Any]] = []
    proxies: list[dict[str, Any]] = []
    errors: list[str] = []

    def load_groups() -> list[dict[str, Any]]:
        return client.list_groups()

    def load_proxies() -> list[dict[str, Any]]:
        return client.list_proxies()

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_groups = executor.submit(load_groups)
        future_proxies = executor.submit(load_proxies)

        try:
            groups = future_groups.result()
        except Exception as exc:
            errors.append(str(exc))

        try:
            proxies = future_proxies.result()
        except Exception as exc:
            errors.append(str(exc))

    return groups, proxies, errors


def build_accounts_partial_context(form_state: dict[str, Any], client: AdminAPIClient) -> dict[str, Any]:
    filters, normalized_state = build_filters_from_form(form_state)
    normalized_state.update({key: form_state.get(key, value) for key, value in normalized_state.items() if key in form_state})
    normalized_state["site_key"] = str(form_state.get("site_key") or "")
    view = list_accounts_for_view(
        client,
        filters=filters,
        page=int(normalized_state["table_page"]),
        page_size=int(normalized_state["table_page_size"]),
    )
    total = int(view["total"])
    page = int(view["page"])
    page_size = int(view["page_size"])
    total_pages = max((total + page_size - 1) // page_size, 1) if total > 0 else 1
    return {
        "accounts": view["items"],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "loaded": True,
        "query_mode": view["mode"],
        "form_state": normalized_state,
    }


def build_accounts_partial_refresh_url(form_state: dict[str, Any]) -> str:
    query: dict[str, str] = {
        "site_key": str(form_state.get("site_key") or ""),
        "account_ids": str(form_state.get("account_ids") or ""),
        "platform": str(form_state.get("platform") or ""),
        "account_type": str(form_state.get("account_type") or ""),
        "account_status": str(form_state.get("account_status") or ""),
        "search": str(form_state.get("search") or ""),
        "name_contains": str(form_state.get("name_contains") or ""),
        "group_id": str(form_state.get("group_id") or ""),
        "max_accounts": str(form_state.get("max_accounts") or ""),
        "scan_page_size": str(form_state.get("scan_page_size") or ""),
        "batch_size": str(form_state.get("batch_size") or ""),
        "table_page_size": str(form_state.get("table_page_size") or ""),
        "table_page": str(form_state.get("table_page") or 1),
    }
    if form_state.get("ungrouped_only"):
        query["ungrouped_only"] = "on"
    return f"/partials/accounts?{urlencode(query)}"


def build_import_overrides_from_form(form: dict[str, Any], proxies: list[dict[str, Any]]) -> dict[str, Any]:
    proxy_id_raw = str(form.get("import_proxy_id") or "").strip()
    proxy_key = None
    if proxy_id_raw:
        proxy_id = int(proxy_id_raw)
        selected_proxy = next((item for item in proxies if int(item.get("id") or 0) == proxy_id), None)
        if selected_proxy is None:
            raise AppError("导入页选择的代理不存在，请刷新页面后重试")
        proxy_key = proxy_key_from_proxy(selected_proxy)

    group_id_raw = str(form.get("import_group_id") or "").strip()
    group_id = int(group_id_raw) if group_id_raw else None

    has_schedulable, schedulable_value = parse_optional_bool_choice(form.get("import_schedulable"), "导入默认允许调度")
    has_auto_pause, auto_pause_value = parse_optional_bool_choice(form.get("import_auto_pause"), "导入默认到期自动暂停")

    credentials = parse_json_text(form.get("import_credentials_json"), "导入默认 credentials JSON", require_dict=True)
    extra = parse_json_text(form.get("import_extra_json"), "导入默认 extra JSON", require_dict=True)
    manual_updates = parse_json_text(form.get("import_manual_updates_json"), "导入默认补充 JSON", require_dict=True)

    return build_import_overrides(
        group_id=group_id,
        proxy_key=proxy_key,
        status=non_empty(form.get("import_status")),
        schedulable=bool(schedulable_value) if has_schedulable else None,
        notes=non_empty(form.get("import_notes")),
        concurrency=parse_optional_positive_int(form.get("import_concurrency"), "导入默认并发数"),
        priority=parse_optional_int(form.get("import_priority"), "导入默认优先级"),
        load_factor=parse_optional_int(form.get("import_load_factor"), "导入默认负载因子"),
        rate_multiplier=parse_optional_float(form.get("import_rate_multiplier"), "导入默认速率倍率", min_value=0.0),
        expires_at=parse_optional_timestamp(form.get("import_expires_at"), "导入默认过期时间"),
        auto_pause_on_expired=bool(auto_pause_value) if has_auto_pause else None,
        credentials=credentials,
        extra=extra,
        manual_updates=manual_updates,
    )


@app.get("/", response_class=HTMLResponse)
async def root(request: Request) -> HTMLResponse:
    if is_authenticated(request):
        return RedirectResponse("/app", status_code=302)
    return RedirectResponse("/login", status_code=302)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    if is_authenticated(request):
        return RedirectResponse("/app", status_code=302)
    return render(request, "login.html", {"error": None, "current_site": get_default_site()})


@app.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request, password: str = Form(...)) -> HTMLResponse:
    expected_password = get_web_password()
    if password != expected_password:
        return render(request, "login.html", {"error": "密码错误", "current_site": get_default_site()}, status_code=400)
    request.session["authenticated"] = True
    return RedirectResponse("/app", status_code=302)


@app.post("/logout")
async def logout(request: Request) -> RedirectResponse:
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


@app.get("/app", response_class=HTMLResponse)
async def dashboard(request: Request, load_refs: bool = False, tab: str = "manage") -> HTMLResponse:
    if not is_authenticated(request):
        return RedirectResponse("/login", status_code=302)

    site_error: str | None = None
    try:
        current_site = get_current_site(request)
    except Exception as exc:
        current_site = get_default_site()
        request.session["site_key"] = current_site.key
        site_error = str(exc)

    active_tab = tab if tab in {"manage", "import", "tasks"} else "manage"
    manage_form = default_manage_form_state()
    import_form = default_import_form_state()
    tasks_context = build_scheduled_tasks_context(current_site.key)
    groups: list[dict[str, Any]] = []
    proxies: list[dict[str, Any]] = []
    load_errors: list[str] = []
    accounts_context: dict[str, Any] = {
        "accounts": [],
        "total": 0,
        "page": 1,
        "page_size": settings.ui.default_list_page_size,
        "total_pages": 1,
        "loaded": False,
        "query_mode": "server-page",
        "form_state": manage_form,
    }
    accounts_error: str | None = None

    if load_refs:
        try:
            groups, proxies, load_errors = load_reference_options(get_client(current_site.key))
        except Exception as exc:
            load_errors = [str(exc)]

    return render(
        request,
        "dashboard.html",
        {
            "active_tab": active_tab,
            "groups": groups,
            "proxies": proxies,
            "load_errors": load_errors,
            "site_error": site_error,
            "manage_form": manage_form,
            "import_form": import_form,
            "tasks_context": tasks_context,
            "accounts_context": accounts_context,
            "accounts_error": accounts_error,
            "current_site": current_site,
        },
    )


@app.get("/partials/accounts", response_class=HTMLResponse)
async def accounts_partial(request: Request) -> HTMLResponse:
    try:
        require_auth(request)
        form_state = dict(request.query_params)
        current_site = get_current_site(request)
        if "site_key" not in form_state:
            form_state["site_key"] = current_site.key
        context = build_accounts_partial_context(form_state, get_client(current_site.key))
        context["current_site"] = current_site
        return render(request, "partials/accounts_table.html", context)
    except Exception as exc:
        return render(request, "partials/error_panel.html", {"title": "账号列表加载失败", "message": str(exc)}, status_code=400)


@app.get("/partials/scheduled-tasks", response_class=HTMLResponse)
async def scheduled_tasks_partial(request: Request) -> HTMLResponse:
    try:
        require_auth(request)
        current_site = get_current_site(request)
        status_filter = normalize_task_status_filter(request.query_params.get("status"))
        return render_scheduled_tasks_partial(request, current_site=current_site, status_filter=status_filter)
    except Exception as exc:
        try:
            current_site = get_current_site(request)
        except Exception:
            current_site = get_default_site()
        return render_scheduled_tasks_partial(request, current_site=current_site, error=str(exc), status_code=400)


@app.post("/actions/bulk/preview", response_class=HTMLResponse)
async def bulk_preview(request: Request) -> HTMLResponse:
    try:
        require_auth(request)
        form = await request.form()
        current_site = get_current_site(request, form)
        filters, _ = build_filters_from_form(form)
        result = bulk_update_all_accounts(get_client(current_site.key), filters=filters, updates=build_manage_updates(form), dry_run=True)
        return render(
            request,
            "partials/bulk_result.html",
            {"result": result, "error": None, "json_preview": preview_json(result), "is_preview": True, "current_site": current_site},
        )
    except Exception as exc:
        return render(request, "partials/bulk_result.html", {"result": None, "error": str(exc), "json_preview": None, "is_preview": True}, status_code=400)


@app.post("/actions/bulk/apply", response_class=HTMLResponse)
async def bulk_apply(request: Request) -> HTMLResponse:
    try:
        require_auth(request)
        form = await request.form()
        current_site = get_current_site(request, form)
        filters, _ = build_filters_from_form(form)
        updates = build_manage_updates(form)
        schedule_payload = parse_schedule_payload(form)
        if schedule_payload["schedule_mode"] == SCHEDULE_MODE_IMMEDIATE:
            result = bulk_update_all_accounts(get_client(current_site.key), filters=filters, updates=updates, dry_run=False)
        else:
            preview_result = bulk_update_all_accounts(get_client(current_site.key), filters=filters, updates=updates, dry_run=True)
            task = scheduled_task_repository.create_bulk_update_task(
                site_key=current_site.key,
                schedule_mode=str(schedule_payload["schedule_mode"]),
                delay_minutes=schedule_payload["delay_minutes"],
                run_at_utc=int(schedule_payload["run_at_utc"]),
                run_at_timezone=settings.app.timezone,
                filters=asdict(filters),
                updates=updates,
                matched_count_preview=int(preview_result.get("matched") or 0),
            )
            scheduled_task_runner.wake()
            result = {
                "task_created": True,
                "task": build_task_view(task, app_timezone),
                "matched": preview_result.get("matched"),
                "sample_ids": preview_result.get("sample_ids") or [],
                "single_update_only_keys": preview_result.get("single_update_only_keys") or [],
                "updates": updates,
                "schedule_mode": schedule_payload["schedule_mode"],
                "run_at_display": schedule_payload["run_at_display"],
                "timezone_name": settings.app.timezone,
            }
        return render(
            request,
            "partials/bulk_result.html",
            {"result": result, "error": None, "json_preview": preview_json(result), "is_preview": False, "current_site": current_site},
        )
    except Exception as exc:
        return render(request, "partials/bulk_result.html", {"result": None, "error": str(exc), "json_preview": None, "is_preview": False}, status_code=400)


@app.post("/actions/bulk-delete/preview", response_class=HTMLResponse)
async def bulk_delete_preview(request: Request) -> HTMLResponse:
    try:
        require_auth(request)
        form = await request.form()
        current_site = get_current_site(request, form)
        filters, form_state = build_filters_from_form(form)
        form_state["site_key"] = current_site.key
        result = bulk_delete_all_accounts(get_client(current_site.key), filters=filters, dry_run=True)
        return render(
            request,
            "partials/delete_result.html",
            {
                "result": result,
                "error": None,
                "json_preview": preview_json(result),
                "is_preview": True,
                "refresh_url": None,
                "current_site": current_site,
            },
        )
    except Exception as exc:
        return render(
            request,
            "partials/delete_result.html",
            {"result": None, "error": str(exc), "json_preview": None, "is_preview": True, "refresh_url": None},
            status_code=400,
        )


@app.post("/actions/bulk-delete/apply", response_class=HTMLResponse)
async def bulk_delete_apply(request: Request) -> HTMLResponse:
    try:
        require_auth(request)
        form = await request.form()
        current_site = get_current_site(request, form)
        filters, form_state = build_filters_from_form(form)
        form_state["site_key"] = current_site.key
        result = bulk_delete_all_accounts(get_client(current_site.key), filters=filters, dry_run=False)
        return render(
            request,
            "partials/delete_result.html",
            {
                "result": result,
                "error": None,
                "json_preview": preview_json(result),
                "is_preview": False,
                "refresh_url": build_accounts_partial_refresh_url(form_state),
                "current_site": current_site,
            },
        )
    except Exception as exc:
        return render(
            request,
            "partials/delete_result.html",
            {"result": None, "error": str(exc), "json_preview": None, "is_preview": False, "refresh_url": None},
            status_code=400,
        )


@app.post("/actions/scheduled-tasks/{task_id}/run-now", response_class=HTMLResponse)
async def scheduled_task_run_now(request: Request, task_id: int) -> HTMLResponse:
    try:
        require_auth(request)
        form = await request.form()
        current_site = get_current_site(request, form)
        status_filter = normalize_task_status_filter(form.get("status"))
        ensure_task_belongs_to_site(task_id, current_site.key)
        scheduled_task_repository.run_now(task_id)
        scheduled_task_runner.wake()
        return render_scheduled_tasks_partial(
            request,
            current_site=current_site,
            status_filter=status_filter,
            message=f"任务 #{task_id} 已改为立即执行。",
        )
    except Exception as exc:
        current_site = get_default_site()
        try:
            form = await request.form()
            current_site = get_current_site(request, form)
            status_filter = normalize_task_status_filter(form.get("status"))
        except Exception:
            status_filter = ""
        return render_scheduled_tasks_partial(request, current_site=current_site, status_filter=status_filter, error=str(exc), status_code=400)


@app.post("/actions/scheduled-tasks/{task_id}/delete", response_class=HTMLResponse)
async def scheduled_task_delete(request: Request, task_id: int) -> HTMLResponse:
    try:
        require_auth(request)
        form = await request.form()
        current_site = get_current_site(request, form)
        status_filter = normalize_task_status_filter(form.get("status"))
        ensure_task_belongs_to_site(task_id, current_site.key)
        scheduled_task_repository.cancel_task(task_id)
        return render_scheduled_tasks_partial(
            request,
            current_site=current_site,
            status_filter=status_filter,
            message=f"任务 #{task_id} 已取消。",
        )
    except Exception as exc:
        current_site = get_default_site()
        try:
            form = await request.form()
            current_site = get_current_site(request, form)
            status_filter = normalize_task_status_filter(form.get("status"))
        except Exception:
            status_filter = ""
        return render_scheduled_tasks_partial(request, current_site=current_site, status_filter=status_filter, error=str(exc), status_code=400)


async def _handle_import_action(request: Request, execute: bool) -> HTMLResponse:
    try:
        require_auth(request)
        form = await request.form()
        current_site = get_current_site(request, form)
        uploaded = form.get("import_file")
        if not isinstance(uploaded, UploadFile):
            return render(
                request,
                "partials/import_result.html",
                {"result": None, "error": "请先选择导入文件", "json_preview": None, "executed": execute, "current_site": current_site},
                status_code=400,
            )

        content = await uploaded.read()
        if not content:
            return render(
                request,
                "partials/import_result.html",
                {"result": None, "error": "上传文件为空", "json_preview": None, "executed": execute, "current_site": current_site},
                status_code=400,
            )

        client = get_client(current_site.key)
        _, proxies, _ = load_reference_options(client)
        raw_payload = parse_uploaded_payload(uploaded.filename or "upload", content)
        overrides = build_import_overrides_from_form(dict(form), proxies)
        skip_default_group_bind = str(form.get("import_skip_default_group_bind") or "") == "on"
        inspected = inspect_import_source(
            raw_payload,
            source_name=uploaded.filename or "upload",
            skip_default_group_bind=skip_default_group_bind,
            overrides=overrides,
        )
        if not inspected["ok"]:
            message = "；".join(inspected["errors"][:5]) or "导入检查失败"
            return render(
                request,
                "partials/import_result.html",
                {"result": inspected, "error": message, "json_preview": None, "executed": execute, "current_site": current_site},
                status_code=400,
            )

        json_preview = preview_json(inspected["request_payload"])
        if not execute:
            return render(
                request,
                "partials/import_result.html",
                {"result": inspected, "error": None, "json_preview": json_preview, "executed": False, "current_site": current_site},
            )

        api_result = client.import_accounts_data(inspected["request_payload"])
        payload = dict(inspected)
        payload["api_result"] = api_result
        return render(
            request,
            "partials/import_result.html",
            {"result": payload, "error": None, "json_preview": json_preview, "executed": True, "current_site": current_site},
        )
    except Exception as exc:
        return render(request, "partials/import_result.html", {"result": None, "error": str(exc), "json_preview": None, "executed": execute}, status_code=400)


@app.post("/actions/import/preview", response_class=HTMLResponse)
async def import_preview(request: Request) -> HTMLResponse:
    return await _handle_import_action(request, execute=False)


@app.post("/actions/import/apply", response_class=HTMLResponse)
async def import_apply(request: Request) -> HTMLResponse:
    return await _handle_import_action(request, execute=True)
