import json
import os
import sys
import time
import shutil
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import importlib.machinery
import importlib.util
from pathlib import Path
from datetime import datetime

from playwright.sync_api import sync_playwright


ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import (
    read_partner_uploaded_dataframe,
    parse_partner_dataframe,
    merge_partner_rows,
    build_partner_source_name,
    apply_onex_period_metadata,
    set_onex_date_range,
    dismiss_onex_blockers,
    safe_text,
)


CRM_BASE_URL = os.getenv("TEAMBEAD_CRM_URL", "https://crm.teambead.work").rstrip("/")
AGENT_API_KEY = os.getenv("TEAMBEAD_ONEX_AGENT_KEY", "teambead-onex-agent").strip() or "teambead-onex-agent"
POLL_INTERVAL = int(os.getenv("TEAMBEAD_ONEX_AGENT_POLL", "3") or "3")
AGENT_VERSION = "1.0-local"
RUNTIME_DIR = ROOT_DIR / "uploaded_data" / "onexbet_runtime"
SESSION_DIR = RUNTIME_DIR / "sessions"
ACCOUNTS_PATH = RUNTIME_DIR / "accounts.json"
AUTORUN_STATE_PATH = RUNTIME_DIR / "autorun_state.json"
LOGIN_URL = "https://1xpartners.com/ru/sign-in"
PLAYERS_REPORT_URL = "https://1xpartners.com/ru/partner/reports/players"
API_TIMEOUT_SECONDS = int(os.getenv("TEAMBEAD_ONEX_API_TIMEOUT", "300") or "300")

PLAYWRIGHT_MANAGER = None
ACTIVE_CONTEXT = None
ACTIVE_PAGE = None
ACTIVE_ACCOUNT_ID = ""
LEGACY_MODULE = None
MULTI_FLOW_JOB_ID = ""
MULTI_FLOW_STATE = {}


def log(message: str):
    print(f"[1X-LOCAL] {message}", flush=True)


def api_request(path: str, method: str = "GET", payload=None, query=None, use_header=True):
    url = f"{CRM_BASE_URL}{path}"
    query = dict(query or {})
    query.setdefault("api_key", AGENT_API_KEY)
    if query:
        url = f"{url}?{urllib.parse.urlencode(query)}"
    headers = {
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/134.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(request, timeout=API_TIMEOUT_SECONDS) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            pass
        raise RuntimeError(f"HTTP {exc.code} for {url}: {error_body or exc.reason}") from exc


def heartbeat(**updates):
    payload = {
        "agent_version": AGENT_VERSION,
        "connected": True,
    }
    payload.update(updates)
    return api_request("/api/onex-agent/heartbeat", method="POST", payload=payload)


def complete_job(**updates):
    payload = {
        "agent_version": AGENT_VERSION,
        "connected": True,
    }
    payload.update(updates)
    return api_request("/api/onex-agent/job/complete", method="POST", payload=payload)


def upload_server_session(account, storage_state_payload, cookies_payload):
    return api_request(
        "/api/onex-agent/upload-session",
        method="POST",
        payload={
            "account_id": safe_text(account.get("id")),
            "storage_state": storage_state_payload,
            "cookies": cookies_payload,
        },
    )


def load_legacy_module():
    global LEGACY_MODULE
    if LEGACY_MODULE is not None:
        return LEGACY_MODULE
    import dotenv.main

    def fake_find_dotenv(*args, **kwargs):
        return str(ROOT_DIR / ".env")

    dotenv.main.find_dotenv = fake_find_dotenv
    module_path = ROOT_DIR / "__pycache__" / "parser_1xbet_halfmonth.cpython-313.pyc"
    loader = importlib.machinery.SourcelessFileLoader("parser_1xbet_halfmonth_legacy_local", str(module_path))
    spec = importlib.util.spec_from_loader("parser_1xbet_halfmonth_legacy_local", loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    module.HEADLESS = False
    LEGACY_MODULE = module
    return module


def read_json(path: Path, default):
    try:
        if path.exists():
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
    except Exception:
        pass
    return default


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def load_autorun_state():
    payload = read_json(AUTORUN_STATE_PATH, default={})
    return payload if isinstance(payload, dict) else {}


def save_autorun_state(payload):
    write_json(AUTORUN_STATE_PATH, payload)


def maybe_queue_scheduled_run(accounts_map):
    return False


def ensure_playwright():
    global PLAYWRIGHT_MANAGER
    if PLAYWRIGHT_MANAGER is None:
        PLAYWRIGHT_MANAGER = sync_playwright().start()
    return PLAYWRIGHT_MANAGER


def profile_dir_for_account(account_id: str) -> Path:
    return SESSION_DIR / f"{safe_text(account_id)}_profile"


def storage_state_for_account(account_id: str) -> Path:
    return SESSION_DIR / f"{safe_text(account_id)}_storage_state.json"


def cookies_path_for_account(account_id: str) -> Path:
    return SESSION_DIR / f"{safe_text(account_id)}_cookies.json"


def close_active_context():
    global ACTIVE_CONTEXT, ACTIVE_PAGE, ACTIVE_ACCOUNT_ID
    try:
        if ACTIVE_CONTEXT:
            ACTIVE_CONTEXT.close()
    except Exception:
        pass
    ACTIVE_CONTEXT = None
    ACTIVE_PAGE = None
    ACTIVE_ACCOUNT_ID = ""


def ensure_context(account: dict):
    global ACTIVE_CONTEXT, ACTIVE_PAGE, ACTIVE_ACCOUNT_ID
    account_id = safe_text(account.get("id"))
    if ACTIVE_CONTEXT and ACTIVE_ACCOUNT_ID == account_id:
        return ACTIVE_CONTEXT, ACTIVE_PAGE
    close_active_context()
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    playwright = ensure_playwright()
    context = playwright.chromium.launch_persistent_context(
        str(profile_dir_for_account(account_id)),
        headless=False,
        accept_downloads=True,
        locale="ru-RU",
        ignore_https_errors=True,
    )
    context.set_default_timeout(90000)
    context.set_default_navigation_timeout(120000)
    page = context.pages[0] if context.pages else context.new_page()
    ACTIVE_CONTEXT = context
    ACTIVE_PAGE = page
    ACTIVE_ACCOUNT_ID = account_id
    return context, page


def reveal_local_window(page):
    try:
        page.bring_to_front()
    except Exception:
        pass


def build_headless_context(account: dict):
    playwright = ensure_playwright()
    browser = playwright.chromium.launch(headless=True)
    storage_state = storage_state_for_account(safe_text(account.get("id")))
    context_kwargs = {
        "accept_downloads": True,
        "locale": "ru-RU",
        "ignore_https_errors": True,
    }
    if storage_state.exists():
        context_kwargs["storage_state"] = str(storage_state)
    context = browser.new_context(**context_kwargs)
    context.set_default_timeout(90000)
    context.set_default_navigation_timeout(120000)
    page = context.new_page()
    return browser, context, page


def build_headless_persistent_context(account: dict):
    playwright = ensure_playwright()
    profile_dir = profile_dir_for_account(safe_text(account.get("id")))
    context = playwright.chromium.launch_persistent_context(
        str(profile_dir),
        headless=True,
        accept_downloads=True,
        locale="ru-RU",
        ignore_https_errors=True,
    )
    context.set_default_timeout(90000)
    context.set_default_navigation_timeout(120000)
    page = context.pages[0] if context.pages else context.new_page()
    return context, page


def first_working_locator(page, selectors):
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if locator.count():
                return locator
        except Exception:
            continue
    return None


def fill_field(locator, value: str):
    text = safe_text(value)
    locator.click(timeout=5000)
    try:
        locator.fill("", timeout=5000)
    except Exception:
        pass
    locator.fill(text, timeout=5000)
    current = safe_text(locator.input_value(timeout=3000))
    if current != text:
        try:
            locator.press("Meta+A", timeout=1000)
        except Exception:
            pass
        try:
            locator.press("Control+A", timeout=1000)
        except Exception:
            pass
        locator.type(text, delay=35, timeout=5000)
        current = safe_text(locator.input_value(timeout=3000))
    if current != text:
        try:
            locator.evaluate(
                """
                (el, val) => {
                    el.removeAttribute('readonly');
                    el.removeAttribute('disabled');
                    el.focus();
                    el.value = '';
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.value = val;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    el.dispatchEvent(new Event('blur', { bubbles: true }));
                }
                """,
                text,
            )
        except Exception:
            pass
        try:
            current = safe_text(locator.input_value(timeout=3000))
        except Exception:
            current = ""
    if current != text:
        raise RuntimeError("1x не дал заполнить поле формы авторизации.")


def click_login_button(page):
    button_selectors = [
        "button[type='submit']",
        "form button",
        "button:has-text('Sign in')",
        "button:has-text('Войти')",
        "button:has-text('Log in')",
        "[role='button']:has-text('Sign in')",
        "[role='button']:has-text('Войти')",
        "input[type='submit']",
    ]
    locator = first_working_locator(page, button_selectors)
    if not locator:
        raise RuntimeError("Не нашёл кнопку входа 1x.")
    try:
        locator.click(timeout=5000)
    except Exception:
        locator.press("Enter", timeout=3000)


def fill_login_fields(page, account):
    login_locator = first_working_locator(page, [
        "input[name='login']",
        "input[name='username']",
        "input[name='email']",
        "input[type='email']",
        "input[autocomplete='username']",
        "input[autocomplete='email']",
        "input[placeholder*='mail' i]",
        "input[placeholder*='логин' i]",
        "input[placeholder*='login' i]",
        "form input[type='text']",
    ])
    password_locator = first_working_locator(page, [
        "input[name='password']",
        "input[autocomplete='current-password']",
        "input[type='password']",
    ])
    if not login_locator or not password_locator:
        raise RuntimeError("Не нашёл поля логина или пароля на странице 1x.")
    fill_field(login_locator, safe_text(account.get("login")))
    fill_field(password_locator, safe_text(account.get("password")))
    return login_locator, password_locator


def submit_login(page, account):
    fill_login_fields(page, account)
    click_login_button(page)
    page.wait_for_timeout(2500)
    try:
        page.wait_for_load_state("domcontentloaded", timeout=10000)
    except Exception:
        pass
    if "/sign-in" in safe_text(page.url).lower():
        try:
            click_login_button(page)
            page.wait_for_timeout(1500)
        except Exception:
            pass


def open_login(account):
    context, page = ensure_context(account)
    reveal_local_window(page)
    page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
    try:
        submit_login(page, account)
        message = "Локальное окно 1x открыто. Логин и пароль заполнены автоматически. Если появилась captcha, пройди её и затем нажми I Passed Captcha в CRM."
    except Exception as exc:
        message = f"Локальное окно 1x открыто, но автологин не завершился: {safe_text(exc)}"
    heartbeat(
        status="waiting_for_user",
        message=message,
        current_account=safe_text(account.get("id")),
        current_account_label=safe_text(account.get("label")),
        job_status="idle",
        job_message="Окно 1x открыто локально.",
        profile_exists=profile_dir_for_account(account["id"]).exists(),
        session_saved=storage_state_for_account(account["id"]).exists(),
    )


def session_looks_authenticated(account):
    legacy = load_legacy_module()
    context, page = ensure_context(account)
    try:
        if legacy.is_authenticated(page):
            return True
    except Exception:
        pass
    current_url = safe_text(getattr(page, "url", "")).lower()
    if "/sign-in" in current_url or "/login" in current_url:
        return False
    try:
        cookies = context.cookies("https://1xpartners.com")
    except Exception:
        cookies = []
    return bool(cookies) and "1xpartners.com" in current_url


def verify_login(account):
    legacy = load_legacy_module()
    context, page = ensure_context(account)
    page.goto(PLAYERS_REPORT_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    dismiss_onex_blockers(page)
    if not legacy.is_authenticated(page):
        raise RuntimeError("Сессия 1x ещё не активна. Заверши captcha и вход в локальном окне.")
    storage_state = storage_state_for_account(account["id"])
    cookies_path = cookies_path_for_account(account["id"])
    context.storage_state(path=str(storage_state))
    cookies_payload = context.cookies()
    with open(cookies_path, "w", encoding="utf-8") as fh:
        json.dump(cookies_payload, fh, ensure_ascii=False, indent=2)
    storage_state_payload = read_json(storage_state, default={})
    if storage_state_payload:
        upload_server_session(account, storage_state_payload, cookies_payload)
    heartbeat(
        status="ready",
        message="Вход подтверждён. Локальная сессия 1x сохранена, можно запускать парсер.",
        current_account=safe_text(account.get("id")),
        current_account_label=safe_text(account.get("label")),
        session_saved=True,
        profile_exists=True,
        job_status="idle",
        job_message="",
        last_error="",
    )


def serialize_row(row, account):
    return {
        "account_id": safe_text(account.get("id")),
        "account_label": safe_text(account.get("label")) or safe_text(account.get("id")),
        "source_name": safe_text(getattr(row, "source_name", "")),
        "report_date": safe_text(getattr(row, "report_date", "")),
        "period_start": safe_text(getattr(row, "period_start", "")),
        "period_end": safe_text(getattr(row, "period_end", "")),
        "period_label": safe_text(getattr(row, "period_label", "")),
        "registration_date": safe_text(getattr(row, "registration_date", "")),
        "country": safe_text(getattr(row, "country", "")),
        "sub_id": safe_text(getattr(row, "sub_id", "")),
        "player_id": safe_text(getattr(row, "player_id", "")),
        "deposit_amount": float(getattr(row, "deposit_amount", 0) or 0),
        "bet_amount": float(getattr(row, "bet_amount", 0) or 0),
        "company_income": float(getattr(row, "company_income", 0) or 0),
        "cpa_amount": float(getattr(row, "cpa_amount", 0) or 0),
        "hold_time": safe_text(getattr(row, "hold_time", "")),
        "blocked": safe_text(getattr(row, "blocked", "")),
    }


def export_csv_for_period(account, export_period):
    legacy = load_legacy_module()
    context, page = ensure_context(account)
    return export_csv_for_period_with_page(legacy, context, page, export_period)


def export_csv_for_period_with_page(legacy, context, page, export_period):
    page.goto(PLAYERS_REPORT_URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(5000)
    dismiss_onex_blockers(page)
    if not legacy.is_authenticated(page):
        raise RuntimeError("Локальная сессия 1x не активна. Сначала заново пройди Open Login и captcha.")
    legacy.open_players_report(page)
    dismiss_onex_blockers(page)
    set_onex_date_range(page, export_period)
    legacy.check_new_players(page)
    legacy.click_generate_report(page)
    return Path(legacy.download_csv(page))


def try_background_login_export(account, export_plan, finalize_job=True):
    legacy = load_legacy_module()
    browser, context, page = build_headless_context(account)
    try:
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(2000)
        dismiss_onex_blockers(page)
        submit_login(page, account)
        page.wait_for_timeout(3500)
        dismiss_onex_blockers(page)
        if not legacy.is_authenticated(page):
            return False, 0
        storage_state = storage_state_for_account(account["id"])
        cookies_path = cookies_path_for_account(account["id"])
        context.storage_state(path=str(storage_state))
        cookies_payload = context.cookies()
        with open(cookies_path, "w", encoding="utf-8") as fh:
            json.dump(cookies_payload, fh, ensure_ascii=False, indent=2)
        storage_state_payload = read_json(storage_state, default={})
        if storage_state_payload:
            upload_server_session(account, storage_state_payload, cookies_payload)

        current_period = dict((export_plan or {}).get("current_period") or {})
        ranges = list((export_plan or {}).get("ranges") or [])
        if not current_period or not ranges:
            raise RuntimeError("CRM не передал актуальный план диапазонов для выгрузки.")
        combined_rows = []
        for export_period in ranges:
            heartbeat(
                status="running",
                message=f"Тихий логин прошёл, выгружаю диапазон {export_period['date_start']} - {export_period['date_end']}.",
                current_account=safe_text(account.get("id")),
                current_account_label=safe_text(account.get("label")),
                session_saved=True,
                profile_exists=profile_dir_for_account(account["id"]).exists(),
                job_status="running",
                job_message=f"Фоновый экспорт после тихого логина {export_period['date_start']} - {export_period['date_end']}.",
            )
            file_path = export_csv_for_period_with_page(legacy, context, page, export_period)
            ext = file_path.suffix.lower() or ".csv"
            df = read_partner_uploaded_dataframe(str(file_path), ext)
            rows = parse_partner_dataframe(
                df,
                source_name=build_partner_source_name(
                    safe_text(current_period.get("date_start")),
                    safe_text(current_period.get("date_end")),
                    prefix=f"1x_parser/{safe_text(account.get('label')) or safe_text(account.get('id'))}",
                ),
                cabinet_name=safe_text(account.get("label")) or safe_text(account.get("id")),
                partner_platform="1xbet",
                upload_period_data=export_period,
            )
            apply_onex_period_metadata(rows, current_period)
            log(
                f"Background login range {export_period['date_start']} -> {export_period['date_end']}: "
                f"csv_rows={len(df.index)} parsed_rows={len(rows)}"
            )
            combined_rows.extend(rows)
        rows_payload = upload_export_rows(account, current_period, combined_rows)
        if finalize_job:
            complete_job(
                status="ready",
                message=f"Кабинет вошёл без captcha и загрузил {len(rows_payload)} строк.",
                current_account=safe_text(account.get("id")),
                current_account_label=safe_text(account.get("label")),
                session_saved=True,
                profile_exists=profile_dir_for_account(account["id"]).exists(),
                last_error="",
            )
        return True, len(rows_payload)
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass


def upload_export_rows(account, current_period, combined_rows):
    current_period = dict(current_period or {})
    source_name = build_partner_source_name(
        safe_text(current_period.get("date_start")),
        safe_text(current_period.get("date_end")),
        prefix=f"1x_parser/{safe_text(account.get('label')) or safe_text(account.get('id'))}",
    )
    merged_rows = merge_partner_rows(combined_rows)
    rows_payload = [serialize_row(item, account) for item in merged_rows]
    api_request(
        "/api/onex-parser/import-rows",
        method="POST",
        payload={
            "account_id": safe_text(account.get("id")),
            "source_name": source_name,
            "period_label": safe_text(current_period.get("period_label")),
            "date_start": safe_text(current_period.get("date_start")),
            "date_end": safe_text(current_period.get("date_end")),
            "rows": rows_payload,
        },
    )
    return rows_payload


def run_export(account, export_plan, finalize_job=True):
    verify_login(account)
    current_period = dict((export_plan or {}).get("current_period") or {})
    ranges = list((export_plan or {}).get("ranges") or [])
    if not current_period or not ranges:
        raise RuntimeError("CRM не передал актуальный план диапазонов для выгрузки.")
    combined_rows = []
    for export_period in ranges:
        heartbeat(
            status="running",
            message=f"Выгружаю диапазон {export_period['date_start']} - {export_period['date_end']}.",
            current_account=safe_text(account.get("id")),
            current_account_label=safe_text(account.get("label")),
            session_saved=True,
            profile_exists=True,
            job_status="running",
            job_message=f"Локальный экспорт диапазона {export_period['date_start']} - {export_period['date_end']}.",
        )
        file_path = export_csv_for_period(account, export_period)
        ext = file_path.suffix.lower() or ".csv"
        df = read_partner_uploaded_dataframe(str(file_path), ext)
        rows = parse_partner_dataframe(
            df,
            source_name=build_partner_source_name(
                safe_text(current_period.get("date_start")),
                safe_text(current_period.get("date_end")),
                prefix=f"1x_parser/{safe_text(account.get('label')) or safe_text(account.get('id'))}",
            ),
            cabinet_name=safe_text(account.get("label")) or safe_text(account.get("id")),
            partner_platform="1xbet",
            upload_period_data=export_period,
        )
        apply_onex_period_metadata(rows, current_period)
        log(
            f"Range {export_period['date_start']} -> {export_period['date_end']}: "
            f"csv_rows={len(df.index)} parsed_rows={len(rows)}"
        )
        combined_rows.extend(rows)
    rows_payload = upload_export_rows(account, current_period, combined_rows)
    log(
        f"Final import for {safe_text(account.get('label')) or safe_text(account.get('id'))}: "
        f"combined_rows={len(combined_rows)} imported_rows={len(rows_payload)}"
    )
    if finalize_job:
        complete_job(
            status="ready",
            message=f"Локальный агент закончил выгрузку. Строк загружено: {len(rows_payload)}.",
            current_account=safe_text(account.get("id")),
            current_account_label=safe_text(account.get("label")),
            session_saved=True,
            profile_exists=True,
            last_error="",
        )
    close_active_context()
    return len(rows_payload)


def try_headless_export(account, export_plan, finalize_job=True):
    storage_state = storage_state_for_account(safe_text(account.get("id")))
    if not storage_state.exists():
        return False, 0
    legacy = load_legacy_module()
    browser, context, page = build_headless_context(account)
    try:
        page.goto(PLAYERS_REPORT_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        if not legacy.is_authenticated(page):
            return False, 0
        current_period = dict((export_plan or {}).get("current_period") or {})
        ranges = list((export_plan or {}).get("ranges") or [])
        if not current_period or not ranges:
            raise RuntimeError("CRM не передал актуальный план диапазонов для выгрузки.")
        combined_rows = []
        for export_period in ranges:
            heartbeat(
                status="running",
                message=f"Тихая выгрузка диапазона {export_period['date_start']} - {export_period['date_end']}.",
                current_account=safe_text(account.get("id")),
                current_account_label=safe_text(account.get("label")),
                session_saved=True,
                profile_exists=True,
                job_status="running",
                job_message=f"Локальный тихий экспорт диапазона {export_period['date_start']} - {export_period['date_end']}.",
            )
            file_path = export_csv_for_period_with_page(legacy, context, page, export_period)
            ext = file_path.suffix.lower() or ".csv"
            df = read_partner_uploaded_dataframe(str(file_path), ext)
            rows = parse_partner_dataframe(
                df,
                source_name=build_partner_source_name(
                    safe_text(current_period.get("date_start")),
                    safe_text(current_period.get("date_end")),
                    prefix=f"1x_parser/{safe_text(account.get('label')) or safe_text(account.get('id'))}",
                ),
                cabinet_name=safe_text(account.get("label")) or safe_text(account.get("id")),
                partner_platform="1xbet",
                upload_period_data=export_period,
            )
            apply_onex_period_metadata(rows, current_period)
            combined_rows.extend(rows)
        rows_payload = upload_export_rows(account, current_period, combined_rows)
        if finalize_job:
            complete_job(
                status="ready",
                message=f"Тихая выгрузка завершена. Строк загружено: {len(rows_payload)}.",
                current_account=safe_text(account.get("id")),
                current_account_label=safe_text(account.get("label")),
                session_saved=True,
                profile_exists=True,
                last_error="",
            )
        return True, len(rows_payload)
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass


def try_profile_export(account, export_plan, finalize_job=True):
    profile_dir = profile_dir_for_account(safe_text(account.get("id")))
    if not profile_dir.exists():
        return False, 0
    legacy = load_legacy_module()
    context, page = build_headless_persistent_context(account)
    try:
        page.goto(PLAYERS_REPORT_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        if not legacy.is_authenticated(page):
            return False, 0
        storage_state = storage_state_for_account(account["id"])
        cookies_path = cookies_path_for_account(account["id"])
        context.storage_state(path=str(storage_state))
        cookies_payload = context.cookies()
        with open(cookies_path, "w", encoding="utf-8") as fh:
            json.dump(cookies_payload, fh, ensure_ascii=False, indent=2)
        storage_state_payload = read_json(storage_state, default={})
        if storage_state_payload:
            upload_server_session(account, storage_state_payload, cookies_payload)
        current_period = dict((export_plan or {}).get("current_period") or {})
        ranges = list((export_plan or {}).get("ranges") or [])
        if not current_period or not ranges:
            raise RuntimeError("CRM не передал актуальный план диапазонов для выгрузки.")
        combined_rows = []
        for export_period in ranges:
            heartbeat(
                status="running",
                message=f"Тихая выгрузка из локального профиля {export_period['date_start']} - {export_period['date_end']}.",
                current_account=safe_text(account.get("id")),
                current_account_label=safe_text(account.get("label")),
                session_saved=True,
                profile_exists=True,
                job_status="running",
                job_message=f"Экспорт из локального профиля {export_period['date_start']} - {export_period['date_end']}.",
            )
            file_path = export_csv_for_period_with_page(legacy, context, page, export_period)
            ext = file_path.suffix.lower() or ".csv"
            df = read_partner_uploaded_dataframe(str(file_path), ext)
            rows = parse_partner_dataframe(
                df,
                source_name=build_partner_source_name(
                    safe_text(current_period.get("date_start")),
                    safe_text(current_period.get("date_end")),
                    prefix=f"1x_parser/{safe_text(account.get('label')) or safe_text(account.get('id'))}",
                ),
                cabinet_name=safe_text(account.get("label")) or safe_text(account.get("id")),
                partner_platform="1xbet",
                upload_period_data=export_period,
            )
            apply_onex_period_metadata(rows, current_period)
            combined_rows.extend(rows)
        rows_payload = upload_export_rows(account, current_period, combined_rows)
        if finalize_job:
            complete_job(
                status="ready",
                message=f"Выгрузка из локального профиля завершена. Строк загружено: {len(rows_payload)}.",
                current_account=safe_text(account.get("id")),
                current_account_label=safe_text(account.get("label")),
                session_saved=True,
                profile_exists=True,
                last_error="",
            )
        return True, len(rows_payload)
    finally:
        try:
            context.close()
        except Exception:
            pass


def run_full_flow(account, export_plan, finalize_job=True):
    account_id = safe_text(account.get("id"))
    success, count = try_headless_export(account, export_plan, finalize_job=finalize_job)
    if success:
        return "done", count
    success, count = try_profile_export(account, export_plan, finalize_job=finalize_job)
    if success:
        return "done", count
    success, count = try_background_login_export(account, export_plan, finalize_job=finalize_job)
    if success:
        return "done", count
    if ACTIVE_CONTEXT and ACTIVE_ACCOUNT_ID == account_id:
        if session_looks_authenticated(account):
            heartbeat(
                status="running",
                message="Captcha завершена, запускаю автоматическую выгрузку игроков.",
                current_account=account_id,
                current_account_label=safe_text(account.get("label")),
                session_saved=storage_state_for_account(account_id).exists(),
                profile_exists=profile_dir_for_account(account_id).exists(),
                job_status="running",
                job_message="Локальный агент увидел успешный вход и запускает экспорт.",
                last_error="",
            )
            count = run_export(account, export_plan, finalize_job=finalize_job)
            return "done", count
        heartbeat(
            status="waiting_for_user",
            message="Окно 1x уже открыто локально. Пройди captcha, после успешного входа агент сам продолжит выгрузку.",
            current_account=account_id,
            current_account_label=safe_text(account.get("label")),
            session_saved=storage_state_for_account(account_id).exists(),
            profile_exists=profile_dir_for_account(account_id).exists(),
            job_status="pending",
            job_message="Жду завершения captcha в локальном окне 1x.",
            last_error="",
        )
        return "waiting", 0
    open_login(account)
    if session_looks_authenticated(account):
        heartbeat(
            status="running",
            message="Вход прошёл без captcha, запускаю автоматическую выгрузку игроков.",
            current_account=account_id,
            current_account_label=safe_text(account.get("label")),
            session_saved=storage_state_for_account(account_id).exists(),
            profile_exists=profile_dir_for_account(account_id).exists(),
            job_status="running",
            job_message="Локальный агент вошёл без captcha и запускает экспорт.",
            last_error="",
        )
        count = run_export(account, export_plan, finalize_job=finalize_job)
        return "done", count
    heartbeat(
        status="waiting_for_user",
        message="Окно 1x открыто локально. Пройди captcha, после успешного входа агент сам сохранит сессию и загрузит статистику в CRM.",
        current_account=account_id,
        current_account_label=safe_text(account.get("label")),
        session_saved=storage_state_for_account(account_id).exists(),
        profile_exists=profile_dir_for_account(account_id).exists(),
        job_status="pending",
        job_message="Жду завершения captcha в локальном окне 1x.",
        last_error="",
    )
    return "waiting", 0


def clear_session(account):
    account_id = safe_text(account.get("id"))
    close_active_context()
    for path in [storage_state_for_account(account_id), cookies_path_for_account(account_id)]:
        if path.exists():
            path.unlink()
    profile_dir = profile_dir_for_account(account_id)
    if profile_dir.exists():
        shutil.rmtree(profile_dir)
    complete_job(
        status="idle",
        message="Локальный профиль и сессия 1x очищены.",
        current_account=account_id,
        current_account_label=safe_text(account.get("label")),
        session_saved=False,
        profile_exists=False,
        last_error="",
    )


def clear_multi_flow_state():
    global MULTI_FLOW_JOB_ID, MULTI_FLOW_STATE
    MULTI_FLOW_JOB_ID = ""
    MULTI_FLOW_STATE = {}


def handle_multi_flow(job, accounts_map):
    global MULTI_FLOW_JOB_ID, MULTI_FLOW_STATE
    job_id = safe_text(job.get("id"))
    payload = dict(job.get("payload") or {})
    account_ids = [safe_text(item) for item in (job.get("account_ids") or payload.get("account_ids") or []) if safe_text(item)]
    if MULTI_FLOW_JOB_ID != job_id:
        MULTI_FLOW_JOB_ID = job_id
        MULTI_FLOW_STATE = {
            "account_ids": account_ids,
            "index": 0,
            "results": [],
            "export_plan": dict(payload.get("export_plan") or payload),
        }
    state = MULTI_FLOW_STATE
    export_plan = dict(state.get("export_plan") or {})
    while state.get("index", 0) < len(state.get("account_ids") or []):
        current_account_id = safe_text(state["account_ids"][state["index"]])
        account = accounts_map.get(current_account_id)
        if not account:
            state["results"].append(f"{current_account_id}: missing account")
            state["index"] += 1
            continue
        log(
            f"Multi-flow step {state.get('index', 0) + 1}/{len(state.get('account_ids') or [])}: "
            f"{safe_text(account.get('label'))}"
        )
        status, count = run_full_flow(account, export_plan, finalize_job=False)
        if status == "waiting":
            heartbeat(
                status="waiting_for_user",
                message=f"Жду captcha для кабинета {safe_text(account.get('label'))}. После успешного входа агент сам продолжит остальные кабинеты.",
                current_account=current_account_id,
                current_account_label=safe_text(account.get("label")),
                session_saved=storage_state_for_account(current_account_id).exists(),
                profile_exists=profile_dir_for_account(current_account_id).exists(),
                job_status="pending",
                job_message=f"Multi-flow остановлен на кабинете {safe_text(account.get('label'))}.",
                last_error="",
            )
            return
        close_active_context()
        next_index = state.get("index", 0) + 1
        state["results"].append(f"{safe_text(account.get('label'))}: {count}")
        state["index"] = next_index
        if next_index < len(state.get("account_ids") or []):
            next_account_id = safe_text(state["account_ids"][next_index])
            next_account = accounts_map.get(next_account_id) or {}
            heartbeat(
                status="running",
                message=f"{safe_text(account.get('label'))} завершён. Перехожу к кабинету {safe_text(next_account.get('label')) or next_account_id}.",
                current_account=next_account_id,
                current_account_label=safe_text(next_account.get("label")),
                session_saved=storage_state_for_account(next_account_id).exists() if next_account_id else False,
                profile_exists=profile_dir_for_account(next_account_id).exists() if next_account_id else False,
                job_status="running",
                job_message=f"Multi-flow переходит к следующему кабинету: {safe_text(next_account.get('label')) or next_account_id}.",
                last_error="",
            )
    complete_job(
        status="ready",
        message="Multi-flow завершён. Данные по всем кабинетам загружены в 1x Parser.",
        current_account="__all__",
        current_account_label="All cabinets",
        session_saved=True,
        profile_exists=True,
        last_error="",
    )
    clear_multi_flow_state()


def handle_job(job, accounts_map):
    job_type = safe_text(job.get("type"))
    account = accounts_map.get(safe_text(job.get("account_id")))
    if job_type == "start_multi_flow":
        handle_multi_flow(job, accounts_map)
        return
    if not account:
        raise RuntimeError("CRM передал задачу без валидного аккаунта.")
    log(f"Handling job {job_type} for {account.get('label')}")
    if job_type == "open_login":
        open_login(account)
        complete_job(
            status="waiting_for_user",
            message="Локальное окно 1x открыто. Пройди captcha и нажми I Passed Captcha в CRM.",
            current_account=safe_text(account.get("id")),
            current_account_label=safe_text(account.get("label")),
            session_saved=storage_state_for_account(account["id"]).exists(),
            profile_exists=profile_dir_for_account(account["id"]).exists(),
            last_error="",
        )
        return
    if job_type == "start_full_flow":
        run_full_flow(account, job.get("payload") or {}, finalize_job=True)
        return
    if job_type == "check_auth":
        verify_login(account)
        complete_job(
            status="ready",
            message="Локальная сессия подтверждена. Можно запускать парсер.",
            current_account=safe_text(account.get("id")),
            current_account_label=safe_text(account.get("label")),
            session_saved=True,
            profile_exists=True,
            last_error="",
        )
        close_active_context()
        return
    if job_type == "run_export":
        run_export(account, job.get("payload") or {})
        return
    if job_type == "clear_session":
        clear_session(account)
        return
    complete_job(
        status="idle",
        message=f"Локальный агент пропустил неизвестную задачу {job_type}.",
        current_account=safe_text(account.get("id")),
        current_account_label=safe_text(account.get("label")),
        last_error="",
    )


def main():
    log(f"Connecting local 1x agent to {CRM_BASE_URL}")
    while True:
        job = {}
        try:
            response = api_request("/api/onex-agent/job")
            job = response.get("job") or {}
            remote_state = response.get("state") or {}
            accounts = response.get("accounts") or []
            accounts_map = {safe_text(item.get("id")): item for item in accounts if isinstance(item, dict)}
            if remote_state.get("paused"):
                close_active_context()
                heartbeat(
                    status="idle",
                    message="Парсер на паузе.",
                    paused=True,
                    current_account="",
                    current_account_label="",
                    session_saved=False,
                    profile_exists=False,
                    job_status="idle",
                    job_message="",
                    last_error="",
                )
                time.sleep(POLL_INTERVAL)
                continue
            if job.get("status") == "pending":
                handle_job(job, accounts_map)
            else:
                maybe_queue_scheduled_run(accounts_map)
                heartbeat(
                    status="idle" if not ACTIVE_CONTEXT else "waiting_for_user",
                    message="Локальный агент подключён и ждёт задачу." if not ACTIVE_CONTEXT else "Локальное окно 1x открыто и ждёт пользователя.",
                    paused=False,
                    current_account=safe_text(ACTIVE_ACCOUNT_ID),
                    current_account_label=safe_text((accounts_map.get(ACTIVE_ACCOUNT_ID) or {}).get("label")),
                    session_saved=storage_state_for_account(ACTIVE_ACCOUNT_ID).exists() if ACTIVE_ACCOUNT_ID else False,
                    profile_exists=profile_dir_for_account(ACTIVE_ACCOUNT_ID).exists() if ACTIVE_ACCOUNT_ID else False,
                    job_status="idle",
                    job_message="",
                    last_error="",
                )
        except Exception as exc:
            log(f"Request failed: {exc}")
            close_active_context()
            if safe_text(job.get("status")) == "pending":
                try:
                    complete_job(
                        status="error",
                        message=f"Локальная задача остановлена после ошибки: {safe_text(exc)}",
                        current_account=safe_text(job.get("account_id")),
                        current_account_label=safe_text(job.get("account_label")),
                        last_error=safe_text(exc),
                    )
                except Exception:
                    pass
                clear_multi_flow_state()
            try:
                heartbeat(
                    status="error",
                    message=f"Локальный агент поймал ошибку: {safe_text(exc)}",
                    last_error=safe_text(exc),
                    job_status="idle",
                    job_message="",
                    session_saved=storage_state_for_account(ACTIVE_ACCOUNT_ID).exists() if ACTIVE_ACCOUNT_ID else False,
                    profile_exists=profile_dir_for_account(ACTIVE_ACCOUNT_ID).exists() if ACTIVE_ACCOUNT_ID else False,
                )
            except Exception:
                pass
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
