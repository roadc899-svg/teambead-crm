from fastapi import FastAPI, UploadFile, File, Query, Form, Request, HTTPException, Header
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse, Response, FileResponse, JSONResponse
from fastapi.exception_handlers import http_exception_handler
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
import shutil
import uuid
import os
import json
from urllib.parse import urlencode
from html import escape
import io
import csv
import secrets
import hashlib
import calendar
import re
import subprocess
import sys
import signal
from datetime import datetime, timedelta, date

# =========================================
# BLOCK 1 — DATABASE
# =========================================
DATABASE_URL = "sqlite:///./test.db"
SESSION_COOKIE_NAME = "teambead_session"
SESSION_DURATION_DAYS = 14
DATA_UPLOAD_DIR = "./uploaded_data"
STATIC_DIR = "./static"
FINANCE_UPLOAD_PATH = os.path.join(DATA_UPLOAD_DIR, "finance_latest.csv")
PARTNER_UPLOAD_DIR = os.path.join(DATA_UPLOAD_DIR, "partner_reports")
PARTNER_IMPORT_API_KEY = os.getenv("TEAMBEAD_PARTNER_IMPORT_KEY", "8hF9sK2LmQpX91zA")
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
ONEXBET_PARSER_PATH = os.path.join(PROJECT_ROOT, "parser_1xbet_halfmonth.py")
ONEXBET_RUNTIME_DIR = os.path.join(DATA_UPLOAD_DIR, "onexbet_runtime")
ONEXBET_ACCOUNTS_PATH = os.path.join(ONEXBET_RUNTIME_DIR, "accounts.json")
ONEXBET_ACCOUNTS_JSON = os.getenv("ONEXBET_ACCOUNTS_JSON", "").strip()
ONEXBET_STATUS_PATH = os.path.join(ONEXBET_RUNTIME_DIR, "status.json")
ONEXBET_LOG_PATH = os.path.join(ONEXBET_RUNTIME_DIR, "parser.log")
ONEXBET_HISTORY_PATH = os.path.join(ONEXBET_RUNTIME_DIR, "history.json")
ONEXBET_SESSION_PATH = os.path.join(PROJECT_ROOT, "playwright_state_1xbet.json")
DEFAULT_USERS = [
    {
        "username": os.getenv("TEAMBEAD_ADMIN1_LOGIN", "Ivan"),
        "password": os.getenv("TEAMBEAD_ADMIN1_PASSWORD", "12345"),
        "role": "superadmin",
        "display_name": os.getenv("TEAMBEAD_ADMIN1_NAME", "Ivan"),
        "legacy_usernames": ["admin1"],
    },
    {
        "username": os.getenv("TEAMBEAD_ADMIN2_LOGIN", "Dmytro"),
        "password": os.getenv("TEAMBEAD_ADMIN2_PASSWORD", "12345"),
        "role": "admin",
        "display_name": os.getenv("TEAMBEAD_ADMIN2_NAME", "Dmytro"),
        "legacy_usernames": ["admin2"],
    },
]

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


def ensure_onexbet_runtime_dir():
    os.makedirs(ONEXBET_RUNTIME_DIR, exist_ok=True)


def onexbet_slugify(value):
    return re.sub(r"[^a-zA-Z0-9]+", "_", (value or "").strip().lower()).strip("_") or "account"


def normalize_onexbet_accounts(raw):
    accounts = []
    if isinstance(raw, dict):
        raw = raw.get("accounts", [])
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            login = safe_text(item.get("login") or item.get("username"))
            password = safe_text(item.get("password"))
            if not login or not password:
                continue
            account_id = safe_text(item.get("id")) or onexbet_slugify(login)
            accounts.append({
                "id": account_id,
                "label": safe_text(item.get("label")) or login,
                "login": login,
            })
    return accounts


def load_onexbet_accounts():
    ensure_onexbet_runtime_dir()
    if ONEXBET_ACCOUNTS_JSON:
        try:
            return normalize_onexbet_accounts(json.loads(ONEXBET_ACCOUNTS_JSON))
        except Exception:
            return []

    if os.path.exists(ONEXBET_ACCOUNTS_PATH):
        try:
            with open(ONEXBET_ACCOUNTS_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            return normalize_onexbet_accounts(raw)
        except Exception:
            return []
    return []


def session_path_for_onexbet_account(account_id):
    return os.path.join(ONEXBET_RUNTIME_DIR, "sessions", f"{account_id}_state.json")


def get_onexbet_accounts_with_state():
    result = []
    for item in load_onexbet_accounts():
        session_path = session_path_for_onexbet_account(item["id"])
        result.append({
            **item,
            "session_saved": os.path.exists(session_path),
            "session_path": session_path,
        })
    return result


def write_onexbet_status(payload):
    ensure_onexbet_runtime_dir()
    with open(ONEXBET_STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def read_onexbet_status():
    if not os.path.exists(ONEXBET_STATUS_PATH):
        return {}
    try:
        with open(ONEXBET_STATUS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def read_onexbet_history():
    if not os.path.exists(ONEXBET_HISTORY_PATH):
        return []
    try:
        with open(ONEXBET_HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def write_onexbet_history(items):
    ensure_onexbet_runtime_dir()
    with open(ONEXBET_HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(items[:40], f, ensure_ascii=False, indent=2)


def upsert_onexbet_history_from_status(status):
    run_id = safe_text(status.get("run_id"))
    if not run_id:
        return
    history = read_onexbet_history()
    entry = {
        "run_id": run_id,
        "status": status.get("status"),
        "mode": status.get("mode"),
        "started_at": status.get("started_at"),
        "finished_at": status.get("finished_at"),
        "launched_by": status.get("launched_by"),
        "launched_by_username": status.get("launched_by_username"),
        "selected_account_ids": status.get("selected_account_ids") or [],
        "message": status.get("message"),
        "error": status.get("error"),
    }
    updated = False
    for idx, item in enumerate(history):
        if safe_text(item.get("run_id")) == run_id:
            history[idx] = {**item, **entry}
            updated = True
            break
    if not updated:
        history.insert(0, entry)
    write_onexbet_history(history)


def tail_onexbet_log(max_lines=120):
    if not os.path.exists(ONEXBET_LOG_PATH):
        return ""
    try:
        with open(ONEXBET_LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except Exception:
        return ""
    return "".join(lines[-max_lines:])


def onexbet_pid_running(pid):
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def get_onexbet_parser_state():
    status = read_onexbet_status()
    running = onexbet_pid_running(status.get("pid"))
    accounts = get_onexbet_accounts_with_state()

    if status.get("status") in {"starting", "running", "waiting_for_user"} and not running:
        status["status"] = "stopped"
        status["message"] = status.get("message") or "Процесс парсера уже завершился"

    status.setdefault("status", "idle")
    status.setdefault("message", "Парсер не запущен")
    status.setdefault("mode", "")
    status["running"] = running
    status["session_saved"] = any(item.get("session_saved") for item in accounts)
    status["accounts"] = accounts
    status["log_tail"] = tail_onexbet_log()
    status["history"] = read_onexbet_history()[:12]
    upsert_onexbet_history_from_status(status)
    return status


def start_onexbet_parser_job(mode, account_ids=None, launched_by=None):
    if not os.path.exists(ONEXBET_PARSER_PATH):
        raise HTTPException(status_code=404, detail="Файл parser_1xbet_halfmonth.py не найден")

    account_ids = [safe_text(item) for item in (account_ids or []) if safe_text(item)]
    available_ids = {item["id"] for item in load_onexbet_accounts()}
    if not available_ids:
        raise HTTPException(status_code=400, detail="Список кабинетов 1xBet пуст. Добавь accounts.json.")
    if not account_ids:
        account_ids = sorted(available_ids)
    invalid_ids = [item for item in account_ids if item not in available_ids]
    if invalid_ids:
        raise HTTPException(status_code=400, detail=f"Неизвестные кабинеты: {', '.join(invalid_ids)}")

    current = get_onexbet_parser_state()
    if current.get("running"):
        raise HTTPException(status_code=409, detail="Парсер уже запущен. Дождись завершения текущего процесса.")

    ensure_onexbet_runtime_dir()
    with open(ONEXBET_LOG_PATH, "w", encoding="utf-8") as f:
        f.write("")

    initial_status = {
        "run_id": uuid.uuid4().hex,
        "status": "starting",
        "mode": mode,
        "message": "Запускаю процесс парсера...",
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "finished_at": None,
        "pid": None,
        "needs_user_action": False,
        "session_saved": any(item.get("session_saved") for item in get_onexbet_accounts_with_state()),
        "selected_account_ids": account_ids,
        "launched_by": (launched_by or {}).get("display_name", ""),
        "launched_by_username": (launched_by or {}).get("username", ""),
        "account_statuses": {account_id: {"status": "queued", "message": "Ожидает запуска"} for account_id in account_ids},
    }
    write_onexbet_status(initial_status)
    upsert_onexbet_history_from_status(initial_status)

    env = os.environ.copy()
    env["PARSER_STATUS_FILE"] = ONEXBET_STATUS_PATH
    env["SESSION_STATE_PATH"] = os.path.join(ONEXBET_RUNTIME_DIR, "sessions", "default_state.json")
    env["ONEXBET_ACCOUNTS_FILE"] = ONEXBET_ACCOUNTS_PATH
    env["ONEXBET_ACCOUNT_IDS"] = ",".join(account_ids)
    env["PARSER_MODE"] = mode
    env["PYTHONUNBUFFERED"] = "1"
    if mode == "auth":
        env["HEADLESS"] = "false"

    with open(ONEXBET_LOG_PATH, "a", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            [sys.executable, ONEXBET_PARSER_PATH, "--mode", mode, "--accounts", ",".join(account_ids)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=PROJECT_ROOT,
            env=env,
            start_new_session=True,
        )

    initial_status["pid"] = process.pid
    initial_status["message"] = "Процесс запущен"
    write_onexbet_status(initial_status)
    upsert_onexbet_history_from_status(initial_status)
    return get_onexbet_parser_state()


def reset_onexbet_sessions(account_ids=None):
    account_ids = [safe_text(item) for item in (account_ids or []) if safe_text(item)]
    available_accounts = get_onexbet_accounts_with_state()
    available_ids = {item["id"] for item in available_accounts}
    if not account_ids:
        account_ids = sorted(available_ids)

    removed = []
    for account_id in account_ids:
        if account_id not in available_ids:
            continue
        session_path = session_path_for_onexbet_account(account_id)
        if os.path.exists(session_path):
            os.remove(session_path)
            removed.append(account_id)

    status = get_onexbet_parser_state()
    status["message"] = "Сессии обновлены"
    write_onexbet_status(status)
    return {"removed": removed, "state": get_onexbet_parser_state()}


def stop_onexbet_parser_job():
    status = read_onexbet_status()
    pid = safe_number(status.get("pid"))
    if not pid:
        raise HTTPException(status_code=404, detail="Активный процесс парсера не найден")
    try:
        os.killpg(int(pid), signal.SIGTERM)
    except ProcessLookupError:
        pass
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Не удалось остановить процесс: {exc}")

    status["status"] = "stopped"
    status["message"] = "Парсер остановлен вручную"
    status["finished_at"] = datetime.now().isoformat(timespec="seconds")
    write_onexbet_status(status)
    upsert_onexbet_history_from_status(status)
    return get_onexbet_parser_state()


# =========================================
# BLOCK 2 — MODEL
# =========================================
class FBRow(Base):
    __tablename__ = "fb_rows"

    id = Column(Integer, primary_key=True, index=True)
    uploader = Column(String)  # internally оставляем старое имя поля для совместимости с БД
    ad_name = Column(String)

    launch_date = Column(String)
    platform = Column(String)
    manager = Column(String)
    geo = Column(String)
    offer = Column(String)
    creative = Column(String)

    leads = Column(Float)
    reg = Column(Float)
    ftd = Column(Float)
    clicks = Column(Float)
    spend = Column(Float)
    cpc = Column(Float)
    ctr = Column(Float)

    date_start = Column(String)
    date_end = Column(String)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    password_hash = Column(String, nullable=False)
    role = Column(String, default="buyer", nullable=False)
    display_name = Column(String, default="")
    buyer_name = Column(String, default="")
    is_active = Column(Integer, default=1)


class UserSession(Base):
    __tablename__ = "user_sessions"

    id = Column(Integer, primary_key=True, index=True)
    token = Column(String, unique=True, index=True, nullable=False)
    username = Column(String, index=True, nullable=False)
    expires_at = Column(DateTime, nullable=False)


class CapRow(Base):
    __tablename__ = "cap_rows"

    id = Column(Integer, primary_key=True, index=True)
    advertiser = Column(String, default="")
    owner_name = Column(String, default="")
    buyer = Column(String, default="")
    flow = Column(String, default="")
    code = Column(String, default="")
    geo = Column(String, default="")
    rate = Column(String, default="")
    baseline = Column(String, default="")
    cap_value = Column(Float, default=0)
    promo_code = Column(String, default="")
    kpi = Column(String, default="")
    link = Column(String, default="")
    comments = Column(String, default="")
    agent = Column(String, default="")
    current_ftd = Column(Float, default=0)


class TaskRow(Base):
    __tablename__ = "task_rows"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, default="")
    description = Column(String, default="")
    assigned_to_username = Column(String, index=True, default="")
    assigned_to_name = Column(String, default="")
    assigned_to_role = Column(String, default="")
    created_by_username = Column(String, default="")
    created_by_name = Column(String, default="")
    status = Column(String, default="Не начато")
    due_at = Column(DateTime, nullable=True)
    response_text = Column(String, default="")
    notes = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    answered_at = Column(DateTime, nullable=True)


class FinanceWalletRow(Base):
    __tablename__ = "finance_wallet_rows"

    id = Column(Integer, primary_key=True, index=True)
    category = Column(String, default="")
    description = Column(String, default="")
    owner_name = Column(String, default="")
    wallet = Column(String, default="")
    amount = Column(Float, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


class FinanceExpenseRow(Base):
    __tablename__ = "finance_expense_rows"

    id = Column(Integer, primary_key=True, index=True)
    expense_date = Column(String, default="")
    category = Column(String, default="")
    amount = Column(Float, default=0)
    paid_by = Column(String, default="")
    comment = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class FinanceIncomeRow(Base):
    __tablename__ = "finance_income_rows"

    id = Column(Integer, primary_key=True, index=True)
    income_date = Column(String, default="")
    category = Column(String, default="")
    description = Column(String, default="")
    amount = Column(Float, default=0)
    wallet = Column(String, default="")
    reconciliation = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class PartnerRow(Base):
    __tablename__ = "partner_rows"

    id = Column(Integer, primary_key=True, index=True)
    source_name = Column(String, default="")
    sub_id = Column(String, index=True, default="")
    player_id = Column(String, default="")
    registration_date = Column(String, default="")
    country = Column(String, default="")
    deposit_amount = Column(Float, default=0)
    bet_amount = Column(Float, default=0)
    company_income = Column(Float, default=0)
    cpa_amount = Column(Float, default=0)
    hold_time = Column(String, default="")
    blocked = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class ChatterfyRow(Base):
    __tablename__ = "chatterfy_rows"

    id = Column(Integer, primary_key=True, index=True)
    source_name = Column(String, default="")
    name = Column(String, default="")
    telegram_id = Column(String, default="")
    username = Column(String, default="")
    tags = Column(String, default="")
    started = Column(String, default="")
    last_user_message = Column(String, default="")
    last_bot_message = Column(String, default="")
    status = Column(String, default="")
    step = Column(String, default="")
    external_id = Column(String, default="")
    launch_date = Column(String, default="")
    platform = Column(String, default="")
    manager = Column(String, default="")
    geo = Column(String, default="")
    offer = Column(String, default="")
    flow_platform = Column(String, default="")
    flow_manager = Column(String, default="")
    flow_geo = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class ChatterfyIdRow(Base):
    __tablename__ = "chatterfy_id_rows"

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String, index=True, default="")
    pp_player_id = Column(String, index=True, default="")
    chat_link = Column(String, default="")
    source_date = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


Base.metadata.create_all(bind=engine)


# =========================================
# BLOCK 3 — APP
# =========================================
app = FastAPI(title="TEAMbead CRM")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/favicon.jpg", include_in_schema=False)
def favicon_jpg():
    candidates = [
        os.path.join(STATIC_DIR, "favicon.jpg"),
        "./favicon.jpg",
    ]
    for path in candidates:
        if os.path.exists(path):
            return FileResponse(path, media_type="image/jpeg")
    raise HTTPException(status_code=404)


@app.get("/favicon.ico", include_in_schema=False)
def favicon_ico():
    candidates = [
        os.path.join(STATIC_DIR, "favicon.jpg"),
        "./favicon.jpg",
    ]
    for path in candidates:
        if os.path.exists(path):
            return FileResponse(path, media_type="image/jpeg")
    raise HTTPException(status_code=404)


@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    if exc.status_code == 401:
        return auth_redirect_response()
    if exc.status_code == 403:
        user = get_current_user(request)
        content = """
        <div class="empty-dev">
            <div class="empty-dev-card">
                <div class="big">Доступ запрещен</div>
                <div class="muted">Для этой страницы или функции у вашей роли сейчас нет прав.</div>
            </div>
        </div>
        """
        return HTMLResponse(page_shell("Access denied", content, current_user=user), status_code=403)
    return await http_exception_handler(request, exc)


# =========================================
# BLOCK 3.1 — AUTH HELPERS
# =========================================
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120000)
    return f"{salt}${digest.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        salt, expected = stored_hash.split("$", 1)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120000).hex()
        return secrets.compare_digest(digest, expected)
    except Exception:
        return False


def ensure_default_users():
    db = SessionLocal()
    try:
        for item in DEFAULT_USERS:
            username = (item.get("username") or "").strip()
            password = item.get("password") or ""
            legacy_usernames = [value.strip() for value in item.get("legacy_usernames", []) if (value or "").strip()]
            if not username or not password:
                continue
            existing = db.query(User).filter(User.username == username).first()
            if not existing:
                for legacy_username in legacy_usernames:
                    existing = db.query(User).filter(User.username == legacy_username).first()
                    if existing:
                        old_username = existing.username
                        existing.username = username
                        db.query(UserSession).filter(UserSession.username == old_username).update({"username": username})
                        break
            if existing:
                existing.display_name = item.get("display_name") or username
                existing.role = item.get("role") or "admin"
                existing.password_hash = hash_password(password)
                existing.is_active = 1
                db.add(existing)
                continue
            db.add(User(
                username=username,
                password_hash=hash_password(password),
                role=item.get("role") or "admin",
                display_name=item.get("display_name") or username,
                is_active=1,
            ))
        db.commit()
    finally:
        db.close()


def create_user_session(username: str) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(days=SESSION_DURATION_DAYS)
    db = SessionLocal()
    try:
        db.add(UserSession(token=token, username=username, expires_at=expires_at))
        db.commit()
        return token
    finally:
        db.close()


def delete_user_session(token: str):
    if not token:
        return
    db = SessionLocal()
    try:
        db.query(UserSession).filter(UserSession.token == token).delete()
        db.commit()
    finally:
        db.close()


def get_current_user(request: Request):
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None
    db = SessionLocal()
    try:
        session = db.query(UserSession).filter(UserSession.token == token).first()
        if not session:
            return None
        if session.expires_at <= datetime.utcnow():
            db.delete(session)
            db.commit()
            return None
        user = db.query(User).filter(User.username == session.username, User.is_active == 1).first()
        if not user:
            return None
        return {
            "id": user.id,
            "username": user.username,
            "display_name": user.display_name or user.username,
            "role": user.role or "buyer",
            "buyer_name": user.buyer_name or "",
        }
    finally:
        db.close()


def require_login(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401)
    return user


def require_any_role(user, *roles):
    if roles and user.get("role") not in roles:
        raise HTTPException(status_code=403)


def is_admin_role(user) -> bool:
    return (user or {}).get("role") in {"superadmin", "admin"}


def can_access_page(user, page_key: str) -> bool:
    role = (user or {}).get("role")
    page_rules = {
        "grouped": {"superadmin", "admin", "buyer", "operator"},
        "hierarchy": {"superadmin", "admin", "buyer", "operator"},
        "tasks": {"superadmin", "admin", "buyer", "operator", "finance"},
        "users": {"superadmin", "admin"},
        "finance": {"superadmin"},
        "caps": {"superadmin", "admin"},
        "chatterfy": {"superadmin", "admin"},
        "holdwager": {"superadmin", "admin"},
        "onexbet_report": {"superadmin", "admin"},
    }
    return role in page_rules.get(page_key, set())


def enforce_page_access(user, page_key: str):
    if not can_access_page(user, page_key):
        raise HTTPException(status_code=403)


def display_user_id(value) -> str:
    try:
        return str(1000 + int(value))
    except Exception:
        return "1000"


def role_label(role: str) -> str:
    mapping = {
        "superadmin": "Founder",
        "admin": "Admin",
        "buyer": "Buyer",
        "operator": "Operator",
        "finance": "Finance",
    }
    return mapping.get((role or "").strip(), role or "User")


def resolve_effective_buyer(user, buyer: str = "") -> str:
    if (user or {}).get("role") == "buyer":
        return ((user or {}).get("buyer_name") or "").strip()
    return (buyer or "").strip()


def get_scoped_filter_options(user):
    buyer_scope = resolve_effective_buyer(user)
    rows = get_filtered_data(buyer=buyer_scope)
    return (
        sorted({r.uploader for r in rows if r.uploader}),
        sorted({r.manager for r in rows if r.manager}),
        sorted({r.geo for r in rows if r.geo}),
        sorted({r.offer for r in rows if r.offer}),
    )


def auth_redirect_response(url: str = "/login"):
    return RedirectResponse(url=url, status_code=302)


def login_page_html(error_text: str = ""):
    error_html = f'<div class="login-error">{escape(error_text)}</div>' if error_text else ''
    return f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <link rel="icon" type="image/jpeg" href="/favicon.jpg?v=2">
        <link rel="shortcut icon" href="/favicon.ico?v=2">
        <title>TEAMbead CRM — Login</title>
        <style>
            :root {{
                --bg: #07101f; --panel:#0d1729; --panel-2:#111f35; --text:#ebf2ff; --muted:#8ca3c7;
                --border:#1f3150; --accent1:#38bdf8; --accent2:#2563eb; --accent3:#22c55e; --shadow:0 18px 40px rgba(0,0,0,.28);
            }}
            * {{ box-sizing:border-box; }}
            body {{ margin:0; min-height:100vh; display:grid; place-items:center; padding:24px; color:var(--text);
                background: radial-gradient(circle at top right, rgba(56,189,248,.14), transparent 24%), var(--bg);
                font-family: "Avenir Next", "Nunito", "Trebuchet MS", "Segoe UI", Arial, sans-serif; }}
            .login-shell {{ width:min(100%, 460px); }}
            .card {{ background:linear-gradient(180deg,var(--panel),var(--panel-2)); border:1px solid var(--border); border-radius:26px; box-shadow:var(--shadow); padding:28px; }}
            .brand {{ display:flex; align-items:center; justify-content:center; gap:12px; font-weight:900; font-size:28px; margin-bottom:18px; }}
            .brand-mark {{ width:18px; height:18px; border-radius:999px; background:linear-gradient(135deg,var(--accent1),var(--accent2),var(--accent3)); box-shadow:0 0 0 5px rgba(56,189,248,.14); }}
            .title {{ font-size:24px; font-weight:900; margin-bottom:8px; text-align:center; }}
            form {{ display:grid; gap:14px; margin-top:14px; }}
            label {{ display:grid; gap:7px; font-size:13px; font-weight:800; }}
            input {{ border-radius:14px; border:1px solid var(--border); background:#16243c; color:var(--text); padding:13px 14px; font-size:15px; outline:none; }}
            button {{ border:1px solid var(--border); background:linear-gradient(90deg,var(--accent2),var(--accent1)); color:white; padding:13px 16px; border-radius:14px; font-weight:900; cursor:pointer; }}
            .login-error {{ margin-top:12px; padding:12px 14px; border-radius:14px; background:rgba(239,68,68,.14); border:1px solid rgba(239,68,68,.28); }}
            @media (max-width: 860px) {{ .brand{{font-size:22px;}} }}
        </style>
    </head>
    <body>
        <div class="login-shell">
            <div class="card">
                <div class="brand"><span class="brand-mark"></span><span>TEAMbead CRM</span></div>
                <div class="title">Login</div>
                {error_html}
                <form method="post" action="/login">
                    <label>Login<input type="text" name="username" autocomplete="username" required></label>
                    <label>Password<input type="password" name="password" autocomplete="current-password" required></label>
                    <button type="submit">Войти</button>
                </form>
            </div>
        </div>
    </body>
    </html>
    """


ensure_default_users()


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if get_current_user(request):
        return RedirectResponse(url="/grouped", status_code=302)
    return HTMLResponse(login_page_html())


@app.post("/login")
def login_submit(username: str = Form(...), password: str = Form(...)):
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.username == username.strip(), User.is_active == 1).first()
        if not user or not verify_password(password, user.password_hash):
            return HTMLResponse(login_page_html("Неверный логин или пароль"), status_code=401)
    finally:
        db.close()

    token = create_user_session(username.strip())
    response = RedirectResponse(url="/grouped", status_code=302)
    response.set_cookie(SESSION_COOKIE_NAME, token, httponly=True, samesite="lax", max_age=SESSION_DURATION_DAYS * 24 * 60 * 60)
    return response


@app.get("/logout")
def logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE_NAME)
    delete_user_session(token)
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


@app.api_route("/", methods=["GET", "HEAD"])
def home(request: Request):
    if request.method == "HEAD":
        return Response(status_code=200)
    return RedirectResponse(url="/grouped" if get_current_user(request) else "/login", status_code=302)


# =========================================
# BLOCK 4 — HELPERS
# =========================================
def safe_number(value):
    if value is None:
        return 0.0
    try:
        if pd.isna(value):
            return 0.0
        if isinstance(value, str):
            value = value.replace(" ", "").replace("$", "").replace("%", "")
            value = value.replace(",", ".")
        return float(value)
    except Exception:
        return 0.0



def format_int_or_float(value):
    if value is None:
        return "0"
    try:
        value = float(value)
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)



def format_money(value):
    try:
        return f"${float(value or 0):,.2f}"
    except Exception:
        return "$0.00"



def format_percent(value):
    try:
        return f"{float(value or 0):.2f}%"
    except Exception:
        return "0.00%"



def parse_ad_name(ad_name):
    if not ad_name:
        return {
            "launch_date": "",
            "platform": "",
            "manager": "",
            "geo": "",
            "offer": "",
            "creative": "",
        }

    parts = str(ad_name).split("/")
    return {
        "launch_date": parts[0].strip() if len(parts) > 0 else "",
        "platform": parts[1].strip() if len(parts) > 1 else "",
        "manager": parts[2].strip() if len(parts) > 2 else "",
        "geo": parts[3].strip() if len(parts) > 3 else "",
        "offer": parts[4].strip() if len(parts) > 4 else "",
        "creative": parts[5].strip() if len(parts) > 5 else "",
    }



def calc_metrics(clicks, reg, ftd, spend, leads=0):
    clicks = clicks or 0
    reg = reg or 0
    ftd = ftd or 0
    spend = spend or 0
    leads = leads or 0

    return {
        "cpc_real": spend / clicks if clicks > 0 else 0,
        "cpl_real": spend / leads if leads > 0 else 0,
        "cpa_real": spend / ftd if ftd > 0 else 0,
        "cr_reg": (reg / clicks) * 100 if clicks > 0 else 0,
        "cr_ftd": (ftd / clicks) * 100 if clicks > 0 else 0,
        "l2ftd": (ftd / leads) * 100 if leads > 0 else 0,
        "r2d": (ftd / reg) * 100 if reg > 0 else 0,
    }



def build_query_string(**kwargs):
    clean = {k: v for k, v in kwargs.items() if v not in [None, ""]}
    return urlencode(clean)



def build_ad_offer_key(launch_date="", platform="", manager="", geo="", offer=""):
    return (
        safe_text(launch_date),
        safe_text(platform).lower(),
        safe_text(manager).lower(),
        normalize_geo_value(geo),
        safe_text(offer).lower(),
    )


def build_flow_key(platform="", manager="", geo=""):
    return (
        safe_text(platform).lower(),
        safe_text(manager).lower(),
        normalize_geo_value(geo),
    )


def make_options(options, selected_value):
    html = '<option value="">Все</option>'
    for option in options:
        option_text = escape(str(option))
        selected = "selected" if str(option) == str(selected_value) else ""
        html += f'<option value="{option_text}" {selected}>{option_text}</option>'
    return html



def parse_uploaded_dataframe(df, buyer):
    colmap = {str(c).strip().lower(): c for c in df.columns}

    def get_col(*names):
        for name in names:
            if name.lower() in colmap:
                return colmap[name.lower()]
        return None

    ad_col = get_col("Название объявления", "Ad name", "Ad Name")
    leads_col = get_col("Лиды", "Leads")
    reg_col = get_col("Завершенные регистрации", "Регистрации", "REG")
    ftd_col = get_col("Покупки", "FTD")
    clicks_col = get_col("Клики по ссылке", "Clicks", "Link Clicks")
    spend_col = get_col("Потраченная сумма (USD)", "Spend", "Amount spent (USD)")
    cpc_col = get_col("CPC (цена за клик по ссылке)", "CPC")
    ctr_col = get_col("CTR (все)", "CTR")
    ds_col = get_col("Дата начала отчетности", "Date Start")
    de_col = get_col("Дата окончания отчетности", "Date End")

    items = []
    for _, row in df.iterrows():
        ad_name = str(row.get(ad_col) or "") if ad_col else ""
        parsed = parse_ad_name(ad_name)

        items.append(
            FBRow(
                uploader=buyer,
                ad_name=ad_name,
                launch_date=parsed["launch_date"],
                platform=parsed["platform"],
                manager=parsed["manager"],
                geo=parsed["geo"],
                offer=parsed["offer"],
                creative=parsed["creative"],
                leads=safe_number(row.get(leads_col)) if leads_col else 0,
                reg=safe_number(row.get(reg_col)) if reg_col else 0,
                ftd=safe_number(row.get(ftd_col)) if ftd_col else 0,
                clicks=safe_number(row.get(clicks_col)) if clicks_col else 0,
                spend=safe_number(row.get(spend_col)) if spend_col else 0,
                cpc=safe_number(row.get(cpc_col)) if cpc_col else 0,
                ctr=safe_number(row.get(ctr_col)) if ctr_col else 0,
                date_start=str(row.get(ds_col) or "") if ds_col else "",
                date_end=str(row.get(de_col) or "") if de_col else "",
            )
        )
    return items


def safe_text(value):
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def safe_cap_number(value):
    text = safe_text(value).replace(",", ".")
    try:
        return float(text) if text else 0.0
    except Exception:
        return 0.0


def normalize_geo_value(value):
    raw = safe_text(value)
    if not raw:
        return ""
    return raw.upper()


def parse_chatterfy_tags(value):
    tags = [safe_text(item) for item in safe_text(value).split(",") if safe_text(item)]
    result = {
        "launch_date": "",
        "platform": "",
        "manager": "",
        "geo": "",
        "offer": "",
        "flow_platform": "",
        "flow_manager": "",
        "flow_geo": "",
    }
    for tag in tags:
        parts = [part.strip() for part in tag.split("/") if part.strip()]
        if len(parts) == 5 and re.match(r"^\d{2}\.\d{2}$", parts[0]):
            result["launch_date"] = parts[0]
            result["platform"] = parts[1]
            result["manager"] = parts[2]
            result["geo"] = normalize_geo_value(parts[3])
            result["offer"] = parts[4]
        elif len(parts) == 3 and not result["flow_platform"]:
            result["flow_platform"] = parts[0]
            result["flow_manager"] = parts[1]
            result["flow_geo"] = normalize_geo_value(parts[2])
    if not result["flow_platform"] and result["platform"]:
        result["flow_platform"] = result["platform"]
        result["flow_manager"] = result["manager"]
        result["flow_geo"] = result["geo"]
    return result


def parse_chatterfy_datetime(value):
    text = safe_text(value)
    if not text:
        return None
    for pattern in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M"):
        try:
            return datetime.strptime(text, pattern)
        except Exception:
            continue
    return None


def cap_fill_percent(current_ftd, cap_value):
    cap_value = safe_number(cap_value)
    current_ftd = safe_number(current_ftd)
    if cap_value <= 0:
        return 0.0
    return (current_ftd / cap_value) * 100


def ensure_upload_dir():
    os.makedirs(DATA_UPLOAD_DIR, exist_ok=True)
    os.makedirs(PARTNER_UPLOAD_DIR, exist_ok=True)


# =========================================
# BLOCK 5 — DATA ACCESS
# =========================================
def get_all_rows():
    db = SessionLocal()
    try:
        return db.query(FBRow).all()
    finally:
        db.close()



def get_filtered_data(buyer="", manager="", geo="", offer="", search=""):
    rows = get_all_rows()
    filtered = []
    search_lower = (search or "").lower().strip()

    for row in rows:
        if buyer and (row.uploader or "") != buyer:
            continue
        if manager and (row.manager or "") != manager:
            continue
        if geo and (row.geo or "") != geo:
            continue
        if offer and (row.offer or "") != offer:
            continue
        if search_lower:
            haystack = " | ".join([
                row.ad_name or "",
                row.platform or "",
                row.manager or "",
                row.geo or "",
                row.offer or "",
                row.creative or "",
                row.uploader or "",
            ]).lower()
            if search_lower not in haystack:
                continue
        filtered.append(row)
    return filtered



def get_filter_options():
    rows = get_all_rows()
    return (
        sorted({r.uploader for r in rows if r.uploader}),
        sorted({r.manager for r in rows if r.manager}),
        sorted({r.geo for r in rows if r.geo}),
        sorted({r.offer for r in rows if r.offer}),
    )


def get_caps_rows(search="", buyer="", geo="", owner_name=""):
    Base.metadata.create_all(bind=engine, tables=[CapRow.__table__])
    db = SessionLocal()
    try:
        query = db.query(CapRow)
        if buyer:
            query = query.filter(CapRow.buyer == buyer)
        if geo:
            query = query.filter(CapRow.geo == geo)
        if owner_name:
            query = query.filter(CapRow.owner_name == owner_name)
        rows = query.order_by(CapRow.buyer.asc(), CapRow.geo.asc(), CapRow.id.desc()).all()
    finally:
        db.close()

    search_lower = (search or "").strip().lower()
    if not search_lower:
        return rows

    filtered = []
    for row in rows:
        haystack = " | ".join([
            row.advertiser or "",
            row.owner_name or "",
            row.buyer or "",
            row.flow or "",
            row.code or "",
            row.geo or "",
            row.promo_code or "",
            row.comments or "",
            row.agent or "",
        ]).lower()
        if search_lower in haystack:
            filtered.append(row)
    return filtered


def get_caps_filter_options():
    Base.metadata.create_all(bind=engine, tables=[CapRow.__table__])
    db = SessionLocal()
    try:
        rows = db.query(CapRow).all()
        return (
            sorted({r.buyer for r in rows if r.buyer}),
            sorted({r.geo for r in rows if r.geo}),
            sorted({r.owner_name for r in rows if r.owner_name}),
        )
    finally:
        db.close()


def import_caps_from_csv_if_needed():
    Base.metadata.create_all(bind=engine, tables=[CapRow.__table__])
    db = SessionLocal()
    try:
        if db.query(CapRow).count() > 0:
            return
        source_path = "/Users/ivansviderko/Downloads/Капы.csv"
        if not os.path.exists(source_path):
            return
        df = pd.read_csv(source_path)
        records = []
        for _, row in df.iterrows():
            records.append(CapRow(
                advertiser=safe_text(row.get("Рекл:")),
                owner_name=safe_text(row.get("Имя:")),
                buyer=safe_text(row.get("Кабинет:")),
                flow=safe_text(row.get("Поток:")),
                code=normalize_geo_value(row.get("CODE:")),
                geo=normalize_geo_value(row.get("GEO:")),
                rate=safe_text(row.get("Ставка:")),
                baseline=safe_text(row.get("БЛ:")),
                cap_value=safe_cap_number(row.get("Капа:")),
                promo_code=safe_text(row.get("Промокод:")),
                kpi=safe_text(row.get("КПИ:")),
                link=safe_text(row.get("Ссылка:")),
                comments=safe_text(row.get("Коментарии:")),
                agent=safe_text(row.get("Агент:")),
                current_ftd=0,
            ))
        if records:
            for item in records:
                db.add(item)
            db.commit()
    finally:
        db.close()


def parse_datetime_local(value: str):
    text = (value or "").strip()
    if not text:
        return None
    for pattern in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, pattern)
        except Exception:
            continue
    return None


def format_datetime_local(value):
    if not value:
        return ""
    try:
        return value.strftime("%Y-%m-%dT%H:%M")
    except Exception:
        return ""


def format_datetime_human(value):
    if not value:
        return "Без срока"
    try:
        return value.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "Без срока"


def build_task_datetime_selects(prefix: str, selected_value: str = ""):
    selected_dt = parse_datetime_local(selected_value) if selected_value else None
    today = datetime(2026, 3, 26, 12, 0)
    default_dt = selected_dt or today

    year_html = ""
    for year in [2026, 2027]:
        selected = "selected" if default_dt.year == year else ""
        year_html += f'<option value="{year}" {selected}>{year}</option>'

    month_html = ""
    for month in range(1, 13):
        selected = "selected" if default_dt.month == month else ""
        month_html += f'<option value="{month}" {selected}>{month:02d}</option>'

    day_html = ""
    for day in range(1, 32):
        selected = "selected" if default_dt.day == day else ""
        day_html += f'<option value="{day}" {selected}>{day:02d}</option>'

    hour_html = ""
    for hour in range(0, 24):
        selected = "selected" if default_dt.hour == hour else ""
        hour_html += f'<option value="{hour}" {selected}>{hour:02d}</option>'

    minute_html = ""
    for minute in [0, 15, 30, 45]:
        selected = "selected" if default_dt.minute == minute else ""
        minute_html += f'<option value="{minute}" {selected}>{minute:02d}</option>'

    return f"""
    <div class="datetime-grid">
        <label>Год<select name="{prefix}_year">{year_html}</select></label>
        <label>Месяц<select name="{prefix}_month">{month_html}</select></label>
        <label>День<select name="{prefix}_day">{day_html}</select></label>
        <label>Час<select name="{prefix}_hour">{hour_html}</select></label>
        <label>Мин<select name="{prefix}_minute">{minute_html}</select></label>
    </div>
    """


def compose_task_datetime_from_form(year: str, month: str, day: str, hour: str, minute: str):
    try:
        dt = datetime(int(year), int(month), int(day), int(hour), int(minute))
    except Exception:
        return ""
    return dt.strftime("%Y-%m-%dT%H:%M")


def parse_money_value(value):
    text = safe_text(value).replace("$", "").replace("\xa0", "").replace(" ", "").replace(",", ".")
    if not text:
        return 0.0
    try:
        return float(text)
    except Exception:
        return 0.0


def load_finance_snapshot():
    ensure_upload_dir()
    default_path = "/Users/ivansviderko/Downloads/финансы.csv"
    source_path = FINANCE_UPLOAD_PATH if os.path.exists(FINANCE_UPLOAD_PATH) else default_path
    result = {
        "source_path": source_path,
        "totals": {
            "wallets": 0.0,
            "expenses": 0.0,
            "income": 0.0,
            "pending": 0.0,
            "transfers": 0.0,
        },
        "wallets": [],
        "expenses": [],
        "income": [],
        "pending": [],
        "transfers": [],
    }
    if not os.path.exists(source_path):
        return result

    with open(source_path, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    if len(rows) < 2:
        return result

    header_totals = rows[0]
    result["totals"] = {
        "wallets": parse_money_value(header_totals[4] if len(header_totals) > 4 else ""),
        "expenses": parse_money_value(header_totals[8] if len(header_totals) > 8 else ""),
        "income": parse_money_value(header_totals[15] if len(header_totals) > 15 else ""),
        "pending": parse_money_value(header_totals[22] if len(header_totals) > 22 else ""),
        "transfers": parse_money_value(header_totals[27] if len(header_totals) > 27 else ""),
    }

    for row in rows[2:]:
        row = row + [""] * (31 - len(row))
        if any(safe_text(x) for x in row[0:5]):
            result["wallets"].append({
                "category": safe_text(row[0]),
                "description": safe_text(row[1]),
                "owner": safe_text(row[2]),
                "wallet": safe_text(row[3]),
                "amount": parse_money_value(row[4]),
            })
        if any(safe_text(x) for x in row[5:11]):
            result["expenses"].append({
                "section": safe_text(row[5]) or "Расход",
                "date": safe_text(row[6]),
                "category": safe_text(row[7]),
                "amount": parse_money_value(row[8]),
                "paid_by": safe_text(row[9]),
                "comment": safe_text(row[10]),
            })
        if any(safe_text(x) for x in row[11:18]):
            result["income"].append({
                "section": safe_text(row[11]) or "Приход",
                "date": safe_text(row[12]),
                "category": safe_text(row[13]),
                "description": safe_text(row[14]),
                "amount": parse_money_value(row[15]),
                "wallet": safe_text(row[16]),
                "reconciliation": safe_text(row[17]),
            })
        if any(safe_text(x) for x in row[18:25]):
            result["pending"].append({
                "section": safe_text(row[18]) or "Ожидаем",
                "date": safe_text(row[19]),
                "category": safe_text(row[20]),
                "description": safe_text(row[21]),
                "amount": parse_money_value(row[22]),
                "wallet": safe_text(row[23]),
                "reconciliation": safe_text(row[24]),
            })
        if any(safe_text(x) for x in row[25:31]):
            result["transfers"].append({
                "section": safe_text(row[25]) or "Перемещение",
                "date": safe_text(row[26]),
                "amount": parse_money_value(row[27]),
                "from_wallet": safe_text(row[28]),
                "to_wallet": safe_text(row[29]),
                "comment": safe_text(row[30]),
            })

    return result


def get_task_status_options():
    return ["Не начато", "В работе", "Ожидает ответ", "Заблокировано", "Выполнено"]


def get_assignable_users():
    db = SessionLocal()
    try:
        return db.query(User).filter(User.is_active == 1, User.role.in_(["buyer", "operator", "finance"])).order_by(User.display_name.asc(), User.username.asc()).all()
    finally:
        db.close()


def get_tasks_for_user(current_user, status_filter="", assignee_filter="", search=""):
    Base.metadata.create_all(bind=engine, tables=[TaskRow.__table__])
    db = SessionLocal()
    try:
        query = db.query(TaskRow)
        if is_admin_role(current_user):
            if assignee_filter:
                query = query.filter(TaskRow.assigned_to_username == assignee_filter)
        else:
            query = query.filter(TaskRow.assigned_to_username == (current_user or {}).get("username"))
        if status_filter:
            query = query.filter(TaskRow.status == status_filter)
        rows = query.order_by(TaskRow.due_at.asc().nulls_last(), TaskRow.updated_at.desc(), TaskRow.id.desc()).all()
    finally:
        db.close()

    search_lower = (search or "").strip().lower()
    if not search_lower:
        return rows

    filtered = []
    for row in rows:
        haystack = " | ".join([
            row.title or "",
            row.description or "",
            row.assigned_to_name or "",
            row.assigned_to_role or "",
            row.created_by_name or "",
            row.notes or "",
            row.response_text or "",
            row.status or "",
        ]).lower()
        if search_lower in haystack:
            filtered.append(row)
    return filtered


def ensure_partner_table():
    Base.metadata.create_all(bind=engine, tables=[PartnerRow.__table__])


def ensure_chatterfy_table():
    Base.metadata.create_all(bind=engine, tables=[ChatterfyRow.__table__])


def ensure_chatterfy_id_table():
    Base.metadata.create_all(bind=engine, tables=[ChatterfyIdRow.__table__])


def get_half_month_period(today: date | None = None):
    if today is None:
        today = datetime.utcnow().date()

    year = today.year
    month = today.month
    last_day = calendar.monthrange(year, month)[1]

    if today.day <= 15:
        start_day = 1
        end_day = 15
    else:
        start_day = 16
        end_day = last_day

    date_start = date(year, month, start_day)
    date_end = date(year, month, end_day)

    return {
        "date_start": date_start.strftime("%Y-%m-%d"),
        "date_end": date_end.strftime("%Y-%m-%d"),
        "period_label": f"{start_day:02d}-{end_day:02d}.{month:02d}.{year}",
    }


def build_partner_source_name(date_start: str, date_end: str, prefix: str = "1xbet_players"):
    try:
        start_dt = datetime.strptime((date_start or "").strip(), "%Y-%m-%d")
        end_dt = datetime.strptime((date_end or "").strip(), "%Y-%m-%d")
        return f"{prefix}|{start_dt.strftime('%d')}-{end_dt.strftime('%d.%m.%Y')}"
    except Exception:
        if date_start and date_end:
            return f"{prefix}|{date_start}_{date_end}"
        return prefix


def detect_partner_period_from_dataframe(df):
    pattern = re.compile(r"(\\d{2}\\.\\d{2}\\.\\d{4})\\s*-\\s*(\\d{2}\\.\\d{2}\\.\\d{4})")
    try:
        for row in df.itertuples(index=False):
            for value in row:
                text_value = safe_text(value)
                if not text_value:
                    continue
                match = pattern.search(text_value)
                if not match:
                    continue
                start_dt = datetime.strptime(match.group(1), "%d.%m.%Y")
                end_dt = datetime.strptime(match.group(2), "%d.%m.%Y")
                return {
                    "date_start": start_dt.strftime("%Y-%m-%d"),
                    "date_end": end_dt.strftime("%Y-%m-%d"),
                    "period_label": f"{start_dt.strftime('%d')}-{end_dt.strftime('%d.%m.%Y')}",
                }
    except Exception:
        pass
    return None


def normalize_partner_period(date_start: str = "", date_end: str = ""):
    clean_start = safe_text(date_start)
    clean_end = safe_text(date_end)
    if clean_start and clean_end:
        try:
            start_dt = datetime.strptime(clean_start, "%Y-%m-%d")
            end_dt = datetime.strptime(clean_end, "%Y-%m-%d")
            return {
                "date_start": start_dt.strftime("%Y-%m-%d"),
                "date_end": end_dt.strftime("%Y-%m-%d"),
                "period_label": f"{start_dt.strftime('%d')}-{end_dt.strftime('%d.%m.%Y')}",
            }
        except Exception:
            pass
    return get_half_month_period()

def parse_partner_dataframe(df, source_name=""):
    records = []
    for _, row in df.iterrows():
        sub_id = safe_text(row.get("SubId"))
        player_id = safe_text(row.get("ID игрока"))
        country = normalize_geo_value(row.get("Страна"))
        if not sub_id or sub_id in {"SUBID", "ID ПАРТНЕРА", "ПЕРИОД", "ВАЛЮТА", "КАМПАНИЯ", "ТОЛЬКО НОВЫЕ ИГРОКИ", "ТОЛЬКО ИГРОКИ БЕЗ ДЕПОЗИТОВ"}:
            continue
        if not player_id or player_id == "ID игрока":
            continue
        if not country or country in {"СТРАНА"}:
            continue
        deposit_amount = safe_number(row.get("Сумма депозитов"))
        company_income = safe_number(row.get("Доход компании (общий)"))
        cpa_amount = safe_number(row.get("CPA"))
        records.append(PartnerRow(
            source_name=source_name,
            sub_id=sub_id,
            player_id=player_id,
            registration_date=safe_text(row.get("Дата регистрации")),
            country=country,
            deposit_amount=deposit_amount,
            bet_amount=safe_number(row.get("Сумма ставок")),
            company_income=company_income,
            cpa_amount=cpa_amount,
            hold_time=safe_text(row.get("Hold time")),
            blocked=safe_text(row.get("Заблокирован")),
        ))
    return records


def replace_partner_rows(source_name, rows_to_insert):
    ensure_partner_table()
    db = SessionLocal()
    try:
        if source_name:
            db.query(PartnerRow).filter(PartnerRow.source_name == source_name).delete()
        else:
            db.query(PartnerRow).delete()
        db.commit()
        for item in rows_to_insert:
            db.add(item)
        db.commit()
    finally:
        db.close()
    refresh_cap_current_ftd_from_partner()


def import_chatterfy_dataframe(df, source_name=""):
    ensure_chatterfy_table()
    records = []
    for _, row in df.iterrows():
        tags = safe_text(row.get("Tags"))
        parsed = parse_chatterfy_tags(tags)
        records.append(ChatterfyRow(
            source_name=source_name,
            name=safe_text(row.get("Name")),
            telegram_id=safe_text(row.get("Telegram ID")),
            username=safe_text(row.get("Username")),
            tags=tags,
            started=safe_text(row.get("Started")),
            last_user_message=safe_text(row.get("Last User Message")),
            last_bot_message=safe_text(row.get("Last Bot Message")),
            status=safe_text(row.get("Status")),
            step=safe_text(row.get("Step")),
            external_id=safe_text(row.get("ID")),
            launch_date=parsed["launch_date"],
            platform=parsed["platform"],
            manager=parsed["manager"],
            geo=parsed["geo"],
            offer=parsed["offer"],
            flow_platform=parsed["flow_platform"],
            flow_manager=parsed["flow_manager"],
            flow_geo=parsed["flow_geo"],
        ))
    db = SessionLocal()
    try:
        db.query(ChatterfyRow).delete()
        db.commit()
        for item in records:
            db.add(item)
        db.commit()
    finally:
        db.close()
    return len(records)


def import_chatterfy_ids_dataframe(df):
    ensure_chatterfy_id_table()
    records = []
    for _, row in df.iterrows():
        telegram_id = safe_text(row.get("TELEGRAM ID") or row.get("Telegram ID") or row.get("telegram_id"))
        pp_player_id = safe_text(row.get("1xbet_id") or row.get("pp_id") or row.get("ID игрока"))
        chat_link = safe_text(row.get("chatlink") or row.get("chat_link") or row.get("link"))
        source_date = safe_text(row.get("date") or row.get("Date"))
        if not telegram_id and not pp_player_id and not chat_link:
            continue
        records.append(ChatterfyIdRow(
            telegram_id=telegram_id,
            pp_player_id=pp_player_id,
            chat_link=chat_link,
            source_date=source_date,
        ))
    db = SessionLocal()
    try:
        db.query(ChatterfyIdRow).delete()
        db.commit()
        for item in records:
            db.add(item)
        db.commit()
    finally:
        db.close()
    return len(records)


def get_chatterfy_rows(status="", search="", date_filter="", time_filter="", telegram_id="", pp_player_id=""):
    ensure_chatterfy_table()
    ensure_chatterfy_id_table()
    db = SessionLocal()
    try:
        rows = db.query(ChatterfyRow).order_by(ChatterfyRow.id.desc()).all()
        id_rows = db.query(ChatterfyIdRow).all()
    finally:
        db.close()
    id_map = {}
    for item in id_rows:
        key = safe_text(item.telegram_id)
        if key:
            id_map[key] = item
    filtered = []
    search_lower = safe_text(search).lower()
    for row in rows:
        linked = id_map.get(safe_text(row.telegram_id))
        started_dt = parse_chatterfy_datetime(row.started)
        started_date = started_dt.strftime("%d.%m.%Y") if started_dt else ""
        started_time = started_dt.strftime("%H:%M") if started_dt else ""
        linked_pp = safe_text(linked.pp_player_id) if linked else ""
        linked_chat = safe_text(linked.chat_link) if linked else ""
        if status and (row.status or "") != status:
            continue
        if date_filter and date_filter != started_date:
            continue
        if time_filter and not started_time.startswith(time_filter):
            continue
        if telegram_id and telegram_id not in safe_text(row.telegram_id):
            continue
        if pp_player_id and pp_player_id not in linked_pp:
            continue
        if search_lower:
            haystack = " | ".join([
                row.name or "",
                row.username or "",
                row.telegram_id or "",
                row.tags or "",
                row.status or "",
                row.offer or "",
                row.manager or "",
                row.geo or "",
                linked_pp,
                linked_chat,
                started_date,
                started_time,
            ]).lower()
            if search_lower not in haystack:
                continue
        filtered.append({
            "row": row,
            "started_date": started_date,
            "started_time": started_time,
            "pp_player_id": linked_pp,
            "chat_link": linked_chat,
        })
    return filtered


def import_chatterfy_from_csv_if_needed():
    ensure_chatterfy_table()
    db = SessionLocal()
    try:
        if db.query(ChatterfyRow).count() > 0:
            return
    finally:
        db.close()
    source_path = "/Users/ivansviderko/Downloads/Выгрузка (16.03-31.03.26) - Chatterfy.csv"
    if not os.path.exists(source_path):
        return
    df = pd.read_csv(source_path)
    import_chatterfy_dataframe(df, os.path.basename(source_path))


def import_chatterfy_ids_from_csv_if_needed():
    ensure_chatterfy_id_table()
    db = SessionLocal()
    try:
        if db.query(ChatterfyIdRow).count() > 0:
            return
    finally:
        db.close()
    source_path = "/Users/ivansviderko/Downloads/ID_Chatterfy.csv"
    if not os.path.exists(source_path):
        return
    df = pd.read_csv(source_path)
    import_chatterfy_ids_dataframe(df)

def get_partner_period_options():
    ensure_partner_table()
    db = SessionLocal()
    try:
        values = db.query(PartnerRow.source_name).distinct().all()
        result = []
        for item in values:
            value = safe_text(item[0])
            if value:
                result.append(value)
        return sorted(result, reverse=True)
    finally:
        db.close()


def get_partner_rows_by_period(period_value=""):
    ensure_partner_table()
    db = SessionLocal()
    try:
        query = db.query(PartnerRow)
        if period_value:
            query = query.filter(PartnerRow.source_name == period_value)
        return query.order_by(PartnerRow.id.desc()).all()
    finally:
        db.close()


def aggregate_partner_totals(rows):
    return {
        "players": len(rows),
        "deposits": sum(safe_number(r.deposit_amount) for r in rows),
        "bets": sum(safe_number(r.bet_amount) for r in rows),
        "income": sum(safe_number(r.company_income) for r in rows),
        "cpa": sum(safe_number(r.cpa_amount) for r in rows),
        "ftd_count": sum(1 for r in rows if safe_number(r.deposit_amount) > 0),
    }
def refresh_cap_current_ftd_from_partner():
    ensure_partner_table()
    Base.metadata.create_all(bind=engine, tables=[CapRow.__table__])
    db = SessionLocal()
    try:
        caps = db.query(CapRow).all()
        partner_rows = db.query(PartnerRow).all()
        by_sub = {}
        for row in partner_rows:
            key = (row.sub_id or "").strip().upper()
            if not key:
                continue
            by_sub.setdefault(key, []).append(row)
        for cap in caps:
            promo_key = (cap.promo_code or "").strip().upper()
            matched = by_sub.get(promo_key, [])
            cap.current_ftd = float(sum(1 for item in matched if safe_number(item.deposit_amount) > 0))
            db.add(cap)
        db.commit()
    finally:
        db.close()


def is_partner_row_qualified_for_cap(row, cap):
    deposit_amount = safe_number(getattr(row, "deposit_amount", 0))
    bet_amount = safe_number(getattr(row, "bet_amount", 0))
    baseline = safe_cap_number(getattr(cap, "baseline", 0))
    if deposit_amount <= 0:
        return False
    if baseline > 0 and deposit_amount < baseline:
        return False
    if baseline > 0 and bet_amount < baseline:
        return False
    return True


def get_statistic_support_maps():
    import_chatterfy_from_csv_if_needed()
    ensure_partner_table()
    ensure_chatterfy_table()
    Base.metadata.create_all(bind=engine, tables=[CapRow.__table__])
    db = SessionLocal()
    try:
        caps = db.query(CapRow).all()
        partner_rows = db.query(PartnerRow).all()
        chatterfy_rows = db.query(ChatterfyRow).all()
    finally:
        db.close()

    caps_by_sub = {}
    for cap in caps:
        promo_key = (cap.promo_code or "").strip().upper()
        if not promo_key:
            continue
        caps_by_sub.setdefault(promo_key, []).append(cap)

    partner_by_flow = {}
    for row in partner_rows:
        promo_key = (row.sub_id or "").strip().upper()
        matched_caps = caps_by_sub.get(promo_key, [])
        for cap in matched_caps:
            flow_parts = [part.strip() for part in safe_text(cap.flow).split("/") if part.strip()]
            platform = flow_parts[0] if len(flow_parts) > 0 else ""
            manager = flow_parts[1] if len(flow_parts) > 1 else safe_text(cap.owner_name)
            geo_code = normalize_geo_value(flow_parts[2] if len(flow_parts) > 2 else (cap.code or cap.geo or row.country or ""))
            if not platform or not manager or not geo_code:
                continue
            key = build_flow_key(platform, manager, geo_code)
            info = partner_by_flow.setdefault(key, {
                "stat_total_ftd": 0.0,
                "stat_qual_ftd": 0.0,
                "stat_rate": safe_cap_number(cap.rate),
                "stat_income": 0.0,
                "stat_cap_limit": 0.0,
                "stat_cap_fill": 0.0,
                "stat_has_cap": 0.0,
                "stat_promos": set(),
            })
            info["stat_has_cap"] = 1.0
            info["stat_rate"] = safe_cap_number(cap.rate) or info["stat_rate"]
            info["stat_cap_limit"] += safe_number(cap.cap_value)
            if safe_number(row.deposit_amount) > 0:
                info["stat_total_ftd"] += 1
            if is_partner_row_qualified_for_cap(row, cap):
                info["stat_qual_ftd"] += 1
                info["stat_income"] += safe_cap_number(cap.rate)
            info["stat_promos"].add(cap.promo_code or "")

    for info in partner_by_flow.values():
        info["stat_cap_fill"] = cap_fill_percent(info["stat_total_ftd"], info["stat_cap_limit"])

    chatterfy_by_ad = {}
    chatterfy_by_flow = {}
    for row in chatterfy_rows:
        ad_key = build_ad_offer_key(row.launch_date, row.platform, row.manager, row.geo, row.offer)
        flow_key = build_flow_key(row.flow_platform or row.platform, row.flow_manager or row.manager, row.flow_geo or row.geo)
        row_identity = safe_text(row.external_id) or safe_text(row.telegram_id) or f"row-{row.id}"
        if all(ad_key):
            chatterfy_by_ad.setdefault(ad_key, set()).add(row_identity)
        if all(flow_key):
            chatterfy_by_flow.setdefault(flow_key, set()).add(row_identity)

    chatterfy_by_ad = {key: float(len(values)) for key, values in chatterfy_by_ad.items()}
    chatterfy_by_flow = {key: float(len(values)) for key, values in chatterfy_by_flow.items()}
    return partner_by_flow, chatterfy_by_ad, chatterfy_by_flow


def enrich_statistic_rows(rows):
    partner_by_flow, chatterfy_by_ad, chatterfy_by_flow = get_statistic_support_maps()
    ad_bucket_weights = {}
    ad_bucket_counts = {}
    for item in rows:
        ad_key = build_ad_offer_key(item.get("launch_date"), item.get("platform"), item.get("manager"), item.get("geo"), item.get("offer"))
        weight = safe_number(item.get("ftd")) or safe_number(item.get("leads")) or safe_number(item.get("spend")) or 1.0
        ad_bucket_weights[ad_key] = ad_bucket_weights.get(ad_key, 0.0) + weight
        ad_bucket_counts[ad_key] = ad_bucket_counts.get(ad_key, 0) + 1
    enriched = []
    for item in rows:
        flow_key = build_flow_key(item.get("platform"), item.get("manager"), item.get("geo"))
        ad_key = build_ad_offer_key(item.get("launch_date"), item.get("platform"), item.get("manager"), item.get("geo"), item.get("offer"))
        flow_stat = partner_by_flow.get(flow_key, {})
        chatter_count = chatterfy_by_ad.get(ad_key, 0.0)
        flow_chatter_count = chatterfy_by_flow.get(flow_key, 0.0)
        offer_share = (chatter_count / flow_chatter_count) if flow_chatter_count > 0 else 0.0
        row_weight = safe_number(item.get("ftd")) or safe_number(item.get("leads")) or safe_number(item.get("spend")) or 1.0
        bucket_weight = ad_bucket_weights.get(ad_key, 0.0)
        bucket_count = ad_bucket_counts.get(ad_key, 1)
        creative_share = (row_weight / bucket_weight) if bucket_weight > 0 else (1.0 / bucket_count)
        share = offer_share * creative_share
        clone = dict(item)
        clone["stat_chatterfy"] = chatter_count * creative_share
        clone["stat_total_ftd"] = flow_stat.get("stat_total_ftd", 0.0) * share
        clone["stat_qual_ftd"] = flow_stat.get("stat_qual_ftd", 0.0) * share
        clone["stat_rate"] = flow_stat.get("stat_rate", 0.0)
        clone["stat_income"] = flow_stat.get("stat_income", 0.0) * share
        clone["stat_cap_limit"] = flow_stat.get("stat_cap_limit", 0.0) * share
        clone["stat_cap_fill"] = cap_fill_percent(clone["stat_total_ftd"], clone["stat_cap_limit"])
        clone["stat_has_cap"] = flow_stat.get("stat_has_cap", 0.0) if share > 0 or flow_stat.get("stat_has_cap", 0.0) else 0.0
        clone["stat_profit"] = clone["stat_income"] - (clone.get("spend") or 0)
        clone["stat_roi"] = (clone["stat_profit"] / clone.get("spend")) * 100 if (clone.get("spend") or 0) > 0 else 0
        enriched.append(clone)
    return enriched


def ensure_finance_tables():
    Base.metadata.create_all(bind=engine, tables=[
        FinanceWalletRow.__table__,
        FinanceExpenseRow.__table__,
        FinanceIncomeRow.__table__,
    ])


def load_manual_finance():
    ensure_finance_tables()
    db = SessionLocal()
    try:
        return {
            "wallets": db.query(FinanceWalletRow).order_by(FinanceWalletRow.id.desc()).all(),
            "expenses": db.query(FinanceExpenseRow).order_by(FinanceExpenseRow.id.desc()).all(),
            "income": db.query(FinanceIncomeRow).order_by(FinanceIncomeRow.id.desc()).all(),
        }
    finally:
        db.close()


def import_caps_dataframe(df):
    Base.metadata.create_all(bind=engine, tables=[CapRow.__table__])
    db = SessionLocal()
    try:
        existing_rows = db.query(CapRow).all()
        existing_map = {
            (
                (item.buyer or "").strip().upper(),
                (item.flow or "").strip().upper(),
                (item.code or "").strip().upper(),
                (item.geo or "").strip().upper(),
                (item.promo_code or "").strip().upper(),
            ): item
            for item in existing_rows
        }
        for _, row in df.iterrows():
            key = (
                safe_text(row.get("Кабинет:")).upper(),
                safe_text(row.get("Поток:")).upper(),
                normalize_geo_value(row.get("CODE:")),
                normalize_geo_value(row.get("GEO:")),
                safe_text(row.get("Промокод:")).upper(),
            )
            item = existing_map.get(key)
            if not item:
                item = CapRow()
                db.add(item)
            item.advertiser = safe_text(row.get("Рекл:"))
            item.owner_name = safe_text(row.get("Имя:"))
            item.buyer = safe_text(row.get("Кабинет:"))
            item.flow = safe_text(row.get("Поток:"))
            item.code = normalize_geo_value(row.get("CODE:"))
            item.geo = normalize_geo_value(row.get("GEO:"))
            item.rate = safe_text(row.get("Ставка:"))
            item.baseline = safe_text(row.get("БЛ:"))
            item.cap_value = safe_cap_number(row.get("Капа:"))
            item.promo_code = safe_text(row.get("Промокод:"))
            item.kpi = safe_text(row.get("КПИ:"))
            item.link = safe_text(row.get("Ссылка:"))
            item.comments = safe_text(row.get("Коментарии:"))
            item.agent = safe_text(row.get("Агент:"))
        db.commit()
    finally:
        db.close()


def import_tasks_dataframe(df, assigned_user, created_by_user):
    Base.metadata.create_all(bind=engine, tables=[TaskRow.__table__])
    db = SessionLocal()
    try:
        target_user = db.query(User).filter(User.username == assigned_user, User.is_active == 1).first()
        if not target_user:
            return False
        for _, row in df.iterrows():
            title = safe_text(row.get("Задача"))
            if not title:
                continue
            existing = db.query(TaskRow).filter(
                TaskRow.assigned_to_username == target_user.username,
                TaskRow.title == title,
            ).first()
            item = existing or TaskRow()
            if not existing:
                db.add(item)
            item.title = title
            item.description = title
            item.assigned_to_username = target_user.username
            item.assigned_to_name = target_user.display_name or target_user.username
            item.assigned_to_role = target_user.role or ""
            item.created_by_username = created_by_user.get("username", "")
            item.created_by_name = created_by_user.get("display_name", created_by_user.get("username", ""))
            item.status = safe_text(row.get("Статус")) or "Не начато"
            item.notes = safe_text(row.get("Примечания"))
            item.due_at = parse_datetime_local(safe_text(row.get("Срок выполнения")).replace(" ", "T"))
            item.updated_at = datetime.utcnow()
            if not getattr(item, "created_at", None):
                item.created_at = datetime.utcnow()
        db.commit()
        return True
    finally:
        db.close()


def build_finance_form_data(wallet_item=None, expense_item=None, income_item=None):
    data = {}
    if wallet_item:
        data.update({
            "wallet_edit_id": str(wallet_item.id),
            "wallet_category": wallet_item.category or "",
            "wallet_description": wallet_item.description or "",
            "wallet_owner_name": wallet_item.owner_name or "",
            "wallet_wallet": wallet_item.wallet or "",
            "wallet_amount": format_int_or_float(wallet_item.amount),
        })
    if expense_item:
        data.update({
            "expense_edit_id": str(expense_item.id),
            "expense_date": expense_item.expense_date or "",
            "expense_category": expense_item.category or "",
            "expense_amount": format_int_or_float(expense_item.amount),
            "expense_paid_by": expense_item.paid_by or "",
            "expense_comment": expense_item.comment or "",
        })
    if income_item:
        data.update({
            "income_edit_id": str(income_item.id),
            "income_date": income_item.income_date or "",
            "income_category": income_item.category or "",
            "income_description": income_item.description or "",
            "income_amount": format_int_or_float(income_item.amount),
            "income_wallet": income_item.wallet or "",
            "income_reconciliation": income_item.reconciliation or "",
        })
    return data


# =========================================
# BLOCK 6 — AGGREGATION
# =========================================
def aggregate_grouped_rows(rows):
    grouped = {}
    for row in rows:
        key = f"{row.uploader or ''}|||{row.ad_name or ''}"
        if key not in grouped:
            grouped[key] = {
                "buyer": row.uploader or "",
                "ad_name": row.ad_name or "",
                "launch_date": row.launch_date or "",
                "platform": row.platform or "",
                "manager": row.manager or "",
                "geo": row.geo or "",
                "offer": row.offer or "",
                "creative": row.creative or "",
                "clicks": 0.0,
                "leads": 0.0,
                "reg": 0.0,
                "ftd": 0.0,
                "spend": 0.0,
                "rows_combined": 0,
                "date_start": row.date_start or "",
                "date_end": row.date_end or "",
            }

        grouped[key]["clicks"] += row.clicks or 0
        grouped[key]["leads"] += row.leads or 0
        grouped[key]["reg"] += row.reg or 0
        grouped[key]["ftd"] += row.ftd or 0
        grouped[key]["spend"] += row.spend or 0
        grouped[key]["rows_combined"] += 1

    result = list(grouped.values())
    for item in result:
        item.update(calc_metrics(item["clicks"], item["reg"], item["ftd"], item["spend"], item["leads"]))
    return result



def aggregate_totals(rows):
    totals = {
        "clicks": sum(r["clicks"] for r in rows),
        "leads": sum(r["leads"] for r in rows),
        "reg": sum(r["reg"] for r in rows),
        "ftd": sum(r["ftd"] for r in rows),
        "spend": sum(r["spend"] for r in rows),
        "stat_chatterfy": sum(r.get("stat_chatterfy", 0) for r in rows),
        "stat_total_ftd": sum(r.get("stat_total_ftd", 0) for r in rows),
        "stat_qual_ftd": sum(r.get("stat_qual_ftd", 0) for r in rows),
        "stat_income": sum(r.get("stat_income", 0) for r in rows),
        "stat_cap_limit": sum(r.get("stat_cap_limit", 0) for r in rows),
        "stat_has_cap": sum(r.get("stat_has_cap", 0) for r in rows),
    }
    totals["stat_profit"] = totals["stat_income"] - totals["spend"]
    totals["stat_roi"] = (totals["stat_profit"] / totals["spend"]) * 100 if totals["spend"] > 0 else 0
    totals["stat_rate"] = totals["stat_income"] / totals["stat_qual_ftd"] if totals["stat_qual_ftd"] > 0 else 0
    totals["stat_cap_fill"] = cap_fill_percent(totals["stat_total_ftd"], totals["stat_cap_limit"])
    totals.update(calc_metrics(totals["clicks"], totals["reg"], totals["ftd"], totals["spend"], totals["leads"]))
    return totals



def aggregate_for_hierarchy(rows, keys):
    if not keys:
        return []

    first_key = keys[0]
    buckets = {}
    for row in rows:
        bucket = (row.get(first_key) or "Без значения").strip() or "Без значения"
        buckets.setdefault(bucket, []).append(row)

    result = []
    for bucket_name in sorted(buckets.keys()):
        bucket_rows = buckets[bucket_name]
        metrics = aggregate_totals(bucket_rows)
        node = {
            "name": bucket_name,
            "key": first_key,
            "metrics": metrics,
            "children": aggregate_for_hierarchy(bucket_rows, keys[1:]),
            "rows": bucket_rows,
        }
        result.append(node)
    return result


# =========================================
# BLOCK 7 — UI HELPERS
# =========================================
def sidebar_html(active_page, current_user=None):
    items = [
        ("grouped", "/grouped", "📘", "FB", [
            ("/grouped", "Export", active_page == "grouped"),
            ("/hierarchy", "Statistic", active_page == "hierarchy"),
        ]),
        ("finance", "/finance", "💸", "Finance", []),
        ("caps", "/caps", "🔶", "Caps", []),
        ("onexbet_report", "/1xbet-report", "🎰", "1xBet Report", []),
        ("chatterfy", "/chatterfy", "💬", "Chatterfy", []),
        ("holdwager", "/hold-wager", "🎯", "Hold/Wager", []),
    ]

    html = '''
    <aside class="sidebar">
        <div class="sidebar-brand">
            <span class="brand-mark"></span>
            <span>TEAMbead CRM</span>
        </div>
    '''

    for key, href, icon, title, children in items:
        if key == "grouped":
            if not can_access_page(current_user, "grouped"):
                continue
            children = [
                child for child in children
                if can_access_page(current_user, "hierarchy" if child[0] == "/hierarchy" else "grouped")
            ]
        elif not can_access_page(current_user, key):
            continue

        if children:
            open_attr = "open" if active_page in ["grouped", "hierarchy"] else ""
            html += f'''
            <details class="sidebar-group" {open_attr}>
                <summary><span class="side-emoji">{icon}</span><span>{title}</span></summary>
                <div class="sidebar-links">
            '''
            for child_href, child_title, active in children:
                active_class = "active-link" if active else ""
                html += f'<a href="{child_href}" class="{active_class}">{child_title}</a>'
            html += '</div></details>'
        else:
            active_class = "sidebar-standalone active-link" if active_page == key else "sidebar-standalone"
            html += f'<a href="{href}" class="{active_class}"><span class="side-emoji">{icon}</span><span>{title}</span></a>'

    bottom_links = ""
    if can_access_page(current_user, "tasks"):
        active_class = "sidebar-standalone subtle-link active-link" if active_page == "tasks" else "sidebar-standalone subtle-link"
        bottom_links += f'<a href="/tasks" class="{active_class}"><span class="side-emoji">✅</span><span>Tasks</span></a>'
    if can_access_page(current_user, "users"):
        active_class = "sidebar-standalone subtle-link active-link" if active_page == "users" else "sidebar-standalone subtle-link"
        bottom_links += f'<a href="/users" class="{active_class}"><span class="side-emoji">🧑</span><span>Users</span></a>'

    html += f'''
        <div class="sidebar-bottom">
            <div class="sidebar-bottom-links">{bottom_links}</div>
            <div class="sidebar-user">
                <div class="user-chip sidebar-user-chip">👤 {escape((current_user or {}).get("display_name", "Гость"))} · {escape(role_label((current_user or {}).get("role", "guest")))}</div>
                <div class="sidebar-mini-actions">
                    <a class="ghost-btn small-btn minimal-btn logout-btn" href="/logout">↩ Log Out</a>
                    <details class="theme-menu">
                        <summary class="ghost-btn small-btn minimal-btn">Theme</summary>
                        <div class="theme-menu-list">
                            <button type="button" class="ghost-btn small-btn minimal-btn" onclick="setTheme('dark')">Dark</button>
                            <button type="button" class="ghost-btn small-btn minimal-btn" onclick="setTheme('light')">Light</button>
                        </div>
                    </details>
                </div>
            </div>
        </div>
    </aside>
    '''
    return html



def page_shell(title, content, active_page="grouped", extra_scripts="", top_actions="", current_user=None):
    sidebar = sidebar_html(active_page, current_user=current_user)
    return f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <link rel="icon" type="image/jpeg" href="/favicon.jpg?v=2">
        <link rel="shortcut icon" href="/favicon.ico?v=2">
        <title>{escape(title)}</title>
        <style>
            :root {{
                --bg: #07101f;
                --panel: #0d1729;
                --panel-2: #111f35;
                --panel-3: #16243c;
                --text: #ebf2ff;
                --muted: #8ca3c7;
                --border: #1f3150;
                --accent1: #38bdf8;
                --accent2: #2563eb;
                --accent3: #22c55e;
                --shadow: 0 12px 30px rgba(0,0,0,0.28);
                --table-head: #081120;
                --table-head-text: #dce8ff;
                --row-even: rgba(255,255,255,0.015);
                --good: rgba(34,197,94,0.13);
                --warn: rgba(245,158,11,0.12);
                --bad: rgba(239,68,68,0.10);
                --chip: #12233c;
                --soft: rgba(56,189,248,0.08);
                --resize-line: rgba(56,189,248,0.75);
            }}
            body.light {{
                --bg: #edf4ff;
                --panel: #ffffff;
                --panel-2: #f7fbff;
                --panel-3: #eef5ff;
                --text: #0f172a;
                --muted: #64748b;
                --border: #d7e3f2;
                --accent1: #2563eb;
                --accent2: #06b6d4;
                --accent3: #22c55e;
                --shadow: 0 10px 28px rgba(15, 23, 42, 0.08);
                --table-head: #f6faff;
                --table-head-text: #0f172a;
                --row-even: #f8fbff;
                --good: #dcfce7;
                --warn: #fef3c7;
                --bad: #fee2e2;
                --chip: #eef5ff;
                --soft: rgba(37,99,235,0.06);
                --resize-line: rgba(37,99,235,0.65);
            }}
            * {{ box-sizing: border-box; }}
            html {{ scroll-behavior: smooth; }}
            body {{
                margin: 0;
                background: radial-gradient(circle at top right, rgba(56,189,248,0.10), transparent 23%), var(--bg);
                color: var(--text);
                font-family: "Avenir Next", "Nunito", "Trebuchet MS", "Segoe UI", Arial, sans-serif;
            }}
            .app {{ display: flex; min-height: 100vh; }}
            .sidebar {{
                width: 280px;
                background: linear-gradient(180deg, rgba(8,16,32,0.90), rgba(10,20,38,0.98));
                border-right: 1px solid var(--border);
                padding: 18px 14px;
                position: sticky;
                top: 0;
                height: 100vh;
                overflow-y: auto;
                display:flex;
                flex-direction:column;
            }}
            body.light .sidebar {{ background: linear-gradient(180deg, #ffffff, #f3f8ff); }}
            .sidebar-brand {{
                display: flex;
                align-items: center;
                gap: 10px;
                font-size: 18px;
                font-weight: 900;
                margin-bottom: 18px;
                color: var(--text);
            }}
            .brand-mark {{
                width: 14px;
                height: 14px;
                border-radius: 999px;
                background: linear-gradient(135deg, var(--accent1), var(--accent2), var(--accent3));
                box-shadow: 0 0 0 4px rgba(56,189,248,0.14);
                display: inline-block;
                flex-shrink: 0;
            }}
            .sidebar-group, .sidebar-standalone {{
                display: block;
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 16px;
                margin-bottom: 12px;
                text-decoration: none;
                color: var(--text);
                box-shadow: var(--shadow);
            }}
            .sidebar-group summary, .sidebar-standalone {{
                padding: 12px 14px;
                cursor: pointer;
                font-weight: 900;
                font-size: 15px;
                list-style: none;
                display: flex;
                align-items: center;
                gap: 10px;
            }}
            .sidebar-group summary::-webkit-details-marker {{ display: none; }}
            .sidebar-group summary::after {{
                content: "▾";
                margin-left: auto;
                opacity: .72;
                font-size: 13px;
            }}
            .sidebar-group[open] summary::after {{ transform: rotate(180deg); }}
            .side-emoji {{ width: 22px; text-align: center; font-size: 18px; flex-shrink: 0; }}
            .sidebar-links {{ display: flex; flex-direction: column; gap: 8px; padding: 0 10px 10px; }}
            .sidebar-links a {{
                text-decoration: none;
                color: var(--text);
                padding: 10px 12px;
                border-radius: 12px;
                font-weight: 800;
            }}
            .sidebar-links a:hover, .sidebar-standalone:hover {{ background: var(--soft); }}
            .active-link {{
                background: linear-gradient(90deg, rgba(56,189,248,0.25), rgba(37,99,235,0.25));
                outline: 1px solid rgba(56,189,248,0.3);
            }}
            .subtle-link {{ opacity: 0.84; }}
            .sidebar-bottom {{
                margin-top:auto;
                padding-top:14px;
                display:grid;
                gap:12px;
            }}
            .sidebar-bottom-links {{ display:flex; gap:8px; }}
            .sidebar-user {{
                border:1px solid var(--border);
                background: var(--panel);
                border-radius:16px;
                padding:12px;
                box-shadow: var(--shadow);
                display:grid;
                gap:10px;
            }}
            .sidebar-user-chip {{
                justify-content:center;
                font-size:11px;
                padding:8px 10px;
            }}
            .sidebar-bottom-links .sidebar-standalone {{
                margin-bottom: 0;
                padding: 9px 12px;
                border-radius: 12px;
                font-size: 13px;
                min-width: 0;
                flex: 1;
                justify-content: center;
            }}
            .sidebar-bottom-links .side-emoji {{ width: 18px; font-size: 15px; }}
            .sidebar-mini-actions {{
                display:grid;
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap:8px;
                align-items:start;
            }}
            .minimal-btn {{
                background: transparent;
                color: var(--text);
                padding: 8px 10px;
                border-radius: 10px;
                font-size: 12px;
                font-weight: 800;
            }}
            .logout-btn {{
                color: #ffffff !important;
                border-color: rgba(239,68,68,0.62) !important;
                background: linear-gradient(90deg, rgba(185,28,28,0.96), rgba(239,68,68,0.92)) !important;
            }}
            .logout-btn:hover {{
                background: linear-gradient(90deg, rgba(153,27,27,1), rgba(220,38,38,0.96)) !important;
                color: #fff7f7 !important;
            }}
            .theme-menu {{ position: relative; }}
            .theme-menu > summary {{
                list-style: none;
            }}
            .theme-menu > summary::-webkit-details-marker {{ display:none; }}
            .upload-menu {{ position: relative; }}
            .upload-menu > summary {{
                list-style:none;
            }}
            .upload-menu > summary::-webkit-details-marker {{ display:none; }}
            .upload-menu-list {{
                display:grid;
                gap:12px;
                position:absolute;
                top: calc(100% + 8px);
                right: 0;
                width: 320px;
                max-width: min(320px, calc(100vw - 48px));
                padding: 12px;
                border-radius: 16px;
                border: 1px solid var(--border);
                background: var(--panel);
                box-shadow: var(--shadow);
                z-index: 40;
            }}
            .upload-menu-list form {{
                display:grid;
                gap:10px;
                align-items:end;
            }}
            .upload-menu-list label {{
                display:grid;
                gap:6px;
                font-size:12px;
                font-weight:800;
                color:var(--text);
            }}
            .upload-menu-list input {{
                width:100%;
                min-width:0;
                border-radius:12px;
                border:1px solid var(--border);
                background: var(--panel-3);
                color: var(--text);
                padding: 11px 12px;
                outline:none;
            }}
            .theme-menu-list {{
                display:grid;
                gap:8px;
                margin-top:8px;
                position:absolute;
                bottom: calc(100% + 8px);
                right: 0;
                width: 140px;
                padding: 10px;
                border-radius: 14px;
                border: 1px solid var(--border);
                background: var(--panel);
                box-shadow: var(--shadow);
                z-index: 40;
            }}
            .toggle-indicator {{
                width:38px;
                height:38px;
                padding:0;
                border-radius:12px;
                flex: 0 0 auto;
                position: relative;
            }}
            .toggle-indicator::before {{
                content:"+";
                display:flex;
                align-items:center;
                justify-content:center;
                width:100%;
                height:100%;
                font-size:20px;
                font-weight:900;
                line-height:1;
            }}
            details[open] > summary .toggle-indicator::before {{
                content:"−";
            }}
            .main {{ flex: 1; padding: 22px; overflow-x: hidden; }}
            .topbar {{ display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; flex-wrap: wrap; margin-bottom: 18px; }}
            .page-title {{ font-size: 26px; font-weight: 900; letter-spacing: 0.2px; }}
            .subtitle {{ color: var(--muted); font-size: 13px; margin-top: 6px; }}
            .top-actions {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }}
            .user-chip {{ display:inline-flex; align-items:center; gap:8px; padding:9px 12px; border-radius:999px; border:1px solid var(--border); background:var(--panel-2); font-size:12px; font-weight:900; }}
            .btn, .small-btn, .theme-toggle, .filters button, .filters a, .upload-btn, .ghost-btn {{
                border: 1px solid var(--border);
                background: linear-gradient(90deg, var(--accent2), var(--accent1));
                color: #fff;
                padding: 10px 14px;
                border-radius: 12px;
                cursor: pointer;
                text-decoration: none;
                font-weight: 900;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 8px;
                transition: 0.18s ease;
                min-height: 40px;
                min-width: 104px;
                font-size: 13px;
                line-height: 1;
                box-sizing: border-box;
            }}
            .ghost-btn {{ background: var(--panel-2); color: var(--text); }}
            .small-btn {{ padding: 10px 14px; font-size: 13px; }}
            .btn:hover, .small-btn:hover, .theme-toggle:hover, .filters button:hover, .filters a:hover, .upload-btn:hover, .ghost-btn:hover {{ transform: translateY(-1px); }}
            .panel {{
                background: linear-gradient(180deg, var(--panel), var(--panel-2));
                border: 1px solid var(--border);
                border-radius: 20px;
                box-shadow: var(--shadow);
                padding: 16px;
                margin-bottom: 16px;
            }}
            .panel > summary::-webkit-details-marker {{ display:none; }}
            .compact-panel {{ padding: 14px 16px; }}
            .panel-title {{ font-size: 15px; font-weight: 900; margin-bottom: 12px; }}
            .panel-subtitle {{ color: var(--muted); font-size: 13px; }}
            .toolbar-grid {{ display: grid; grid-template-columns: 1.35fr 1fr; gap: 16px; align-items: start; margin-bottom: 16px; }}
            .upload-form, .filters form {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: end; }}
            .upload-form label, .filters label {{ display: flex; flex-direction: column; gap: 6px; font-size: 12px; font-weight: 800; color: var(--text); }}
            .upload-form input, .filters input, .filters select {{
                min-width: 150px;
                border-radius: 12px;
                border: 1px solid var(--border);
                background: var(--panel-3);
                color: var(--text);
                padding: 11px 12px;
                outline: none;
            }}
            .upload-inline {{ display: flex; flex-wrap: wrap; gap: 10px; align-items: end; }}
            .hint {{ color: var(--muted); font-size: 12px; margin-top: 8px; }}
            .notice {{
                padding: 12px 14px;
                border-radius: 14px;
                border: 1px solid var(--border);
                background: var(--soft);
                margin-bottom: 16px;
                font-size: 13px;
                font-weight: 800;
            }}
            .notice-danger {{
                background: rgba(239,68,68,0.10);
                border-color: rgba(239,68,68,0.25);
            }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(7, minmax(120px, 1fr)); gap: 12px; }}
            .stat-card {{
                background: linear-gradient(180deg, var(--panel), var(--panel-3));
                border: 1px solid var(--border);
                border-radius: 16px;
                padding: 14px;
            }}
            .stat-card .name {{ font-size: 12px; color: var(--muted); margin-bottom: 8px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.4px; }}
            .stat-card .value {{ font-size: 28px; font-weight: 900; line-height: 1.05; }}
            .controls-line {{ display: flex; justify-content: space-between; gap: 10px; align-items: center; flex-wrap: wrap; margin-bottom: 10px; }}
            .table-wrap {{
                overflow: auto;
                border-radius: 18px;
                border: 1px solid var(--border);
                box-shadow: var(--shadow);
                background: var(--panel);
            }}
            table {{ border-collapse: separate; border-spacing: 0; width: 100%; min-width: 1450px; background: var(--panel); color: var(--text); }}
            th, td {{
                border-right: 1px solid var(--border);
                border-bottom: 1px solid var(--border);
                padding: 10px 12px;
                text-align: left;
                font-size: 14px;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                position: relative;
            }}
            th {{
                background: var(--table-head);
                color: var(--table-head-text);
                position: sticky;
                top: 0;
                z-index: 5;
                user-select: none;
            }}
            tbody tr:nth-child(even) {{ background: var(--row-even); }}
            tbody tr.good-row {{ background: var(--good); }}
            tbody tr.warn-row {{ background: var(--warn); }}
            tbody tr.bad-row {{ background: var(--bad); }}
            body.hide-row-colors tbody tr.good-row,
            body.hide-row-colors tbody tr.warn-row,
            body.hide-row-colors tbody tr.bad-row {{ background: transparent !important; }}
            th a {{ color: inherit; text-decoration: none; }}
            .th-inner {{ display: flex; align-items: center; justify-content: space-between; gap: 8px; padding-right: 10px; }}
            .drag-handle {{ cursor: grab; opacity: 0.75; font-size: 12px; }}
            .dragging {{ opacity: 0.45; }}
            .drag-target-left::before, .drag-target-right::after {{
                content: "";
                position: absolute;
                top: 0;
                bottom: 0;
                width: 3px;
                background: var(--resize-line);
                z-index: 8;
            }}
            .drag-target-left::before {{ left: 0; }}
            .drag-target-right::after {{ right: 0; }}
            .resizer {{
                position: absolute;
                top: 0;
                right: 0;
                width: 8px;
                height: 100%;
                cursor: col-resize;
                user-select: none;
                z-index: 9;
            }}
            .column-menu-wrap {{ position: relative; }}
            .column-menu {{
                position: absolute;
                right: 0;
                top: calc(100% + 8px);
                width: 340px;
                max-width: calc(100vw - 40px);
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 18px;
                box-shadow: var(--shadow);
                padding: 14px;
                display: none;
                z-index: 30;
            }}
            .column-menu.open {{ display: block; }}
            .column-actions {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }}
            .column-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; max-height: 300px; overflow: auto; padding-right: 4px; }}
            .column-chip {{
                display: flex;
                align-items: center;
                gap: 8px;
                padding: 9px 10px;
                border: 1px solid var(--border);
                border-radius: 12px;
                background: var(--chip);
                font-size: 13px;
                font-weight: 800;
            }}
            .tree-root details {{ margin-bottom: 10px; border: 1px solid var(--border); border-radius: 16px; overflow: hidden; background: var(--panel); }}
            .tree-root summary {{ cursor: pointer; list-style: none; display: flex; justify-content: space-between; gap: 12px; align-items: center; flex-wrap: wrap; padding: 14px 16px; font-weight: 900; }}
            .tree-root summary::-webkit-details-marker {{ display: none; }}
            .tree-meta {{ color: var(--muted); font-size: 13px; }}
            .tree-line {{ display: grid; grid-template-columns: 2.5fr repeat(7, 1fr); gap: 10px; padding: 10px 16px; border-top: 1px solid var(--border); font-size: 14px; align-items: center; }}
            .tree-level-1 > summary {{ background: rgba(56,189,248,0.09); }}
            .tree-level-2 > summary {{ background: rgba(37,99,235,0.09); }}
            .tree-level-3 > summary {{ background: rgba(34,197,94,0.09); }}
            .tree-level-4 > summary {{ background: rgba(147,51,234,0.09); }}
            .tree-level-5 > summary {{ background: rgba(245,158,11,0.09); }}
            .empty-dev {{ min-height: 58vh; display: flex; align-items: center; justify-content: center; text-align: center; }}
            .empty-dev-card {{ max-width: 540px; padding: 28px; border-radius: 24px; border: 1px solid var(--border); background: linear-gradient(180deg, var(--panel), var(--panel-2)); box-shadow: var(--shadow); }}
            .empty-dev-card .big {{ font-size: 22px; font-weight: 900; margin-bottom: 10px; }}
            .users-layout {{ display:grid; grid-template-columns: minmax(320px, 420px) 1fr; gap:16px; align-items:start; }}
            .users-form {{ display:grid; gap:12px; }}
            .users-form label {{ display:grid; gap:6px; font-size:12px; font-weight:800; }}
            .users-form input, .users-form select {{
                width: 100%;
                border-radius: 12px;
                border: 1px solid var(--border);
                background: var(--panel-3);
                color: var(--text);
                padding: 11px 12px;
                outline: none;
            }}
            .role-grid {{ display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:8px; }}
            .role-option {{
                display:flex;
                gap:8px;
                align-items:flex-start;
                padding:10px 12px;
                border-radius:14px;
                border:1px solid var(--border);
                background: var(--chip);
                font-size:13px;
                font-weight:800;
            }}
            .role-option input {{ margin-top:2px; }}
            .users-table {{ min-width: 980px; }}
            .status-dot {{
                width:10px;
                height:10px;
                border-radius:999px;
                display:inline-block;
                background:#22c55e;
                box-shadow:0 0 0 4px rgba(34,197,94,0.12);
            }}
            .status-dot.off {{
                background:#ef4444;
                box-shadow:0 0 0 4px rgba(239,68,68,0.12);
            }}
            .caps-layout {{ display:grid; grid-template-columns: minmax(300px, 360px) minmax(0, 1fr); gap:16px; align-items:start; }}
            .caps-layout > div {{ min-width: 0; }}
            .caps-form {{ display:grid; gap:12px; }}
            .caps-form label {{ display:grid; gap:6px; font-size:12px; font-weight:800; }}
            .caps-form input, .caps-form textarea, .caps-form select {{
                width:100%;
                border-radius:12px;
                border:1px solid var(--border);
                background: var(--panel-3);
                color: var(--text);
                padding:11px 12px;
                outline:none;
            }}
            .caps-form textarea {{ min-height: 110px; resize: vertical; }}
            .caps-grid-2 {{ display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:10px; }}
            .caps-table {{ min-width: 1180px; table-layout: fixed; width: 100%; }}
            .caps-table th, .caps-table td {{
                white-space: normal;
                overflow: visible;
                text-overflow: initial;
                vertical-align: top;
            }}
            .caps-table .comment-col {{ min-width: 180px; max-width: 220px; }}
            .caps-table .agent-col {{ min-width: 110px; }}
            .caps-actions {{ display:flex; gap:8px; flex-wrap:wrap; min-width:110px; }}
            .progress-shell {{
                min-width: 130px;
                display:grid;
                gap:6px;
            }}
            .finance-grid {{ display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:16px; }}
            .finance-table {{ min-width: 980px; }}
            .finance-table th, .finance-table td {{
                white-space: normal;
                overflow: visible;
                text-overflow: initial;
                vertical-align: top;
            }}
            .wallet-code {{ font-family: "Menlo", "Monaco", monospace; font-size: 12px; line-height: 1.45; }}
            .progress-bar {{
                width:100%;
                height:10px;
                border-radius:999px;
                background: rgba(255,255,255,0.08);
                overflow:hidden;
            }}
            .progress-bar > span {{
                display:block;
                height:100%;
                border-radius:999px;
                background: linear-gradient(90deg, var(--accent3), var(--accent1));
            }}
            .tasks-layout {{ display:grid; grid-template-columns: minmax(360px, 430px) 1fr; gap:16px; align-items:start; }}
            .tasks-form {{ display:grid; gap:12px; }}
            .tasks-form label {{ display:grid; gap:6px; font-size:12px; font-weight:800; }}
            .tasks-form input, .tasks-form textarea, .tasks-form select {{
                width:100%;
                border-radius:12px;
                border:1px solid var(--border);
                background: var(--panel-3);
                color: var(--text);
                padding:11px 12px;
                outline:none;
            }}
            .tasks-form textarea {{ min-height: 110px; resize: vertical; }}
            .datetime-grid {{ display:grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap:10px; }}
            .task-stack {{ display:grid; gap:14px; }}
            .task-card {{
                border:1px solid var(--border);
                border-radius:18px;
                background: linear-gradient(180deg, var(--panel), var(--panel-2));
                box-shadow: var(--shadow);
                padding:16px;
            }}
            .task-head {{
                display:flex;
                justify-content:space-between;
                gap:12px;
                align-items:flex-start;
                flex-wrap:wrap;
                margin-bottom:10px;
            }}
            .task-title {{ font-size:18px; font-weight:900; margin-bottom:6px; }}
            .task-meta {{ display:flex; gap:8px; flex-wrap:wrap; }}
            .task-chip {{
                display:inline-flex;
                align-items:center;
                gap:6px;
                border-radius:999px;
                border:1px solid var(--border);
                background: var(--chip);
                padding:7px 11px;
                font-size:12px;
                font-weight:900;
            }}
            .task-body {{
                display:grid;
                grid-template-columns: 1.15fr .85fr;
                gap:14px;
                margin-top:10px;
            }}
            .task-note {{
                border:1px solid var(--border);
                background: rgba(255,255,255,0.02);
                border-radius:14px;
                padding:12px 14px;
                white-space:pre-wrap;
                line-height:1.45;
            }}
            .task-answer-empty {{
                color: var(--muted);
                font-weight: 800;
            }}
            .muted {{ color: var(--muted); }}
            @media (max-width: 1200px) {{
                .stats-grid {{ grid-template-columns: repeat(3, minmax(130px, 1fr)); }}
                .toolbar-grid {{ grid-template-columns: 1fr; }}
                .users-layout {{ grid-template-columns: 1fr; }}
                .caps-layout {{ grid-template-columns: 1fr; }}
                .tasks-layout {{ grid-template-columns: 1fr; }}
                .task-body {{ grid-template-columns: 1fr; }}
                .finance-grid {{ grid-template-columns: 1fr; }}
                .datetime-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
            }}
            @media (max-width: 900px) {{
                .app {{ display: block; }}
                .sidebar {{ width: 100%; height: auto; position: relative; }}
                .main {{ padding: 14px; }}
                .page-title {{ font-size: 22px; }}
                .stats-grid {{ grid-template-columns: repeat(2, minmax(130px, 1fr)); }}
                .column-grid {{ grid-template-columns: 1fr; }}
            }}
        </style>
    </head>
    <body>
        <div class="app">
            {sidebar}
            <main class="main">
                <div class="topbar">
                    <div>
                        <div class="page-title">{escape(title)}</div>
                        <div class="subtitle">TeamBead CRM System</div>
                    </div>
                    <div class="top-actions">
                        {top_actions}
                    </div>
                </div>
                {content}
            </main>
        </div>
        <script>
            function setTheme(mode) {{
                if (mode === 'light') document.body.classList.add('light');
                else document.body.classList.remove('light');
                localStorage.setItem('teambead-theme', mode === 'light' ? 'light' : 'dark');
            }}
            (function initTheme() {{
                const saved = localStorage.getItem('teambead-theme');
                if (saved === 'light') document.body.classList.add('light');
            }})();
            document.addEventListener('click', function(e) {{
                const wrap = document.querySelector('.column-menu-wrap');
                const menu = document.getElementById('columnMenu');
                if (!wrap || !menu) return;
                if (!wrap.contains(e.target)) menu.classList.remove('open');
                document.querySelectorAll('.theme-menu').forEach(function(item) {{
                    if (!item.contains(e.target)) item.removeAttribute('open');
                }});
                document.querySelectorAll('.upload-menu').forEach(function(item) {{
                    if (!item.contains(e.target)) item.removeAttribute('open');
                }});
            }});
        </script>
        {extra_scripts}
    </body>
    </html>
    """



def get_row_class(row):
    if (row.get("ftd") or 0) > 0:
        return "good-row"
    if (row.get("reg") or 0) > 0:
        return "warn-row"
    return "bad-row"



def sort_link(label, field, current_sort, current_order, **params):
    next_order = "asc" if current_sort != field or current_order == "desc" else "desc"
    qs = build_query_string(sort_by=field, order=next_order, **params)
    arrow = ""
    if current_sort == field:
        arrow = " ↑" if current_order == "asc" else " ↓"
    return f'<a href="?{qs}">{escape(label)}{arrow}</a>'



def render_stats_cards(totals):
    cards = [
        ("Spend", format_money(totals["spend"])),
        ("Leads", format_int_or_float(totals["leads"])),
        ("Reg", format_int_or_float(totals["reg"])),
        ("FTD", format_int_or_float(totals["ftd"])),
        ("CPA", format_money(totals["cpa_real"])),
        ("L2FTD", format_percent(totals["l2ftd"])),
        ("R2D", format_percent(totals["r2d"])),
    ]
    html = '<div class="panel compact-panel"><div class="stats-grid">'
    for title, value in cards:
        html += f'<div class="stat-card"><div class="name">{title}</div><div class="value">{value}</div></div>'
    html += '</div></div>'
    return html


def render_statistic_cards(totals):
    cards = [
        ("Spend", format_money(totals["spend"])),
        ("Chatterfy", format_int_or_float(totals.get("stat_chatterfy", 0))),
        ("Income", format_money(totals.get("stat_income", 0))),
        ("Profit", format_money(totals.get("stat_profit", 0))),
        ("ROI", format_percent(totals.get("stat_roi", 0))),
        ("FB FTD", format_int_or_float(totals["ftd"])),
        ("Total FTD", format_int_or_float(totals.get("stat_total_ftd", 0))),
        ("Qual FTD", format_int_or_float(totals.get("stat_qual_ftd", 0))),
    ]
    html = '<div class="panel compact-panel"><div class="stats-grid">'
    for title, value in cards:
        html += f'<div class="stat-card"><div class="name">{title}</div><div class="value">{value}</div></div>'
    html += '</div></div>'
    return html



def render_tree_nodes(nodes, level=1):
    html = ""
    level_class = f"tree-level-{min(level, 5)}"
    for node in nodes:
        m = node["metrics"]
        meta = f'<span class="tree-meta">Spend: {format_money(m["spend"])} · Chatterfy: {format_int_or_float(m.get("stat_chatterfy", 0))} · Income: {format_money(m.get("stat_income", 0))} · Profit: {format_money(m.get("stat_profit", 0))} · ROI: {format_percent(m.get("stat_roi", 0))} · Total FTD: {format_int_or_float(m.get("stat_total_ftd", 0))} · Qual FTD: {format_int_or_float(m.get("stat_qual_ftd", 0))}</span>'
        if node["children"]:
            children_html = render_tree_nodes(node["children"], level + 1)
            html += f'''
            <details class="{level_class}" open>
                <summary><span>{escape(node["name"])} <span class="muted">({escape(node["key"])})</span></span>{meta}</summary>
                {children_html}
            </details>
            '''
        else:
            html += f'''
            <div class="tree-line {level_class}">
                <div><strong>{escape(node["name"])}</strong> <span class="muted">({escape(node["key"])})</span></div>
                <div>{format_money(m["spend"])}</div>
                <div>{format_int_or_float(m.get("stat_chatterfy", 0))}</div>
                <div>{format_money(m.get("stat_income", 0))}</div>
                <div>{format_money(m.get("stat_profit", 0))}</div>
                <div>{format_percent(m.get("stat_roi", 0))}</div>
                <div>{format_int_or_float(m.get("stat_total_ftd", 0))}</div>
                <div>{format_int_or_float(m.get("stat_qual_ftd", 0))}</div>
            </div>
            '''
    return html


def load_users():
    db = SessionLocal()
    try:
        return db.query(User).order_by(User.username.asc()).all()
    finally:
        db.close()


def users_page_html(current_user, error_text="", success_text="", form_data=None):
    users = load_users()
    form_data = form_data or {}
    role_value = (form_data.get("role") or "buyer").strip() or "buyer"
    active_checked = "checked" if str(form_data.get("is_active", "1")) == "1" else ""
    current_edit_id = str(form_data.get("edit_user_id") or "")
    role_options = [
        ("superadmin", "Founder"),
        ("admin", "Admin"),
        ("buyer", "Buyer"),
        ("operator", "Operator"),
        ("finance", "Finance"),
    ]
    role_html = "".join([
        f'<option value="{value}" {"selected" if role_value == value else ""}>{label}</option>'
        for value, label in role_options
    ])

    rows_html = ""
    for item in users:
        delete_form = ""
        if item.username != (current_user or {}).get("username"):
            delete_form = f"""
            <form method="post" action="/users/delete" onsubmit="return confirm('Удалить пользователя?');">
                <input type="hidden" name="user_id" value="{item.id}">
                <button type="submit" class="ghost-btn small-btn">Delete</button>
            </form>
            """
        rows_html += f"""
        <tr>
            <td>{display_user_id(item.id)}</td>
            <td>{escape(item.display_name or item.username)}</td>
            <td>{escape(item.username)}</td>
            <td>{escape(role_label(item.role or "buyer"))}</td>
            <td>{escape(item.buyer_name or "—")}</td>
            <td><span class="status-dot {'off' if not item.is_active else ''}"></span> {'Active' if item.is_active else 'Disabled'}</td>
            <td>
                <div class="caps-actions">
                    <form method="get" action="/users">
                        <input type="hidden" name="edit" value="{item.id}">
                        <button type="submit" class="ghost-btn small-btn">Edit</button>
                    </form>
                    {delete_form}
                </div>
            </td>
        </tr>
        """

    mode_title = "Редактирование пользователя" if current_edit_id else "Новый пользователь"
    submit_label = "Save Changes" if current_edit_id else "Create User"
    password_hint = "Оставь пустым, если пароль менять не нужно." if current_edit_id else "Пароль"
    message_html = ""
    if error_text:
        message_html += f'<div class="notice notice-danger">{escape(error_text)}</div>'
    if success_text:
        message_html += f'<div class="notice">{escape(success_text)}</div>'

    create_panel = f"""
    <details class="panel" {'open' if current_edit_id else ''}>
        <summary class="panel-title" style="cursor:pointer; list-style:none; display:flex; align-items:center; justify-content:space-between;">
            <span>{'Edit User' if current_edit_id else 'Add User'}</span>
            <span class="btn toggle-indicator"></span>
        </summary>
        <form method="post" action="/users/save" class="users-form" style="margin-top:14px;">
            <input type="hidden" name="edit_user_id" value="{escape(current_edit_id)}">
            <label>Name
                <input type="text" name="display_name" value="{escape(form_data.get('display_name', ''))}" required placeholder="Ivan">
            </label>
            <label>Login
                <input type="text" name="username" value="{escape(form_data.get('username', ''))}" required placeholder="ivan">
            </label>
            <label>Password
                <input type="text" name="password" value="" placeholder="{escape(password_hint)}">
            </label>
            <label>Buyer binding
                <input type="text" name="buyer_name" value="{escape(form_data.get('buyer_name', ''))}" placeholder="TeamBead1">
            </label>
            <label>Role
                <select name="role">{role_html}</select>
            </label>
            <label class="role-option">
                <input type="checkbox" name="is_active" value="1" {active_checked}>
                <span><strong>Active</strong><br><span class="muted">Пользователь может войти.</span></span>
            </label>
            <div style="display:flex; gap:10px; flex-wrap:wrap;">
                <button type="submit" class="btn">{submit_label}</button>
                <a href="/users" class="ghost-btn">Reset</a>
            </div>
        </form>
    </details>
    """

    content = f"""
    {message_html}
    <div class="users-layout">
        <div>{create_panel}</div>

        <div class="panel">
            <div class="controls-line">
                <div>
                    <div class="panel-title" style="margin-bottom:4px;">Users</div>
                    <div class="panel-subtitle">Founder, team members and access control.</div>
                </div>
            </div>
            <div class="table-wrap">
                <table class="users-table">
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Name</th>
                            <th>Login</th>
                            <th>Role</th>
                            <th>Buyer</th>
                            <th>Status</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>{rows_html}</tbody>
                </table>
            </div>
        </div>
    </div>
    """
    return page_shell("Users", content, active_page="users", current_user=current_user)


def caps_page_html(current_user, rows, filter_values=None, form_data=None, success_text="", error_text=""):
    filter_values = filter_values or {}
    form_data = form_data or {}
    buyers, geos, owners = get_caps_filter_options()
    buyer_options = make_options(buyers, filter_values.get("buyer", ""))
    geo_options = make_options(geos, filter_values.get("geo", ""))
    owner_options = make_options(owners, filter_values.get("owner_name", ""))

    total_cap = sum(safe_number(row.cap_value) for row in rows)
    total_current = sum(safe_number(row.current_ftd) for row in rows)
    fill_avg = cap_fill_percent(total_current, total_cap)
    active_caps = len([row for row in rows if safe_number(row.cap_value) > 0])

    rows_html = ""
    for row in rows:
        fill_percent = cap_fill_percent(row.current_ftd, row.cap_value)
        bar_width = max(0, min(100, fill_percent))
        state = "OK"
        if fill_percent >= 100:
            state = "FULL"
        elif fill_percent >= 80:
            state = "HOT"
        rows_html += f"""
        <tr>
            <td>{row.id}</td>
            <td>{escape(row.advertiser or "")}</td>
            <td>{escape(row.owner_name or "")}</td>
            <td>{escape(row.buyer or "")}</td>
            <td>{escape(row.flow or "")}</td>
            <td>{escape(row.code or "")}</td>
            <td>{escape(row.geo or "")}</td>
            <td>{escape(row.rate or "")}</td>
            <td>{escape(row.baseline or "")}</td>
            <td>{format_int_or_float(row.cap_value)}</td>
            <td>{format_int_or_float(row.current_ftd)}</td>
            <td>
                <div class="progress-shell">
                    <div><strong>{fill_percent:.0f}%</strong> · {state}</div>
                    <div class="progress-bar"><span style="width:{bar_width}%;"></span></div>
                </div>
            </td>
            <td>{escape(row.promo_code or "")}</td>
            <td class="agent-col">{escape(row.agent or "")}</td>
            <td class="comment-col">{escape(row.comments or "")}</td>
            <td>
                <div class="caps-actions">
                    <form method="get" action="/caps">
                        <input type="hidden" name="edit" value="{row.id}">
                        <button type="submit" class="ghost-btn small-btn">Edit</button>
                    </form>
                    <form method="post" action="/caps/delete" onsubmit="return confirm('Удалить эту капу?');">
                        <input type="hidden" name="cap_id" value="{row.id}">
                        <button type="submit" class="ghost-btn small-btn">Delete</button>
                    </form>
                </div>
            </td>
        </tr>
        """

    message_html = ""
    if success_text:
        message_html += f'<div class="notice">{escape(success_text)}</div>'
    if error_text:
        message_html += f'<div class="notice notice-danger">{escape(error_text)}</div>'

    current_edit_id = str(form_data.get("edit_id") or "")
    form_title = "Edit Cap" if current_edit_id else "Add Cap"
    submit_label = "Save" if current_edit_id else "Add Cap"
    create_panel = f"""
    <details class="panel" {'open' if current_edit_id else ''}>
        <summary class="panel-title" style="cursor:pointer; list-style:none; display:flex; align-items:center; justify-content:space-between;">
            <span>{form_title}</span>
            <span class="btn toggle-indicator"></span>
        </summary>
        <div class="panel-subtitle" style="margin-top:10px;">Advertiser caps and manual updates.</div>
        <form method="post" action="/caps/upload" enctype="multipart/form-data" class="caps-form" style="margin-top:14px; margin-bottom:14px;">
            <label>Upload caps CSV / XLSX
                <input type="file" name="file" accept=".csv,.xlsx,.xls" required>
            </label>
            <button type="submit" class="ghost-btn">Upload</button>
        </form>
        <form method="post" action="/caps/save" class="caps-form" style="margin-top:14px;">
            <input type="hidden" name="edit_id" value="{escape(current_edit_id)}">
            <div class="caps-grid-2">
                <label>Рекл
                    <input type="text" name="advertiser" value="{escape(form_data.get('advertiser', ''))}">
                </label>
                <label>Имя
                    <input type="text" name="owner_name" value="{escape(form_data.get('owner_name', ''))}">
                </label>
            </div>
            <div class="caps-grid-2">
                <label>Кабинет
                    <input type="text" name="buyer" value="{escape(form_data.get('buyer', ''))}" required>
                </label>
                <label>Поток
                    <input type="text" name="flow" value="{escape(form_data.get('flow', ''))}">
                </label>
            </div>
            <div class="caps-grid-2">
                <label>CODE
                    <input type="text" name="code" value="{escape(form_data.get('code', ''))}">
                </label>
                <label>GEO
                    <input type="text" name="geo" value="{escape(form_data.get('geo', ''))}">
                </label>
            </div>
            <div class="caps-grid-2">
                <label>Ставка
                    <input type="text" name="rate" value="{escape(form_data.get('rate', ''))}">
                </label>
                <label>БЛ
                    <input type="text" name="baseline" value="{escape(form_data.get('baseline', ''))}">
                </label>
            </div>
            <div class="caps-grid-2">
                <label>Капа
                    <input type="number" step="0.01" name="cap_value" value="{escape(form_data.get('cap_value', ''))}" required>
                </label>
                <label>Current FTD
                    <input type="number" step="0.01" name="current_ftd" value="{escape(form_data.get('current_ftd', '0'))}">
                </label>
            </div>
            <div class="caps-grid-2">
                <label>Промокод
                    <input type="text" name="promo_code" value="{escape(form_data.get('promo_code', ''))}">
                </label>
                <label>Агент
                    <input type="text" name="agent" value="{escape(form_data.get('agent', ''))}">
                </label>
            </div>
            <label>Ссылка
                <input type="text" name="link" value="{escape(form_data.get('link', ''))}">
            </label>
            <label>КПИ
                <textarea name="kpi">{escape(form_data.get('kpi', ''))}</textarea>
            </label>
            <label>Комментарии
                <textarea name="comments">{escape(form_data.get('comments', ''))}</textarea>
            </label>
            <div style="display:flex; gap:10px; flex-wrap:wrap;">
                <button type="submit" class="btn">{submit_label}</button>
                <a href="/caps" class="ghost-btn">Reset</a>
            </div>
        </form>
    </details>
    """

    content = f"""
    {message_html}
    <div class="caps-layout">
        <div>{create_panel}</div>

        <div>
            <div class="panel compact-panel filters">
                <div class="panel-title">Filters</div>
                <form method="get" action="/caps">
                    <label>Buyer<select name="buyer">{buyer_options}</select></label>
                    <label>Geo<select name="geo">{geo_options}</select></label>
                    <label>Имя<select name="owner_name">{owner_options}</select></label>
                    <label>Search<input type="text" name="search" value="{escape(filter_values.get('search', ''))}" placeholder="Search caps"></label>
                    <button type="submit" class="btn small-btn">Filter</button>
                    <a href="/caps" class="ghost-btn small-btn">Reset</a>
                </form>
            </div>

            <div class="panel compact-panel">
                <div class="stats-grid">
                    <div class="stat-card"><div class="name">Caps</div><div class="value">{active_caps}</div></div>
                    <div class="stat-card"><div class="name">Cap Total</div><div class="value">{format_int_or_float(total_cap)}</div></div>
                    <div class="stat-card"><div class="name">Current FTD</div><div class="value">{format_int_or_float(total_current)}</div></div>
                    <div class="stat-card"><div class="name">Fill Avg</div><div class="value">{fill_avg:.0f}%</div></div>
                </div>
            </div>

            <div class="panel compact-panel">
                <div class="controls-line">
                    <div>
                        <div class="panel-title" style="margin-bottom:4px;">Caps Table</div>
                        <div class="panel-subtitle">Advertiser caps, promo codes and current load.</div>
                    </div>
                </div>
                <div class="table-wrap">
                    <table class="caps-table">
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Рекл</th>
                                <th>Имя</th>
                                <th>Кабинет</th>
                                <th>Поток</th>
                                <th>CODE</th>
                                <th>GEO</th>
                                <th>Ставка</th>
                                <th>БЛ</th>
                                <th>Капа</th>
                                <th>Current FTD</th>
                                <th>Fill</th>
                                <th>Промокод</th>
                                <th>Агент</th>
                                <th>Комментарии</th>
                                <th>Action</th>
                            </tr>
                        </thead>
                        <tbody>{rows_html if rows_html else '<tr><td colspan="16">Нет кап</td></tr>'}</tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
    """
    return page_shell("Caps", content, active_page="caps", current_user=current_user)


def render_finance_table(title, subtitle, headers, rows_html, min_width="980px"):
    return f"""
    <div class="panel compact-panel">
        <div class="controls-line">
            <div>
                <div class="panel-title" style="margin-bottom:4px;">{escape(title)}</div>
                <div class="panel-subtitle">{escape(subtitle)}</div>
            </div>
        </div>
        <div class="table-wrap">
            <table class="finance-table" style="min-width:{escape(min_width)};">
                <thead><tr>{headers}</tr></thead>
                <tbody>{rows_html if rows_html else '<tr><td colspan="6">Нет данных</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    """


def finance_page_html(current_user, success_text="", error_text="", form_data=None):
    snapshot = load_finance_snapshot()
    manual = load_manual_finance()
    form_data = form_data or {}

    wallet_rows = ""
    for item in snapshot["wallets"]:
        wallet_rows += f"""
        <tr>
            <td>{escape(item['category'])}</td>
            <td>{escape(item['description'])}</td>
            <td>{escape(item['owner'])}</td>
            <td class="wallet-code">{escape(item['wallet'])}</td>
            <td>{format_money(item['amount'])}</td>
            <td>CSV</td>
            <td></td>
        </tr>
        """

    for item in manual["wallets"]:
        wallet_rows += f"""
        <tr>
            <td>{escape(item.category or "")}</td>
            <td>{escape(item.description or "")}</td>
            <td>{escape(item.owner_name or "")}</td>
            <td class="wallet-code">{escape(item.wallet or "")}</td>
            <td>{format_money(item.amount)}</td>
            <td>Manual</td>
            <td>
                <div class="caps-actions">
                    <form method="get" action="/finance">
                        <input type="hidden" name="edit_wallet" value="{item.id}">
                        <button type="submit" class="ghost-btn small-btn">Edit</button>
                    </form>
                    <form method="post" action="/finance/wallets/delete" onsubmit="return confirm('Удалить кошелек?');">
                        <input type="hidden" name="wallet_id" value="{item.id}">
                        <button type="submit" class="ghost-btn small-btn">Delete</button>
                    </form>
                </div>
            </td>
        </tr>
        """

    manual_expense_rows = ""
    operation_rows = ""
    for item in manual["expenses"]:
        manual_expense_rows += f"""
        <tr>
            <td>{item.id}</td>
            <td>{escape(item.expense_date or "")}</td>
            <td>{escape(item.category or "")}</td>
            <td>{format_money(item.amount)}</td>
            <td>{escape(item.paid_by or "")}</td>
            <td>{escape(item.comment or "")}</td>
            <td>
                <div class="caps-actions">
                    <form method="get" action="/finance">
                        <input type="hidden" name="edit_expense" value="{item.id}">
                        <button type="submit" class="ghost-btn small-btn">Edit</button>
                    </form>
                    <form method="post" action="/finance/expenses/delete" onsubmit="return confirm('Удалить расход?');">
                        <input type="hidden" name="expense_id" value="{item.id}">
                        <button type="submit" class="ghost-btn small-btn">Delete</button>
                    </form>
                </div>
            </td>
        </tr>
        """
        operation_rows += f"""
        <tr>
            <td>{item.id}</td>
            <td>Expense</td>
            <td>{escape(item.expense_date or "")}</td>
            <td>{escape(item.category or "")}</td>
            <td>{escape(item.paid_by or "")}</td>
            <td>{format_money(item.amount)}</td>
            <td>{escape(item.comment or "")}</td>
            <td>
                <div class="caps-actions">
                    <form method="get" action="/finance">
                        <input type="hidden" name="edit_expense" value="{item.id}">
                        <button type="submit" class="ghost-btn small-btn">Edit</button>
                    </form>
                    <form method="post" action="/finance/expenses/delete" onsubmit="return confirm('Удалить расход?');">
                        <input type="hidden" name="expense_id" value="{item.id}">
                        <button type="submit" class="ghost-btn small-btn">Delete</button>
                    </form>
                </div>
            </td>
        </tr>
        """

    manual_income_rows = ""
    for item in manual["income"]:
        manual_income_rows += f"""
        <tr>
            <td>{item.id}</td>
            <td>{escape(item.income_date or "")}</td>
            <td>{escape(item.category or "")}</td>
            <td>{escape(item.description or "")}</td>
            <td>{format_money(item.amount)}</td>
            <td class="wallet-code">{escape(item.wallet or "")}</td>
            <td>{escape(item.reconciliation or "")}</td>
            <td>
                <div class="caps-actions">
                    <form method="get" action="/finance">
                        <input type="hidden" name="edit_income" value="{item.id}">
                        <button type="submit" class="ghost-btn small-btn">Edit</button>
                    </form>
                    <form method="post" action="/finance/income/delete" onsubmit="return confirm('Удалить приход?');">
                        <input type="hidden" name="income_id" value="{item.id}">
                        <button type="submit" class="ghost-btn small-btn">Delete</button>
                    </form>
                </div>
            </td>
        </tr>
        """
        operation_rows += f"""
        <tr>
            <td>{item.id}</td>
            <td>Income</td>
            <td>{escape(item.income_date or "")}</td>
            <td>{escape(item.category or "")}</td>
            <td>{escape(item.wallet or "")}</td>
            <td>{format_money(item.amount)}</td>
            <td>{escape((item.description or "") + (f" · {item.reconciliation}" if item.reconciliation else ""))}</td>
            <td>
                <div class="caps-actions">
                    <form method="get" action="/finance">
                        <input type="hidden" name="edit_income" value="{item.id}">
                        <button type="submit" class="ghost-btn small-btn">Edit</button>
                    </form>
                    <form method="post" action="/finance/income/delete" onsubmit="return confirm('Удалить приход?');">
                        <input type="hidden" name="income_id" value="{item.id}">
                        <button type="submit" class="ghost-btn small-btn">Delete</button>
                    </form>
                </div>
            </td>
        </tr>
        """

    manual_wallet_total = sum(safe_number(item.amount) for item in manual["wallets"])
    manual_expense_total = sum(safe_number(item.amount) for item in manual["expenses"])
    manual_income_total = sum(safe_number(item.amount) for item in manual["income"])
    wallet_submit_label = "Save Changes" if form_data.get("wallet_edit_id") else "Save Wallet"
    expense_submit_label = "Save Changes" if form_data.get("expense_edit_id") else "Save Expense"
    income_submit_label = "Save Changes" if form_data.get("income_edit_id") else "Save Income"
    message_html = ""
    if success_text:
        message_html += f'<div class="notice">{escape(success_text)}</div>'
    if error_text:
        message_html += f'<div class="notice notice-danger">{escape(error_text)}</div>'
    create_panel = f"""
    <details class="panel" {'open' if form_data else ''}>
        <summary class="panel-title" style="cursor:pointer; list-style:none; display:flex; align-items:center; justify-content:space-between;">
            <span>Manage Finance</span>
            <span class="btn toggle-indicator"></span>
        </summary>
        <div class="panel-subtitle" style="margin-top:10px;">Manual wallets, expenses and income.</div>
        <div class="finance-grid" style="margin-top:14px;">
            <div class="panel">
                <div class="panel-title">{'Edit Wallet' if form_data.get('wallet_edit_id') else 'Add Wallet'}</div>
                <form method="post" action="/finance/wallets/save" class="caps-form" style="margin-top:14px;">
                    <input type="hidden" name="edit_id" value="{escape(form_data.get('wallet_edit_id', ''))}">
                    <label>Категория<input type="text" name="category" value="{escape(form_data.get('wallet_category', ''))}" placeholder="Binance"></label>
                    <label>Описание<input type="text" name="description" value="{escape(form_data.get('wallet_description', ''))}" placeholder="Банк"></label>
                    <label>Метка<input type="text" name="owner_name" value="{escape(form_data.get('wallet_owner_name', ''))}" placeholder="Ivan"></label>
                    <label>Кошелек<input type="text" name="wallet" value="{escape(form_data.get('wallet_wallet', ''))}" placeholder="Адрес кошелька"></label>
                    <label>Сумма<input type="number" step="0.01" name="amount" value="{escape(form_data.get('wallet_amount', ''))}" placeholder="0.00"></label>
                    <div style="display:flex; gap:10px; flex-wrap:wrap;">
                        <button type="submit" class="btn">{wallet_submit_label}</button>
                        <a href="/finance" class="ghost-btn">Reset</a>
                    </div>
                </form>
            </div>
            <div class="panel">
                <div class="panel-title">{'Edit Expense' if form_data.get('expense_edit_id') else 'Add Expense'}</div>
                <form method="post" action="/finance/expenses/save" class="caps-form" style="margin-top:14px;">
                    <input type="hidden" name="edit_id" value="{escape(form_data.get('expense_edit_id', ''))}">
                    <label>Дата<input type="text" name="expense_date" value="{escape(form_data.get('expense_date', ''))}" placeholder="26.03"></label>
                    <label>Категория<input type="text" name="category" value="{escape(form_data.get('expense_category', ''))}" placeholder="Сервисы"></label>
                    <label>Сумма<input type="number" step="0.01" name="amount" value="{escape(form_data.get('expense_amount', ''))}" placeholder="0.00"></label>
                    <label>Кто оплатил<input type="text" name="paid_by" value="{escape(form_data.get('expense_paid_by', ''))}" placeholder="Кошелек или человек"></label>
                    <label>Комментарий<textarea name="comment">{escape(form_data.get('expense_comment', ''))}</textarea></label>
                    <div style="display:flex; gap:10px; flex-wrap:wrap;">
                        <button type="submit" class="btn">{expense_submit_label}</button>
                        <a href="/finance" class="ghost-btn">Reset</a>
                    </div>
                </form>
            </div>
        </div>
        <div class="panel" style="margin-top:16px;">
            <div class="panel-title">{'Edit Income' if form_data.get('income_edit_id') else 'Add Income'}</div>
            <form method="post" action="/finance/income/save" class="caps-form" style="margin-top:14px;">
                <input type="hidden" name="edit_id" value="{escape(form_data.get('income_edit_id', ''))}">
                <div class="caps-grid-2">
                    <label>Дата<input type="text" name="income_date" value="{escape(form_data.get('income_date', ''))}" placeholder="26.03"></label>
                    <label>Категория<input type="text" name="category" value="{escape(form_data.get('income_category', ''))}" placeholder="TeamBead"></label>
                </div>
                <div class="caps-grid-2">
                    <label>Описание<input type="text" name="description" value="{escape(form_data.get('income_description', ''))}" placeholder="PE, CO"></label>
                    <label>Сумма<input type="number" step="0.01" name="amount" value="{escape(form_data.get('income_amount', ''))}" placeholder="0.00"></label>
                </div>
                <div class="caps-grid-2">
                    <label>Кошелек<input type="text" name="wallet" value="{escape(form_data.get('income_wallet', ''))}" placeholder="Куда пришло"></label>
                    <label>Сверка<input type="text" name="reconciliation" value="{escape(form_data.get('income_reconciliation', ''))}" placeholder="OK / pending"></label>
                </div>
                <div style="display:flex; gap:10px; flex-wrap:wrap;">
                    <button type="submit" class="btn">{income_submit_label}</button>
                    <a href="/finance" class="ghost-btn">Reset</a>
                </div>
            </form>
        </div>
    </details>
    """

    content = f"""
    {message_html}
    <div class="panel compact-panel">
        <div class="panel-title">Finance</div>
        <div class="panel-subtitle">Current wallets and manual finance records.</div>
    </div>

    <div class="panel compact-panel">
        <div class="stats-grid">
            <div class="stat-card"><div class="name">Wallets</div><div class="value">{format_money(snapshot['totals']['wallets'] + manual_wallet_total)}</div></div>
            <div class="stat-card"><div class="name">CSV Wallets</div><div class="value">{format_money(snapshot['totals']['wallets'])}</div></div>
            <div class="stat-card"><div class="name">Manual Wallets</div><div class="value">{format_money(manual_wallet_total)}</div></div>
            <div class="stat-card"><div class="name">Manual Expenses</div><div class="value">{format_money(manual_expense_total)}</div></div>
            <div class="stat-card"><div class="name">Manual Income</div><div class="value">{format_money(manual_income_total)}</div></div>
        </div>
    </div>

    {create_panel}

    {render_finance_table("Wallets", "Current wallets in one list", '<th>Категория</th><th>Описание</th><th>Метка</th><th>Кошелек</th><th>Сумма</th><th>Source</th><th>Action</th>', wallet_rows, "1320px")}

    {render_finance_table("Operations", "Manual income and expenses in one list", '<th>ID</th><th>Type</th><th>Дата</th><th>Категория</th><th>Кошелек / Кто оплатил</th><th>Сумма</th><th>Комментарий</th><th>Action</th>', operation_rows, "1360px")}
    """
    return page_shell("Finance", content, active_page="finance", current_user=current_user)


def tasks_page_html(current_user, rows, filter_values=None, form_data=None, success_text="", error_text=""):
    filter_values = filter_values or {}
    form_data = form_data or {}
    status_options = get_task_status_options()
    assignable_users = get_assignable_users()

    my_open = len([row for row in rows if row.status != "Выполнено"])
    answered = len([row for row in rows if (row.response_text or "").strip()])
    overdue = len([row for row in rows if row.due_at and row.due_at < datetime.utcnow() and row.status != "Выполнено"])

    message_html = ""
    if success_text:
        message_html += f'<div class="notice">{escape(success_text)}</div>'
    if error_text:
        message_html += f'<div class="notice notice-danger">{escape(error_text)}</div>'

    status_options_html = make_options(status_options, filter_values.get("status", ""))
    assignee_options_html = make_options(
        [f"{item.username}|||{item.display_name or item.username}|||{item.role or ''}" for item in assignable_users],
        filter_values.get("assignee", ""),
    )
    assignee_filter_rendered = ""
    if is_admin_role(current_user):
        clean_html = '<option value="">Все</option>'
        for item in assignable_users:
            selected = "selected" if filter_values.get("assignee", "") == item.username else ""
            clean_html += f'<option value="{escape(item.username)}" {selected}>{escape((item.display_name or item.username) + " · " + (item.role or ""))}</option>'
        assignee_filter_rendered = f'<label>Кому<select name="assignee">{clean_html}</select></label>'

    assign_options = ""
    for item in assignable_users:
        selected = "selected" if form_data.get("assigned_to_username", "") == item.username else ""
        assign_options += f'<option value="{escape(item.username)}" {selected}>{escape((item.display_name or item.username) + " · " + (item.role or ""))}</option>'

    create_block = ""
    if is_admin_role(current_user):
        due_selects = build_task_datetime_selects("due", form_data.get("due_at", ""))
        create_block = f"""
        <details class="panel">
            <summary class="panel-title" style="cursor:pointer; list-style:none; display:flex; align-items:center; justify-content:space-between;">
                <span>Add Task</span>
                <span class="btn toggle-indicator"></span>
            </summary>
            <form method="post" action="/tasks/upload" enctype="multipart/form-data" class="tasks-form" style="margin-top:14px; margin-bottom:14px;">
                <label>Task CSV Import
                    <input type="file" name="file" accept=".csv" required>
                </label>
                <label>Кому
                    <select name="assigned_to_username" required>{assign_options}</select>
                </label>
                <button type="submit" class="ghost-btn">Upload</button>
            </form>
            <form method="post" action="/tasks/save" class="tasks-form" style="margin-top:14px;">
                <label>Кому
                    <select name="assigned_to_username" required>{assign_options}</select>
                </label>
                <label>Задача
                    <input type="text" name="title" value="{escape(form_data.get('title', ''))}" required placeholder="Example: Check KPI for Peru">
                </label>
                <label>Описание
                    <textarea name="description" placeholder="What needs to be done and what result is expected">{escape(form_data.get('description', ''))}</textarea>
                </label>
                <div>
                    <div class="panel-subtitle" style="margin-bottom:8px;">Due Date</div>
                    {due_selects}
                </div>
                <label>Примечания
                    <textarea name="notes" placeholder="Extra context, links or agreements">{escape(form_data.get('notes', ''))}</textarea>
                </label>
                <button type="submit" class="btn">Add Task</button>
            </form>
        </details>
        """

    task_cards = ""
    for row in rows:
        response_html = f'<div class="task-note">{escape(row.response_text)}</div>' if (row.response_text or "").strip() else '<div class="task-answer-empty">Ответа пока нет</div>'
        due_text = format_datetime_human(row.due_at)
        answered_text = format_datetime_human(row.answered_at) if row.answered_at else "Нет ответа"
        admin_controls = ""
        if is_admin_role(current_user):
            admin_controls = f'<div class="task-chip">Исполнитель: {escape(row.assigned_to_name or row.assigned_to_username)} · {escape(row.assigned_to_role or "")}</div>'
        respond_block = ""
        if row.assigned_to_username == (current_user or {}).get("username") or is_admin_role(current_user):
            delete_block = ""
            if is_admin_role(current_user):
                delete_block = f"""
                <form method="post" action="/tasks/delete" style="margin-top:10px;" onsubmit="return confirm('Удалить задачу?');">
                    <input type="hidden" name="task_id" value="{row.id}">
                    <button type="submit" class="ghost-btn small-btn">Delete</button>
                </form>
                """
            respond_block = f"""
            <form method="post" action="/tasks/respond" class="tasks-form" style="margin-top:14px;">
                <input type="hidden" name="task_id" value="{row.id}">
                <label>Статус
                    <select name="status">
                        {''.join([f'<option value="{escape(option)}" {"selected" if row.status == option else ""}>{escape(option)}</option>' for option in status_options])}
                    </select>
                </label>
                <label>Reply
                    <textarea name="response_text" placeholder="What was done, current update or blocker">{escape(row.response_text or '')}</textarea>
                </label>
                <button type="submit" class="btn">Save Reply</button>
            </form>
            {delete_block}
            """
        task_cards += f"""
        <div class="task-card">
            <div class="task-head">
                <div>
                    <div class="task-title">{escape(row.title or "Без названия")}</div>
                    <div class="muted">{escape(row.description or "Без описания")}</div>
                </div>
                <div class="task-meta">
                    <div class="task-chip">Статус: {escape(row.status or "Не начато")}</div>
                    <div class="task-chip">Срок: {escape(due_text)}</div>
                    <div class="task-chip">Ответ: {escape(answered_text)}</div>
                    {admin_controls}
                </div>
            </div>
            <div class="task-body">
                <div>
                    <div class="panel-subtitle" style="margin-bottom:8px;">Task Brief</div>
                    <div class="task-note">{escape(row.notes or "No extra notes")}</div>
                </div>
                <div>
                    <div class="panel-subtitle" style="margin-bottom:8px;">Assignee Reply</div>
                    {response_html}
                </div>
            </div>
            <div class="muted" style="margin-top:12px;">От {escape(row.created_by_name or row.created_by_username)} · создано {escape(format_datetime_human(row.created_at))}</div>
            {respond_block}
        </div>
        """

    subtitle = "Tasks and deadlines." if is_admin_role(current_user) else "Your current tasks."
    content = f"""
    {message_html}
    <div class="panel compact-panel">
        <div class="panel-title">Tasks</div>
        <div class="panel-subtitle">{subtitle}</div>
    </div>

    <div class="panel compact-panel">
        <div class="stats-grid">
            <div class="stat-card"><div class="name">Tasks</div><div class="value">{len(rows)}</div></div>
            <div class="stat-card"><div class="name">Open</div><div class="value">{my_open}</div></div>
            <div class="stat-card"><div class="name">Answered</div><div class="value">{answered}</div></div>
            <div class="stat-card"><div class="name">Overdue</div><div class="value">{overdue}</div></div>
        </div>
    </div>

    <div class="tasks-layout">
        {create_block}
        <div>
            <div class="panel compact-panel filters">
                <div class="panel-title">Filters</div>
                <form method="get" action="/tasks">
                    {assignee_filter_rendered}
                    <label>Статус<select name="status">{status_options_html}</select></label>
                    <label>Search<input type="text" name="search" value="{escape(filter_values.get('search', ''))}" placeholder="Search tasks"></label>
                    <button type="submit" class="btn small-btn">Filter</button>
                    <a href="/tasks" class="ghost-btn small-btn">Reset</a>
                </form>
            </div>
            <div class="task-stack">{task_cards if task_cards else '<div class="panel">Нет задач</div>'}</div>
        </div>
    </div>
    """
    return page_shell("Tasks", content, active_page="tasks", current_user=current_user)


def render_dev_page(title, emoji, active_page, current_user=None):
    content = f'''
    <div class="empty-dev">
        <div class="empty-dev-card">
            <div class="big">{emoji} {escape(title)}</div>
            <div class="muted">Эта страница пока в разработке. Блок уже добавлен в меню, дальше сможем наполнять его отдельно.</div>
        </div>
    </div>
    '''
    return page_shell(title, content, active_page=active_page, current_user=current_user)


def chatterfy_page_html(
    current_user,
    rows,
    status="",
    search="",
    date_filter="",
    time_filter="",
    telegram_id="",
    pp_player_id="",
    sort_by="started_date",
    order="desc",
    page=1,
    total_count=0,
    per_page=100,
    success_text="",
    error_text="",
):
    status_values = sorted({safe_text(item["row"].status) for item in rows if safe_text(item["row"].status)})
    status_options = make_options(status_values, status)
    total_pages = max(1, (int(total_count or 0) + per_page - 1) // per_page)

    rows_html = ""
    for item in rows:
        row = item["row"]
        chat_link = item.get("chat_link") or ""
        chat_link_html = f'<a href="https://{escape(chat_link)}" target="_blank" rel="noreferrer" class="ghost-btn small-btn">Open</a>' if chat_link else "—"
        rows_html += f"""
        <tr>
            <td data-col="started_date">{escape(item.get("started_date") or "")}</td>
            <td data-col="started_time">{escape(item.get("started_time") or "")}</td>
            <td data-col="name">{escape(row.name or "")}</td>
            <td data-col="telegram_id">{escape(row.telegram_id or "")}</td>
            <td data-col="pp_player_id">{escape(item.get("pp_player_id") or "")}</td>
            <td data-col="chat_link">{chat_link_html}</td>
            <td data-col="username">{escape(row.username or "")}</td>
            <td data-col="tags">{escape(row.tags or "")}</td>
            <td data-col="launch_date">{escape(row.launch_date or "")}</td>
            <td data-col="platform">{escape(row.platform or "")}</td>
            <td data-col="manager">{escape(row.manager or "")}</td>
            <td data-col="geo">{escape(row.geo or "")}</td>
            <td data-col="offer">{escape(row.offer or "")}</td>
            <td data-col="status">{escape(row.status or "")}</td>
        </tr>
        """

    message_html = ""
    if success_text:
        message_html += f'<div class="notice">{escape(success_text)}</div>'
    if error_text:
        message_html += f'<div class="notice notice-danger">{escape(error_text)}</div>'

    base_qs = build_query_string(
        status=status,
        search=search,
        date_filter=date_filter,
        time_filter=time_filter,
        telegram_id=telegram_id,
        pp_player_id=pp_player_id,
        sort_by=sort_by,
        order=order,
    )
    prev_link = f"/chatterfy?{base_qs}&page={page - 1}" if page > 1 else ""
    next_link = f"/chatterfy?{base_qs}&page={page + 1}" if page < total_pages else ""

    def header_link(field, label):
        next_order = "asc"
        arrow = ""
        if sort_by == field:
            if order == "asc":
                next_order = "desc"
                arrow = " ↑"
            else:
                arrow = " ↓"
        qs = build_query_string(
            status=status,
            search=search,
            date_filter=date_filter,
            time_filter=time_filter,
            telegram_id=telegram_id,
            pp_player_id=pp_player_id,
            sort_by=field,
            order=next_order,
            page=1,
        )
        return f'<a href="/chatterfy?{qs}">{escape(label)}{arrow}</a>'

    column_defs = [
        ("started_date", "Date"),
        ("started_time", "Time"),
        ("name", "Name"),
        ("telegram_id", "Telegram ID"),
        ("pp_player_id", "ID in PP"),
        ("chat_link", "Chat"),
        ("username", "Username"),
        ("tags", "Tags"),
        ("launch_date", "Tag Date"),
        ("platform", "Platform"),
        ("manager", "Manager"),
        ("geo", "Geo"),
        ("offer", "Offer"),
        ("status", "Status"),
    ]
    column_chips = "".join([
        f'<label class="column-chip"><input class="column-toggle-chatterfy" type="checkbox" value="{key}" checked> {label}</label>'
        for key, label in column_defs
    ])

    extra_scripts = """
    <script>
        (function() {
            const table = document.getElementById('chatterfyTable');
            if (!table) return;
            const WIDTH_KEY = 'teambead_chatterfy_widths_v1';
            const HIDDEN_KEY = 'teambead_chatterfy_hidden_columns_v1';
            const ORDER_KEY = 'teambead_chatterfy_column_order_v1';
            function getHeaderRow() { return table.querySelector('thead tr'); }
            function getRows() { return Array.from(table.querySelectorAll('tr')); }
            function getCurrentOrder() {
                return Array.from(getHeaderRow().querySelectorAll('th[data-col]')).map(th => th.dataset.col);
            }
            function reorderCells(order) {
                getRows().forEach(row => {
                    const cellsMap = {};
                    Array.from(row.children).forEach(cell => {
                        const key = cell.dataset.col;
                        if (key) cellsMap[key] = cell;
                    });
                    order.forEach(key => {
                        if (cellsMap[key]) row.appendChild(cellsMap[key]);
                    });
                });
            }
            function applyOrder() {
                const saved = JSON.parse(localStorage.getItem(ORDER_KEY) || '[]');
                const current = getCurrentOrder();
                if (!saved.length) return;
                const merged = saved.filter(x => current.includes(x)).concat(current.filter(x => !saved.includes(x)));
                reorderCells(merged);
            }
            function applyVisibility() {
                const hidden = JSON.parse(localStorage.getItem(HIDDEN_KEY) || '[]');
                document.querySelectorAll('.column-toggle-chatterfy').forEach(cb => {
                    cb.checked = !hidden.includes(cb.value);
                });
                document.querySelectorAll('#chatterfyTable [data-col]').forEach(el => {
                    el.style.display = hidden.includes(el.dataset.col) ? 'none' : '';
                });
            }
            function saveVisibility() {
                const hidden = [];
                document.querySelectorAll('.column-toggle-chatterfy').forEach(cb => {
                    if (!cb.checked) hidden.push(cb.value);
                });
                localStorage.setItem(HIDDEN_KEY, JSON.stringify(hidden));
                applyVisibility();
            }
            function showAllColumns() {
                localStorage.setItem(HIDDEN_KEY, JSON.stringify([]));
                applyVisibility();
            }
            function resetColumnsAll() {
                localStorage.removeItem(HIDDEN_KEY);
                localStorage.removeItem(ORDER_KEY);
                localStorage.removeItem(WIDTH_KEY);
                window.location.reload();
            }
            window.toggleChatterfyColumnMenu = function() {
                const menu = document.getElementById('chatterfyColumnMenu');
                if (menu) menu.classList.toggle('open');
            }
            function applyWidths() {
                const widths = JSON.parse(localStorage.getItem(WIDTH_KEY) || '{}');
                Object.entries(widths).forEach(([key, width]) => {
                    document.querySelectorAll('[data-col=\"' + key + '\"]').forEach(el => {
                        el.style.width = width + 'px';
                        el.style.minWidth = width + 'px';
                        el.style.maxWidth = width + 'px';
                    });
                });
            }
            function saveWidth(key, width) {
                const widths = JSON.parse(localStorage.getItem(WIDTH_KEY) || '{}');
                widths[key] = Math.max(90, Math.round(width));
                localStorage.setItem(WIDTH_KEY, JSON.stringify(widths));
            }
            table.querySelectorAll('th[data-col]').forEach(th => {
                const resizer = th.querySelector('.resizer');
                if (!resizer) return;
                let startX = 0;
                let startWidth = 0;
                let resizing = false;
                th.setAttribute('draggable', 'true');
                const key = th.dataset.col;
                resizer.addEventListener('mousedown', function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    resizing = true;
                    startX = e.clientX;
                    startWidth = th.getBoundingClientRect().width;
                    document.body.style.cursor = 'col-resize';
                });
                document.addEventListener('mousemove', function(e) {
                    if (!resizing) return;
                    const newWidth = Math.max(90, startWidth + (e.clientX - startX));
                    document.querySelectorAll('[data-col=\"' + key + '\"]').forEach(el => {
                        el.style.width = newWidth + 'px';
                        el.style.minWidth = newWidth + 'px';
                        el.style.maxWidth = newWidth + 'px';
                    });
                });
                document.addEventListener('mouseup', function() {
                    if (!resizing) return;
                    resizing = false;
                    document.body.style.cursor = '';
                    saveWidth(key, th.getBoundingClientRect().width);
                });
            });
            let dragged = null;
            table.querySelectorAll('th[data-col]').forEach(th => {
                th.addEventListener('dragstart', function(e) {
                    if (e.target.classList.contains('resizer')) { e.preventDefault(); return; }
                    dragged = th;
                    th.classList.add('dragging');
                });
                th.addEventListener('dragend', function() {
                    table.querySelectorAll('th[data-col]').forEach(x => x.classList.remove('dragging', 'drag-target-left', 'drag-target-right'));
                    dragged = null;
                });
                th.addEventListener('dragover', function(e) {
                    e.preventDefault();
                    if (!dragged || dragged === th) return;
                    const rect = th.getBoundingClientRect();
                    const before = (e.clientX - rect.left) < rect.width / 2;
                    th.classList.toggle('drag-target-left', before);
                    th.classList.toggle('drag-target-right', !before);
                });
                th.addEventListener('dragleave', function() {
                    th.classList.remove('drag-target-left', 'drag-target-right');
                });
                th.addEventListener('drop', function(e) {
                    e.preventDefault();
                    if (!dragged || dragged === th) return;
                    const rect = th.getBoundingClientRect();
                    const before = (e.clientX - rect.left) < rect.width / 2;
                    if (before) th.parentNode.insertBefore(dragged, th);
                    else th.parentNode.insertBefore(dragged, th.nextSibling);
                    const order = getCurrentOrder();
                    reorderCells(order);
                    localStorage.setItem(ORDER_KEY, JSON.stringify(order));
                    table.querySelectorAll('th[data-col]').forEach(x => x.classList.remove('drag-target-left', 'drag-target-right'));
                });
            });
            document.querySelectorAll('.column-toggle-chatterfy').forEach(cb => cb.addEventListener('change', saveVisibility));
            applyOrder();
            applyVisibility();
            applyWidths();
        })();
    </script>
    """

    content = f"""
    {message_html}

    <div class="panel compact-panel">
        <div class="controls-line">
            <div>
                <div class="panel-title" style="margin-bottom:4px;">Chatterfy Report</div>
            </div>
            <div style="display:flex; gap:10px; align-items:flex-start; flex-wrap:wrap; justify-content:flex-end;">
                <details class="upload-menu">
                    <summary class="btn toggle-indicator" style="width:34px; height:34px; border-radius:10px;"></summary>
                    <div class="upload-menu-list">
                        <form method="post" action="/chatterfy/upload" enctype="multipart/form-data" class="upload-inline" style="justify-content:space-between;">
                            <label>Chatterfy File
                                <input type="file" name="file" accept=".csv,.xlsx,.xls" required>
                            </label>
                            <button type="submit" class="btn small-btn">Upload</button>
                        </form>
                        <form method="post" action="/chatterfy/upload-ids" enctype="multipart/form-data" class="upload-inline" style="justify-content:space-between;">
                            <label>ID File
                                <input type="file" name="file" accept=".csv,.xlsx,.xls" required>
                            </label>
                            <button type="submit" class="btn small-btn">Upload</button>
                        </form>
                    </div>
                </details>
                <div class="panel compact-panel filters" style="margin:0; min-width:620px;">
                    <form method="get" action="/chatterfy" style="justify-content:flex-end;">
                        <label>Date<input type="text" name="date_filter" value="{escape(date_filter)}" placeholder="27.03.2026"></label>
                        <label>Time<input type="text" name="time_filter" value="{escape(time_filter)}" placeholder="09:3"></label>
                        <label>Telegram ID<input type="text" name="telegram_id" value="{escape(telegram_id)}" placeholder="5065148172"></label>
                        <label>ID in PP<input type="text" name="pp_player_id" value="{escape(pp_player_id)}" placeholder="1601157577"></label>
                        <label>Status<select name="status">{status_options}</select></label>
                        <label>Search<input type="text" name="search" value="{escape(search)}" placeholder="tags, manager, geo, offer"></label>
                        <input type="hidden" name="page" value="1">
                        <button type="submit" class="btn small-btn">Filter</button>
                        <a href="/chatterfy" class="btn small-btn">Reset</a>
                    </form>
                </div>
            </div>
        </div>
        <div class="controls-line" style="margin-top:12px;">
            <div></div>
            <div class="column-menu-wrap">
                <button type="button" class="ghost-btn small-btn" onclick="toggleChatterfyColumnMenu()">⚙️ Columns</button>
                <div class="column-menu" id="chatterfyColumnMenu">
                    <div class="column-actions">
                        <button type="button" class="ghost-btn small-btn" onclick="showAllColumns()">Show All</button>
                        <button type="button" class="ghost-btn small-btn" onclick="resetColumnsAll()">Reset All</button>
                    </div>
                    <div class="column-grid">{column_chips}</div>
                </div>
            </div>
        </div>
        <div class="table-wrap">
            <table id="chatterfyTable" style="min-width:1900px;">
                <thead>
                    <tr>
                        <th data-col="started_date"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("started_date", "Date")}<span class="resizer"></span></div></th>
                        <th data-col="started_time"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("started_time", "Time")}<span class="resizer"></span></div></th>
                        <th data-col="name"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("name", "Name")}<span class="resizer"></span></div></th>
                        <th data-col="telegram_id"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("telegram_id", "Telegram ID")}<span class="resizer"></span></div></th>
                        <th data-col="pp_player_id"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("pp_player_id", "ID in PP")}<span class="resizer"></span></div></th>
                        <th data-col="chat_link"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("chat_link", "Chat")}<span class="resizer"></span></div></th>
                        <th data-col="username"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("username", "Username")}<span class="resizer"></span></div></th>
                        <th data-col="tags"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("tags", "Tags")}<span class="resizer"></span></div></th>
                        <th data-col="launch_date"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("launch_date", "Tag Date")}<span class="resizer"></span></div></th>
                        <th data-col="platform"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("platform", "Platform")}<span class="resizer"></span></div></th>
                        <th data-col="manager"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("manager", "Manager")}<span class="resizer"></span></div></th>
                        <th data-col="geo"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("geo", "Geo")}<span class="resizer"></span></div></th>
                        <th data-col="offer"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("offer", "Offer")}<span class="resizer"></span></div></th>
                        <th data-col="status"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("status", "Status")}<span class="resizer"></span></div></th>
                    </tr>
                </thead>
                <tbody>{rows_html if rows_html else '<tr><td colspan="14">Нет данных</td></tr>'}</tbody>
            </table>
        </div>
        <div style="display:flex; justify-content:space-between; align-items:center; gap:12px; margin-top:14px; flex-wrap:wrap;">
            <div class="panel-subtitle">Showing {len(rows)} of {total_count}</div>
            <div style="display:flex; gap:8px; align-items:center;">
                {f'<a href="{prev_link}" class="ghost-btn small-btn">Prev</a>' if prev_link else '<span class="ghost-btn small-btn" style="opacity:0.45; pointer-events:none;">Prev</span>'}
                <span class="user-chip">Page {page} / {total_pages}</span>
                {f'<a href="{next_link}" class="ghost-btn small-btn">Next</a>' if next_link else '<span class="ghost-btn small-btn" style="opacity:0.45; pointer-events:none;">Next</span>'}
            </div>
        </div>
    </div>
    """
    return page_shell("Chatterfy", content, active_page="chatterfy", current_user=current_user, extra_scripts=extra_scripts)


# =========================================
# BLOCK 7.5 — USERS
# =========================================
@app.get("/users", response_class=HTMLResponse)
def users_page(request: Request, edit: str = Query(default=""), message: str = Query(default="")):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "users")

    form_data = {}
    if edit:
        db = SessionLocal()
        try:
            edit_user = db.query(User).filter(User.id == safe_number(edit)).first()
            if edit_user:
                form_data = {
                    "edit_user_id": str(edit_user.id),
                    "display_name": edit_user.display_name or "",
                    "username": edit_user.username or "",
                    "role": edit_user.role or "buyer",
                    "buyer_name": edit_user.buyer_name or "",
                    "is_active": "1" if edit_user.is_active else "0",
                }
        finally:
            db.close()

    return users_page_html(user, success_text=message, form_data=form_data)


@app.get("/tasks", response_class=HTMLResponse)
def tasks_page(
    request: Request,
    status: str = Query(default=""),
    assignee: str = Query(default=""),
    search: str = Query(default=""),
    message: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "tasks")
    rows = get_tasks_for_user(user, status_filter=status, assignee_filter=assignee, search=search)
    return tasks_page_html(
        user,
        rows,
        filter_values={"status": status, "assignee": assignee, "search": search},
        success_text=message,
    )


@app.post("/tasks/save")
def save_task(
    request: Request,
    assigned_to_username: str = Form(...),
    title: str = Form(...),
    description: str = Form(default=""),
    due_year: str = Form(default="2026"),
    due_month: str = Form(default="3"),
    due_day: str = Form(default="26"),
    due_hour: str = Form(default="12"),
    due_minute: str = Form(default="0"),
    notes: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin", "admin")
    Base.metadata.create_all(bind=engine, tables=[TaskRow.__table__])

    clean_title = safe_text(title)
    clean_assignee = safe_text(assigned_to_username)
    due_at = compose_task_datetime_from_form(due_year, due_month, due_day, due_hour, due_minute)
    form_data = {
        "assigned_to_username": clean_assignee,
        "title": title,
        "description": description,
        "due_at": due_at,
        "notes": notes,
    }
    if not clean_title or not clean_assignee:
        rows = get_tasks_for_user(user)
        return HTMLResponse(tasks_page_html(user, rows, form_data=form_data, error_text="Заполни задачу и выбери исполнителя."), status_code=400)

    db = SessionLocal()
    try:
        target_user = db.query(User).filter(User.username == clean_assignee, User.is_active == 1).first()
        if not target_user or target_user.role not in {"buyer", "operator", "finance"}:
            rows = get_tasks_for_user(user)
            return HTMLResponse(tasks_page_html(user, rows, form_data=form_data, error_text="Можно ставить задачи только buyer, operator или finance."), status_code=400)
        now = datetime.utcnow()
        db.add(TaskRow(
            title=clean_title,
            description=safe_text(description),
            assigned_to_username=target_user.username,
            assigned_to_name=target_user.display_name or target_user.username,
            assigned_to_role=target_user.role or "",
            created_by_username=user.get("username", ""),
            created_by_name=user.get("display_name", user.get("username", "")),
            status="Не начато",
            due_at=parse_datetime_local(due_at),
            response_text="",
            notes=safe_text(notes),
            created_at=now,
            updated_at=now,
        ))
        db.commit()
    finally:
        db.close()

    return RedirectResponse(url="/tasks?message=Задача поставлена", status_code=303)


@app.post("/tasks/upload")
async def upload_tasks_file(
    request: Request,
    assigned_to_username: str = Form(...),
    file: UploadFile = File(...),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin", "admin")
    filename = f"temp_tasks_{uuid.uuid4()}.csv"
    try:
        with open(filename, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        df = pd.read_csv(filename)
        import_tasks_dataframe(df, assigned_to_username=assigned_to_username, created_by_user=user)
        return RedirectResponse(url="/tasks?message=Задачи загружены", status_code=303)
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@app.post("/tasks/delete")
def delete_task(request: Request, task_id: str = Form(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin", "admin")
    Base.metadata.create_all(bind=engine, tables=[TaskRow.__table__])
    db = SessionLocal()
    try:
        db.query(TaskRow).filter(TaskRow.id == safe_number(task_id)).delete()
        db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/tasks?message=Задача удалена", status_code=303)


@app.post("/tasks/respond")
def respond_task(
    request: Request,
    task_id: str = Form(...),
    status: str = Form(...),
    response_text: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "tasks")
    Base.metadata.create_all(bind=engine, tables=[TaskRow.__table__])
    if status not in get_task_status_options():
        return RedirectResponse(url="/tasks?message=Неизвестный статус", status_code=303)

    db = SessionLocal()
    try:
        task = db.query(TaskRow).filter(TaskRow.id == safe_number(task_id)).first()
        if not task:
            return RedirectResponse(url="/tasks?message=Задача не найдена", status_code=303)
        if not is_admin_role(user) and task.assigned_to_username != user.get("username"):
            raise HTTPException(status_code=403)
        task.status = status
        task.response_text = safe_text(response_text)
        task.updated_at = datetime.utcnow()
        task.answered_at = datetime.utcnow() if task.response_text else None
        db.add(task)
        db.commit()
    finally:
        db.close()

    return RedirectResponse(url="/tasks?message=Ответ сохранен", status_code=303)


@app.post("/users/save")
def save_user(
    request: Request,
    edit_user_id: str = Form(default=""),
    display_name: str = Form(...),
    username: str = Form(...),
    password: str = Form(default=""),
    role: str = Form(...),
    buyer_name: str = Form(default=""),
    is_active: str = Form(default="0"),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "users")

    clean_display_name = display_name.strip()
    clean_username = username.strip()
    clean_role = (role or "").strip()
    clean_buyer_name = buyer_name.strip()
    form_data = {
        "edit_user_id": edit_user_id,
        "display_name": clean_display_name,
        "username": clean_username,
        "role": clean_role,
        "buyer_name": clean_buyer_name,
        "is_active": "1" if is_active == "1" else "0",
    }

    if clean_role not in {"superadmin", "admin", "buyer", "operator", "finance"}:
        return HTMLResponse(users_page_html(user, error_text="Неизвестная роль.", form_data=form_data), status_code=400)
    if not clean_display_name or not clean_username:
        return HTMLResponse(users_page_html(user, error_text="Display name и username обязательны.", form_data=form_data), status_code=400)
    if not edit_user_id and len(password.strip()) < 4:
        return HTMLResponse(users_page_html(user, error_text="Для нового пользователя пароль должен быть минимум 4 символа.", form_data=form_data), status_code=400)
    if clean_role == "buyer" and not clean_buyer_name:
        return HTMLResponse(users_page_html(user, error_text="Для buyer нужно заполнить Buyer binding.", form_data=form_data), status_code=400)

    db = SessionLocal()
    try:
        existing = db.query(User).filter(User.username == clean_username).first()
        target_user = db.query(User).filter(User.id == safe_number(edit_user_id)).first() if edit_user_id else None
        if existing and (not target_user or existing.id != target_user.id):
            return HTMLResponse(users_page_html(user, error_text="Такой username уже существует.", form_data=form_data), status_code=400)

        if not target_user:
            target_user = User(
                username=clean_username,
                password_hash=hash_password(password.strip()),
            )
            db.add(target_user)
        elif password.strip():
            target_user.password_hash = hash_password(password.strip())

        target_user.display_name = clean_display_name
        target_user.username = clean_username
        target_user.role = clean_role
        target_user.buyer_name = clean_buyer_name if clean_role == "buyer" else ""
        target_user.is_active = 1 if is_active == "1" else 0
        db.commit()
    finally:
        db.close()

    return RedirectResponse(url="/users?message=Пользователь сохранен", status_code=303)


@app.post("/users/delete")
def delete_user(request: Request, user_id: str = Form(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "users")
    db = SessionLocal()
    try:
        target_user = db.query(User).filter(User.id == safe_number(user_id)).first()
        if not target_user:
            return RedirectResponse(url="/users?message=Пользователь не найден", status_code=303)
        if target_user.username == user.get("username"):
            return RedirectResponse(url="/users?message=Нельзя удалить самого себя", status_code=303)
        db.query(UserSession).filter(UserSession.username == target_user.username).delete()
        db.query(TaskRow).filter(TaskRow.assigned_to_username == target_user.username).delete()
        db.delete(target_user)
        db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/users?message=Пользователь удален", status_code=303)


# =========================================
# BLOCK 8 — UPLOAD
# =========================================
@app.post("/upload")
async def upload_file(request: Request, buyer: str = Form(...), file: UploadFile = File(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin", "admin")
    original_name = file.filename or ""
    ext = os.path.splitext(original_name)[1].lower() or ".csv"
    filename = f"temp_{uuid.uuid4()}{ext}"

    try:
        with open(filename, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(filename)
        else:
            try:
                df = pd.read_csv(filename)
            except Exception:
                df = pd.read_csv(filename, sep=";")

        rows_to_insert = parse_uploaded_dataframe(df, buyer.strip())

        db = SessionLocal()
        try:
            db.query(FBRow).filter(FBRow.uploader == buyer.strip()).delete()
            db.commit()
            for item in rows_to_insert:
                db.add(item)
            db.commit()
        finally:
            db.close()

        return RedirectResponse(url="/grouped", status_code=303)
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@app.post("/upload/partner")
async def upload_partner_file(request: Request, file: UploadFile = File(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin", "admin")
    ensure_partner_table()
    original_name = file.filename or "partner.csv"
    ext = os.path.splitext(original_name)[1].lower() or ".csv"
    filename = f"temp_partner_{uuid.uuid4()}{ext}"
    try:
        with open(filename, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(filename, skiprows=6)
        else:
            df = pd.read_csv(filename, skiprows=6)
        replace_partner_rows(original_name, parse_partner_dataframe(df, source_name=original_name))
        return RedirectResponse(url="/hierarchy", status_code=303)
    finally:
        if os.path.exists(filename):
            os.remove(filename)


# =========================================
# BLOCK 9 — EXPORT
# =========================================
@app.get("/export/grouped")
def export_grouped_csv(
    request: Request,
    buyer: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
    sort_by: str = Query(default="spend"),
    order: str = Query(default="desc"),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin", "admin")
    buyer = resolve_effective_buyer(user, buyer)
    rows = aggregate_grouped_rows(get_filtered_data(buyer, manager, geo, offer, search))
    reverse = order.lower() != "asc"
    rows.sort(key=lambda x: x.get(sort_by, 0) if x.get(sort_by) is not None else 0, reverse=reverse)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Buyer", "Ad Name", "Launch Date", "Platform", "Manager", "Geo", "Offer", "Creative",
        "Rows", "Clicks", "Leads", "Reg", "FTD", "Spend", "CPC", "CPL", "CPA",
        "L2FTD", "R2D", "Date Start", "Date End"
    ])
    for row in rows:
        writer.writerow([
            row["buyer"], row["ad_name"], row["launch_date"], row["platform"], row["manager"], row["geo"], row["offer"], row["creative"],
            format_int_or_float(row["rows_combined"]), format_int_or_float(row["clicks"]), format_int_or_float(row["leads"]),
            format_int_or_float(row["reg"]), format_int_or_float(row["ftd"]), format_money(row["spend"]),
            format_money(row["cpc_real"]), format_money(row["cpl_real"]), format_money(row["cpa_real"]),
            format_percent(row["l2ftd"]), format_percent(row["r2d"]), row["date_start"], row["date_end"],
        ])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=teambead_export.csv"})


@app.get("/export/hierarchy")
def export_hierarchy_csv(
    request: Request,
    buyer: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin", "admin")
    buyer = resolve_effective_buyer(user, buyer)
    rows = enrich_statistic_rows(aggregate_grouped_rows(get_filtered_data(buyer, manager, geo, offer, search)))
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Geo", "Platform", "Manager", "Offer", "Creative", "Ad Name", "Leads", "Reg", "FB FTD", "Chatterfy", "Total FTD", "Qual FTD", "Rate", "Spend", "Income", "Profit", "ROI"])
    for row in rows:
        writer.writerow([
            row["geo"], row["platform"], row["manager"], row["offer"], row["creative"], row["ad_name"],
            format_int_or_float(row["leads"]), format_int_or_float(row["reg"]), format_int_or_float(row["ftd"]),
            format_int_or_float(row.get("stat_chatterfy", 0)),
            format_int_or_float(row.get("stat_total_ftd", 0)), format_int_or_float(row.get("stat_qual_ftd", 0)), format_money(row.get("stat_rate", 0)),
            format_money(row["spend"]), format_money(row.get("stat_income", 0)), format_money(row.get("stat_profit", 0)), format_percent(row.get("stat_roi", 0)),
        ])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=teambead_statistic.csv"})


# =========================================
# BLOCK 10 — GROUPED PAGE
# =========================================
@app.get("/grouped", response_class=HTMLResponse)
def show_grouped_table(
    request: Request,
    buyer: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
    sort_by: str = Query(default="spend"),
    order: str = Query(default="desc"),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "grouped")
    buyer = resolve_effective_buyer(user, buyer)
    data = get_filtered_data(buyer, manager, geo, offer, search)
    rows = aggregate_grouped_rows(data)
    all_buyers, all_managers, all_geos, all_offers = get_scoped_filter_options(user)

    allowed_sort_fields = {
        "buyer", "ad_name", "launch_date", "platform", "manager", "geo", "offer", "creative",
        "rows_combined", "clicks", "leads", "reg", "ftd", "spend", "cpa_real", "l2ftd", "r2d",
        "date_start", "date_end"
    }
    if sort_by not in allowed_sort_fields:
        sort_by = "spend"
    reverse = order.lower() != "asc"
    rows.sort(key=lambda x: x.get(sort_by, 0) if x.get(sort_by) is not None else 0, reverse=reverse)

    totals = aggregate_totals(rows)
    buyer_options = make_options(all_buyers, buyer) if is_admin_role(user) or user.get("role") == "operator" else f'<option value="{escape(buyer)}">{escape(buyer or "Мой buyer")}</option>'
    manager_options = make_options(all_managers, manager)
    geo_options = make_options(all_geos, geo)
    offer_options = make_options(all_offers, offer)
    export_qs = build_query_string(buyer=buyer, manager=manager, geo=geo, offer=offer, search=search, sort_by=sort_by, order=order)
    export_link = f"/export/grouped?{export_qs}" if export_qs else "/export/grouped"

    table_headers = [
        ("buyer", "Buyer"),
        ("ad_name", "Ad Name"),
        ("launch_date", "Launch"),
        ("platform", "Platform"),
        ("manager", "Manager"),
        ("geo", "Geo"),
        ("offer", "Offer"),
        ("creative", "Creative"),
        ("rows_combined", "Rows"),
        ("clicks", "Clicks"),
        ("leads", "Leads"),
        ("reg", "Reg"),
        ("ftd", "FTD"),
        ("spend", "Spend"),
        ("cpa_real", "CPA"),
        ("l2ftd", "L2FTD"),
        ("r2d", "R2D"),
        ("date_start", "Date Start"),
        ("date_end", "Date End"),
    ]

    head_html = ""
    for field, label in table_headers:
        head_html += f'''
        <th data-col="{field}" draggable="true">
            <div class="th-inner">
                <span class="drag-handle">⋮⋮</span>
                <span>{sort_link(label, field, sort_by, order, buyer=buyer, manager=manager, geo=geo, offer=offer, search=search)}</span>
            </div>
            <span class="resizer"></span>
        </th>
        '''

    rows_html = ""
    for row in rows:
        rows_html += f'''
        <tr class="{get_row_class(row)}">
            <td data-col="buyer">{escape(row["buyer"])}</td>
            <td data-col="ad_name">{escape(row["ad_name"])}</td>
            <td data-col="launch_date">{escape(row["launch_date"])}</td>
            <td data-col="platform">{escape(row["platform"])}</td>
            <td data-col="manager">{escape(row["manager"])}</td>
            <td data-col="geo">{escape(row["geo"])}</td>
            <td data-col="offer">{escape(row["offer"])}</td>
            <td data-col="creative">{escape(row["creative"])}</td>
            <td data-col="rows_combined">{format_int_or_float(row["rows_combined"])}</td>
            <td data-col="clicks">{format_int_or_float(row["clicks"])}</td>
            <td data-col="leads">{format_int_or_float(row["leads"])}</td>
            <td data-col="reg">{format_int_or_float(row["reg"])}</td>
            <td data-col="ftd">{format_int_or_float(row["ftd"])}</td>
            <td data-col="spend">{format_money(row["spend"])}</td>
            <td data-col="cpa_real">{format_money(row["cpa_real"])}</td>
            <td data-col="l2ftd">{format_percent(row["l2ftd"])}</td>
            <td data-col="r2d">{format_percent(row["r2d"])}</td>
            <td data-col="date_start">{escape(row["date_start"])}</td>
            <td data-col="date_end">{escape(row["date_end"])}</td>
        </tr>
        '''

    column_chips = ""
    for field, label in table_headers:
        column_chips += f'<label class="column-chip"><input type="checkbox" class="column-toggle" value="{field}" checked> {escape(label)}</label>'

    upload_block = ""
    if is_admin_role(user):
        upload_block = '''
        <div class="panel compact-panel">
            <div class="panel-title">Upload Data</div>
            <form method="post" action="/upload" enctype="multipart/form-data" class="upload-form">
                <label>Buyer
                    <input type="text" name="buyer" required placeholder="Example: TeamBead1">
                </label>
                <label>CSV / XLSX
                    <input type="file" name="file" accept=".csv,.xlsx,.xls" required>
                </label>
                <button type="submit" class="upload-btn">Upload</button>
            </form>
            <div class="hint">If you upload the same buyer again, old rows are replaced with new ones.</div>
        </div>
        '''
    elif user.get("role") == "buyer":
        upload_block = '<div class="panel compact-panel"><div class="panel-title">Access</div><div class="hint">Buyer can only view data for their own buyer. Upload and CSV are hidden.</div></div>'
    else:
        upload_block = '<div class="panel compact-panel"><div class="panel-title">Access</div><div class="hint">Operator can only view FB pages without upload and CSV access.</div></div>'

    content = f'''
    <div class="toolbar-grid">
        {upload_block}

        <div class="panel compact-panel">
            <div class="panel-title">Filters</div>
            <div class="filters">
                <form method="get" action="/grouped">
                    {'<label>Buyer<select name="buyer">' + buyer_options + '</select></label>' if is_admin_role(user) or user.get("role") == "operator" else ''}
                    <label>Manager<select name="manager">{manager_options}</select></label>
                    <label>Geo<select name="geo">{geo_options}</select></label>
                    <label>Offer<select name="offer">{offer_options}</select></label>
                    <label>Search<input type="text" name="search" value="{escape(search)}" placeholder="Search rows"></label>
                    <input type="hidden" name="sort_by" value="{escape(sort_by)}">
                    <input type="hidden" name="order" value="{escape(order)}">
                    <button type="submit" class="btn small-btn">Filter</button>
                    <a href="/grouped" class="ghost-btn small-btn">Reset</a>
                </form>
            </div>
        </div>
    </div>

    {render_stats_cards(totals)}

    <div class="panel compact-panel">
        <div class="controls-line">
            <div>
                <div class="panel-title" style="margin-bottom:4px;">Export Table</div>
                <div class="panel-subtitle">Columns can be dragged by the header and resized from the right edge.</div>
            </div>
            <div class="column-menu-wrap">
                <button type="button" class="ghost-btn small-btn" onclick="toggleColumnMenu()">⚙️ Колонки</button>
                <div class="column-menu" id="columnMenu">
                    <div class="column-actions">
                        <button type="button" class="ghost-btn small-btn" onclick="showAllColumns()">Show All</button>
                        <button type="button" class="ghost-btn small-btn" onclick="resetColumnsAll()">Reset All</button>
                    </div>
                    <div class="column-grid">{column_chips}</div>
                </div>
            </div>
        </div>

        <div class="table-wrap">
            <table id="groupedTable">
                <thead><tr>{head_html}</tr></thead>
                <tbody>{rows_html if rows_html else '<tr><td colspan="19">Нет данных</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    '''

    extra_scripts = """
    <script>
        const HIDDEN_KEY = 'teambead-hidden-columns-v2';
        const ORDER_KEY = 'teambead-column-order-v2';
        const WIDTH_KEY = 'teambead-column-widths-v2';

        function getTable() { return document.getElementById('groupedTable'); }
        function getHeaderRow() { return getTable()?.querySelector('thead tr'); }
        function getCurrentOrder() {
            return Array.from(getHeaderRow().querySelectorAll('th[data-col]')).map(th => th.dataset.col);
        }
        function getRows() {
            return Array.from(getTable().querySelectorAll('tr'));
        }
        function reorderCells(order) {
            getRows().forEach(row => {
                const cellsMap = {};
                Array.from(row.children).forEach(cell => {
                    const key = cell.dataset.col;
                    if (key) cellsMap[key] = cell;
                });
                order.forEach(key => {
                    if (cellsMap[key]) row.appendChild(cellsMap[key]);
                });
            });
        }
        function applyOrder() {
            const saved = JSON.parse(localStorage.getItem(ORDER_KEY) || '[]');
            const current = getCurrentOrder();
            if (!saved.length) return;
            const merged = saved.filter(x => current.includes(x)).concat(current.filter(x => !saved.includes(x)));
            reorderCells(merged);
        }
        function applyVisibility() {
            const hidden = JSON.parse(localStorage.getItem(HIDDEN_KEY) || '[]');
            document.querySelectorAll('.column-toggle').forEach(cb => {
                cb.checked = !hidden.includes(cb.value);
            });
            document.querySelectorAll('[data-col]').forEach(el => {
                el.style.display = hidden.includes(el.dataset.col) ? 'none' : '';
            });
        }
        function saveVisibility() {
            const hidden = [];
            document.querySelectorAll('.column-toggle').forEach(cb => {
                if (!cb.checked) hidden.push(cb.value);
            });
            localStorage.setItem(HIDDEN_KEY, JSON.stringify(hidden));
            applyVisibility();
        }
        function showAllColumns() {
            localStorage.setItem(HIDDEN_KEY, JSON.stringify([]));
            applyVisibility();
        }
        function resetColumnsAll() {
            localStorage.removeItem(HIDDEN_KEY);
            localStorage.removeItem(ORDER_KEY);
            localStorage.removeItem(WIDTH_KEY);
            window.location.reload();
        }
        function toggleColumnMenu() {
            document.getElementById('columnMenu').classList.toggle('open');
        }
        function applyWidths() {
            const widths = JSON.parse(localStorage.getItem(WIDTH_KEY) || '{}');
            Object.entries(widths).forEach(([key, width]) => {
                document.querySelectorAll('[data-col="' + key + '"]').forEach(el => {
                    el.style.width = width + 'px';
                    el.style.minWidth = width + 'px';
                    el.style.maxWidth = width + 'px';
                });
            });
        }
        function saveWidth(key, width) {
            const widths = JSON.parse(localStorage.getItem(WIDTH_KEY) || '{}');
            widths[key] = Math.max(80, Math.round(width));
            localStorage.setItem(WIDTH_KEY, JSON.stringify(widths));
        }
        function initResizers() {
            document.querySelectorAll('#groupedTable th[data-col]').forEach(th => {
                const resizer = th.querySelector('.resizer');
                if (!resizer) return;
                let startX = 0;
                let startWidth = 0;
                let resizing = false;
                const key = th.dataset.col;
                resizer.addEventListener('mousedown', function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    resizing = true;
                    startX = e.clientX;
                    startWidth = th.getBoundingClientRect().width;
                    document.body.style.cursor = 'col-resize';
                });
                document.addEventListener('mousemove', function(e) {
                    if (!resizing) return;
                    const newWidth = Math.max(80, startWidth + (e.clientX - startX));
                    document.querySelectorAll('[data-col="' + key + '"]').forEach(el => {
                        el.style.width = newWidth + 'px';
                        el.style.minWidth = newWidth + 'px';
                        el.style.maxWidth = newWidth + 'px';
                    });
                });
                document.addEventListener('mouseup', function() {
                    if (!resizing) return;
                    resizing = false;
                    document.body.style.cursor = '';
                    saveWidth(key, th.getBoundingClientRect().width);
                });
            });
        }
        function initDragAndDrop() {
            let dragged = null;
            document.querySelectorAll('#groupedTable th[data-col]').forEach(th => {
                th.addEventListener('dragstart', function(e) {
                    if (e.target.classList.contains('resizer')) { e.preventDefault(); return; }
                    dragged = th;
                    th.classList.add('dragging');
                });
                th.addEventListener('dragend', function() {
                    document.querySelectorAll('#groupedTable th[data-col]').forEach(x => x.classList.remove('dragging', 'drag-target-left', 'drag-target-right'));
                    dragged = null;
                });
                th.addEventListener('dragover', function(e) {
                    e.preventDefault();
                    if (!dragged || dragged === th) return;
                    const rect = th.getBoundingClientRect();
                    const before = (e.clientX - rect.left) < rect.width / 2;
                    th.classList.toggle('drag-target-left', before);
                    th.classList.toggle('drag-target-right', !before);
                });
                th.addEventListener('dragleave', function() {
                    th.classList.remove('drag-target-left', 'drag-target-right');
                });
                th.addEventListener('drop', function(e) {
                    e.preventDefault();
                    if (!dragged || dragged === th) return;
                    const rect = th.getBoundingClientRect();
                    const before = (e.clientX - rect.left) < rect.width / 2;
                    if (before) th.parentNode.insertBefore(dragged, th);
                    else th.parentNode.insertBefore(dragged, th.nextSibling);
                    const order = getCurrentOrder();
                    reorderCells(order);
                    localStorage.setItem(ORDER_KEY, JSON.stringify(order));
                    document.querySelectorAll('#groupedTable th[data-col]').forEach(x => x.classList.remove('drag-target-left', 'drag-target-right'));
                });
            });
        }
        document.querySelectorAll('.column-toggle').forEach(cb => cb.addEventListener('change', saveVisibility));
        applyOrder();
        applyVisibility();
        applyWidths();
        initResizers();
        initDragAndDrop();
    </script>
    """

    top_actions = f'<a class="small-btn" href="{export_link}">⬇ CSV</a>' if is_admin_role(user) else ""
    return page_shell("FB — Export", content, "grouped", extra_scripts, top_actions=top_actions, current_user=user)


# =========================================
# BLOCK 11 — STATISTIC PAGE
# =========================================
@app.get("/hierarchy", response_class=HTMLResponse)
def show_hierarchy(
    request: Request,
    buyer: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "hierarchy")
    buyer = resolve_effective_buyer(user, buyer)
    data = get_filtered_data(buyer, manager, geo, offer, search)
    rows = enrich_statistic_rows(aggregate_grouped_rows(data))
    all_buyers, all_managers, all_geos, all_offers = get_scoped_filter_options(user)

    buyer_options = make_options(all_buyers, buyer) if is_admin_role(user) or user.get("role") == "operator" else f'<option value="{escape(buyer)}">{escape(buyer or "Мой buyer")}</option>'
    manager_options = make_options(all_managers, manager)
    geo_options = make_options(all_geos, geo)
    offer_options = make_options(all_offers, offer)

    hierarchy_keys = ["geo", "platform", "manager", "offer", "creative", "ad_name"]
    tree = aggregate_for_hierarchy(rows, hierarchy_keys)
    tree_html = render_tree_nodes(tree) if tree else '<div class="panel">Нет данных</div>'
    totals = aggregate_totals(rows)

    export_qs = build_query_string(buyer=buyer, manager=manager, geo=geo, offer=offer, search=search)
    export_link = f"/export/hierarchy?{export_qs}" if export_qs else "/export/hierarchy"

    content = f'''
    <div class="panel compact-panel filters">
        <div class="panel-title">Filters</div>
        <form method="get" action="/hierarchy">
            {'<label>Buyer<select name="buyer">' + buyer_options + '</select></label>' if is_admin_role(user) or user.get("role") == "operator" else ''}
            <label>Manager<select name="manager">{manager_options}</select></label>
            <label>Geo<select name="geo">{geo_options}</select></label>
            <label>Offer<select name="offer">{offer_options}</select></label>
            <label>Search<input type="text" name="search" value="{escape(search)}"></label>
            <button type="submit" class="btn small-btn">Filter</button>
            <a href="/hierarchy" class="ghost-btn small-btn">Reset</a>
        </form>
    </div>

    {render_statistic_cards(totals)}

    <div class="panel compact-panel">
        <div class="panel-title">Statistic</div>
        <div class="panel-subtitle">FB + Chatterfy + Partner + Caps in one structure for tracking traffic, FTD, quals, income and profit.</div>
    </div>

    <div class="tree-root">{tree_html}</div>
    '''

    top_actions = f'<a class="small-btn" href="{export_link}">⬇ CSV</a>' if is_admin_role(user) else ""
    return page_shell("FB — Statistic", content, "hierarchy", top_actions=top_actions, current_user=user)


# =========================================
# BLOCK 12 — PLACEHOLDERS
# =========================================
@app.get("/finance", response_class=HTMLResponse)
def finance_page(
    request: Request,
    message: str = Query(default=""),
    edit_wallet: str = Query(default=""),
    edit_expense: str = Query(default=""),
    edit_income: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_finance_tables()
    form_data = {}
    db = SessionLocal()
    try:
        wallet_item = db.query(FinanceWalletRow).filter(FinanceWalletRow.id == safe_number(edit_wallet)).first() if edit_wallet else None
        expense_item = db.query(FinanceExpenseRow).filter(FinanceExpenseRow.id == safe_number(edit_expense)).first() if edit_expense else None
        income_item = db.query(FinanceIncomeRow).filter(FinanceIncomeRow.id == safe_number(edit_income)).first() if edit_income else None
        form_data = build_finance_form_data(wallet_item=wallet_item, expense_item=expense_item, income_item=income_item)
    finally:
        db.close()
    return finance_page_html(user, success_text=message, form_data=form_data)


@app.post("/finance/wallets/save")
def save_finance_wallet(
    request: Request,
    edit_id: str = Form(default=""),
    category: str = Form(default=""),
    description: str = Form(default=""),
    owner_name: str = Form(default=""),
    wallet: str = Form(default=""),
    amount: str = Form(default="0"),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_finance_tables()
    form_data = {
        "wallet_category": category,
        "wallet_description": description,
        "wallet_owner_name": owner_name,
        "wallet_wallet": wallet,
        "wallet_amount": amount,
    }
    if not safe_text(wallet):
        return HTMLResponse(finance_page_html(user, error_text="Укажи кошелек.", form_data=form_data), status_code=400)
    db = SessionLocal()
    try:
        item = db.query(FinanceWalletRow).filter(FinanceWalletRow.id == safe_number(edit_id)).first() if edit_id else None
        if not item:
            item = FinanceWalletRow()
            db.add(item)
        item.category = safe_text(category)
        item.description = safe_text(description)
        item.owner_name = safe_text(owner_name)
        item.wallet = safe_text(wallet)
        item.amount = safe_cap_number(amount)
        db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/finance?message=Кошелек сохранен", status_code=303)


@app.post("/finance/upload")
async def upload_finance_file(request: Request, file: UploadFile = File(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_upload_dir()
    with open(FINANCE_UPLOAD_PATH, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return RedirectResponse(url="/finance?message=Финансы загружены", status_code=303)


@app.post("/finance/expenses/save")
def save_finance_expense(
    request: Request,
    edit_id: str = Form(default=""),
    expense_date: str = Form(default=""),
    category: str = Form(default=""),
    amount: str = Form(default="0"),
    paid_by: str = Form(default=""),
    comment: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_finance_tables()
    form_data = {
        "expense_date": expense_date,
        "expense_category": category,
        "expense_amount": amount,
        "expense_paid_by": paid_by,
        "expense_comment": comment,
    }
    if safe_cap_number(amount) <= 0:
        return HTMLResponse(finance_page_html(user, error_text="Сумма расхода должна быть больше 0.", form_data=form_data), status_code=400)
    db = SessionLocal()
    try:
        item = db.query(FinanceExpenseRow).filter(FinanceExpenseRow.id == safe_number(edit_id)).first() if edit_id else None
        if not item:
            item = FinanceExpenseRow()
            db.add(item)
        item.expense_date = safe_text(expense_date)
        item.category = safe_text(category)
        item.amount = safe_cap_number(amount)
        item.paid_by = safe_text(paid_by)
        item.comment = safe_text(comment)
        db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/finance?message=Расход сохранен", status_code=303)


@app.post("/finance/income/save")
def save_finance_income(
    request: Request,
    edit_id: str = Form(default=""),
    income_date: str = Form(default=""),
    category: str = Form(default=""),
    description: str = Form(default=""),
    amount: str = Form(default="0"),
    wallet: str = Form(default=""),
    reconciliation: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_finance_tables()
    form_data = {
        "income_date": income_date,
        "income_category": category,
        "income_description": description,
        "income_amount": amount,
        "income_wallet": wallet,
        "income_reconciliation": reconciliation,
    }
    if safe_cap_number(amount) <= 0:
        return HTMLResponse(finance_page_html(user, error_text="Сумма прихода должна быть больше 0.", form_data=form_data), status_code=400)
    db = SessionLocal()
    try:
        item = db.query(FinanceIncomeRow).filter(FinanceIncomeRow.id == safe_number(edit_id)).first() if edit_id else None
        if not item:
            item = FinanceIncomeRow()
            db.add(item)
        item.income_date = safe_text(income_date)
        item.category = safe_text(category)
        item.description = safe_text(description)
        item.amount = safe_cap_number(amount)
        item.wallet = safe_text(wallet)
        item.reconciliation = safe_text(reconciliation)
        db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/finance?message=Приход сохранен", status_code=303)


@app.post("/finance/wallets/delete")
def delete_finance_wallet(request: Request, wallet_id: str = Form(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_finance_tables()
    db = SessionLocal()
    try:
        db.query(FinanceWalletRow).filter(FinanceWalletRow.id == safe_number(wallet_id)).delete()
        db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/finance?message=Кошелек удален", status_code=303)


@app.post("/finance/expenses/delete")
def delete_finance_expense(request: Request, expense_id: str = Form(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_finance_tables()
    db = SessionLocal()
    try:
        db.query(FinanceExpenseRow).filter(FinanceExpenseRow.id == safe_number(expense_id)).delete()
        db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/finance?message=Расход удален", status_code=303)


@app.post("/finance/income/delete")
def delete_finance_income(request: Request, income_id: str = Form(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_finance_tables()
    db = SessionLocal()
    try:
        db.query(FinanceIncomeRow).filter(FinanceIncomeRow.id == safe_number(income_id)).delete()
        db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/finance?message=Приход удален", status_code=303)


@app.get("/caps", response_class=HTMLResponse)
def caps_page(
    request: Request,
    search: str = Query(default=""),
    buyer: str = Query(default=""),
    geo: str = Query(default=""),
    owner_name: str = Query(default=""),
    edit: str = Query(default=""),
    message: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "caps")
    import_caps_from_csv_if_needed()

    rows = get_caps_rows(search=search, buyer=buyer, geo=geo, owner_name=owner_name)
    form_data = {}
    if edit:
        db = SessionLocal()
        try:
            item = db.query(CapRow).filter(CapRow.id == safe_number(edit)).first()
            if item:
                form_data = {
                    "edit_id": str(item.id),
                    "advertiser": item.advertiser or "",
                    "owner_name": item.owner_name or "",
                    "buyer": item.buyer or "",
                    "flow": item.flow or "",
                    "code": item.code or "",
                    "geo": item.geo or "",
                    "rate": item.rate or "",
                    "baseline": item.baseline or "",
                    "cap_value": format_int_or_float(item.cap_value),
                    "current_ftd": format_int_or_float(item.current_ftd),
                    "promo_code": item.promo_code or "",
                    "kpi": item.kpi or "",
                    "link": item.link or "",
                    "comments": item.comments or "",
                    "agent": item.agent or "",
                }
        finally:
            db.close()
    return caps_page_html(
        user,
        rows,
        filter_values={"search": search, "buyer": buyer, "geo": geo, "owner_name": owner_name},
        form_data=form_data,
        success_text=message,
    )


@app.post("/caps/save")
def save_cap(
    request: Request,
    edit_id: str = Form(default=""),
    advertiser: str = Form(default=""),
    owner_name: str = Form(default=""),
    buyer: str = Form(...),
    flow: str = Form(default=""),
    code: str = Form(default=""),
    geo: str = Form(default=""),
    rate: str = Form(default=""),
    baseline: str = Form(default=""),
    cap_value: str = Form(...),
    current_ftd: str = Form(default="0"),
    promo_code: str = Form(default=""),
    kpi: str = Form(default=""),
    link: str = Form(default=""),
    comments: str = Form(default=""),
    agent: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "caps")

    clean_buyer = safe_text(buyer)
    clean_cap_value = safe_cap_number(cap_value)
    form_data = {
        "edit_id": edit_id,
        "advertiser": advertiser,
        "owner_name": owner_name,
        "buyer": buyer,
        "flow": flow,
        "code": code,
        "geo": geo,
        "rate": rate,
        "baseline": baseline,
        "cap_value": cap_value,
        "current_ftd": current_ftd,
        "promo_code": promo_code,
        "kpi": kpi,
        "link": link,
        "comments": comments,
        "agent": agent,
    }
    if not clean_buyer:
        return HTMLResponse(caps_page_html(user, get_caps_rows(), form_data=form_data, error_text="Поле Кабинет обязательно."), status_code=400)
    if clean_cap_value <= 0:
        return HTMLResponse(caps_page_html(user, get_caps_rows(), form_data=form_data, error_text="Капа должна быть больше 0."), status_code=400)

    db = SessionLocal()
    try:
        item = db.query(CapRow).filter(CapRow.id == safe_number(edit_id)).first() if edit_id else None
        if not item:
            item = CapRow()
            db.add(item)

        item.advertiser = safe_text(advertiser)
        item.owner_name = safe_text(owner_name)
        item.buyer = clean_buyer
        item.flow = safe_text(flow)
        item.code = normalize_geo_value(code)
        item.geo = normalize_geo_value(geo)
        item.rate = safe_text(rate)
        item.baseline = safe_text(baseline)
        item.cap_value = clean_cap_value
        item.current_ftd = safe_cap_number(current_ftd)
        item.promo_code = safe_text(promo_code)
        item.kpi = safe_text(kpi)
        item.link = safe_text(link)
        item.comments = safe_text(comments)
        item.agent = safe_text(agent)
        db.commit()
    finally:
        db.close()

    return RedirectResponse(url="/caps?message=Капа сохранена", status_code=303)


@app.post("/caps/upload")
async def upload_caps_file(request: Request, file: UploadFile = File(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "caps")
    original_name = file.filename or "caps.csv"
    ext = os.path.splitext(original_name)[1].lower() or ".csv"
    filename = f"temp_caps_{uuid.uuid4()}{ext}"
    try:
        with open(filename, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(filename)
        else:
            df = pd.read_csv(filename)
        import_caps_dataframe(df)
        refresh_cap_current_ftd_from_partner()
        return RedirectResponse(url="/caps?message=Капы загружены", status_code=303)
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@app.post("/caps/delete")
def delete_cap(request: Request, cap_id: str = Form(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "caps")
    db = SessionLocal()
    try:
        db.query(CapRow).filter(CapRow.id == safe_number(cap_id)).delete()
        db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/caps?message=Капа удалена", status_code=303)

@app.get("/api/partner/current-period")
def api_partner_current_period(request: Request):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    if not is_admin_role(user):
        raise HTTPException(status_code=403)
    return get_half_month_period()


@app.post("/api/partner/import")
async def api_partner_import(
    request: Request,
    file: UploadFile = File(...),
    source_name: str = Form(default="1xbet_players"),
    date_start: str = Form(default=""),
    date_end: str = Form(default=""),
    period_mode: str = Form(default="half_month"),
    api_key: str = Header(default="", alias="X-API-Key"),
):
    user = get_current_user(request)
    authorized = bool(user and is_admin_role(user))
    if not authorized and PARTNER_IMPORT_API_KEY:
        authorized = api_key == PARTNER_IMPORT_API_KEY

    if not authorized:
        if user:
            raise HTTPException(status_code=403)
        raise HTTPException(status_code=401)

    ensure_partner_table()
    ensure_upload_dir()

    original_name = file.filename or "partner_report.xlsx"
    ext = os.path.splitext(original_name)[1].lower() or ".xlsx"
    temp_name = f"partner_{uuid.uuid4()}{ext}"
    temp_path = os.path.join(PARTNER_UPLOAD_DIR, temp_name)

    try:
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(temp_path)
        else:
            try:
                df = pd.read_csv(temp_path)
            except Exception:
                df = pd.read_csv(temp_path, sep=";")

        detected_period = detect_partner_period_from_dataframe(df)
        if period_mode == "half_month":
            period = normalize_partner_period(date_start, date_end)
        else:
            period = detected_period or normalize_partner_period(date_start, date_end)

        final_source_name = build_partner_source_name(
            period["date_start"],
            period["date_end"],
            prefix=safe_text(source_name) or "1xbet_players",
        )

        rows = parse_partner_dataframe(df, source_name=final_source_name)
        replace_partner_rows(final_source_name, rows)

        return {
            "status": "ok",
            "inserted": len(rows),
            "source_name": final_source_name,
            "date_start": period["date_start"],
            "date_end": period["date_end"],
            "period_label": period["period_label"],
            "stored_file": temp_path,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Не удалось обработать файл партнера: {exc}")
@app.post("/1xbet-report/parser/start-auth")
async def start_onexbet_auth(request: Request):
    user = require_login(request)
    enforce_page_access(user, "onexbet_report")
    payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    account_ids = payload.get("account_ids") if isinstance(payload, dict) else []
    return JSONResponse({"ok": True, "state": start_onexbet_parser_job("auth", account_ids=account_ids, launched_by=user)})


@app.post("/1xbet-report/parser/start-run")
async def start_onexbet_run(request: Request):
    user = require_login(request)
    enforce_page_access(user, "onexbet_report")
    payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    account_ids = payload.get("account_ids") if isinstance(payload, dict) else []
    return JSONResponse({"ok": True, "state": start_onexbet_parser_job("run", account_ids=account_ids, launched_by=user)})


@app.get("/1xbet-report/parser-status")
def onexbet_parser_status(request: Request):
    user = require_login(request)
    enforce_page_access(user, "onexbet_report")
    return JSONResponse(get_onexbet_parser_state())


@app.post("/1xbet-report/parser/reset-sessions")
async def onexbet_reset_sessions(request: Request):
    user = require_login(request)
    enforce_page_access(user, "onexbet_report")
    payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    account_ids = payload.get("account_ids") if isinstance(payload, dict) else []
    return JSONResponse({"ok": True, **reset_onexbet_sessions(account_ids=account_ids)})


@app.post("/1xbet-report/parser/stop")
def onexbet_stop_parser(request: Request):
    user = require_login(request)
    enforce_page_access(user, "onexbet_report")
    return JSONResponse({"ok": True, "state": stop_onexbet_parser_job()})


@app.post("/1xbet-report/run-parser")
def run_onexbet_parser(request: Request):
    user = require_login(request)
    enforce_page_access(user, "onexbet_report")
    try:
        start_onexbet_parser_job("run", launched_by=user)
        return RedirectResponse(
            url="/1xbet-report?run_ok=Парсер запущен в фоне. Смотри live log ниже.",
            status_code=303,
        )
    except Exception as e:
        return RedirectResponse(
            url="/1xbet-report?run_error=" + urlencode({"v": str(e)})[2:],
            status_code=303,
        )
@app.get("/1xbet-report", response_class=HTMLResponse)
@app.get("/1xbet-report", response_class=HTMLResponse)
def onexbet_report_page(
    request: Request,
    period: str = Query(default=""),
    country: str = Query(default=""),
    sub_id: str = Query(default=""),
    search: str = Query(default=""),
    run_ok: str = Query(default=""),
    run_error: str = Query(default=""),
):
    user = require_login(request)
    enforce_page_access(user, "onexbet_report")
    parser_state = get_onexbet_parser_state()
    account_cards_html = ""
    parser_accounts = parser_state.get("accounts") or []
    for account in parser_accounts:
        account_status_map = parser_state.get("account_statuses") or {}
        account_status = account_status_map.get(account["id"], {})
        account_status_text = escape(account_status.get("status") or ("authorized" if account.get("session_saved") else "idle"))
        account_cards_html += f"""
        <label class="user-chip" style="display:grid;gap:10px;padding:12px 14px;">
            <span style="display:flex;align-items:center;gap:10px;justify-content:space-between;">
                <span style="display:flex;align-items:center;gap:10px;">
                <input type="checkbox" class="onexbet-account-checkbox" value="{escape(account["id"])}" checked>
                <span>
                    <strong>{escape(account["label"])}</strong><br>
                    <span style="color:var(--muted);font-size:12px;">{escape(account["login"])}</span>
                </span>
            </span>
                <span style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
                    <span class="user-chip" id="status-chip-{escape(account["id"])}">{account_status_text}</span>
                    <span class="user-chip" id="session-chip-{escape(account["id"])}">{'Session Saved' if account.get("session_saved") else 'No Session'}</span>
                </span>
            </span>
            <span style="display:flex;gap:8px;flex-wrap:wrap;">
                <button type="button" class="ghost-btn small-btn" onclick="startSingleOnexbetJob('auth', '{escape(account["id"])}')">Auth</button>
                <button type="button" class="ghost-btn small-btn" onclick="startSingleOnexbetJob('run', '{escape(account["id"])}')">Run</button>
                <button type="button" class="ghost-btn small-btn" onclick="resetSingleOnexbetSession('{escape(account["id"])}')">Reset Session</button>
            </span>
        </label>
        """
    if not account_cards_html:
        account_cards_html = '<div class="notice notice-danger">Кабинеты 1xBet не найдены. Добавь accounts.json.</div>'
    history_rows_html = ""
    for item in parser_state.get("history") or []:
        accounts_text = ", ".join(item.get("selected_account_ids") or []) or "all"
        history_rows_html += f"""
        <tr>
            <td>{escape(item.get("started_at") or "")}</td>
            <td>{escape(item.get("launched_by") or item.get("launched_by_username") or "")}</td>
            <td>{escape(item.get("mode") or "")}</td>
            <td>{escape(item.get("status") or "")}</td>
            <td>{escape(accounts_text)}</td>
            <td>{escape(item.get("message") or item.get("error") or "")}</td>
        </tr>
        """
    if not history_rows_html:
        history_rows_html = '<tr><td colspan="6" style="text-align:center;padding:18px;">История запусков пока пуста</td></tr>'

    all_periods = get_partner_period_options()
    selected_period = period or (all_periods[0] if all_periods else "")

    rows = get_partner_rows_by_period(selected_period)

    if country:
        rows = [r for r in rows if (r.country or "") == country]

    if sub_id:
        rows = [r for r in rows if (r.sub_id or "") == sub_id]

    search_lower = (search or "").strip().lower()
    if search_lower:
        filtered = []
        for r in rows:
            haystack = " | ".join([
                r.source_name or "",
                r.sub_id or "",
                r.player_id or "",
                r.registration_date or "",
                r.country or "",
                r.hold_time or "",
                r.blocked or "",
            ]).lower()
            if search_lower in haystack:
                filtered.append(r)
        rows = filtered

    totals = aggregate_partner_totals(rows)

    source_rows = get_partner_rows_by_period(selected_period)
    country_options = sorted({r.country for r in source_rows if r.country})
    subid_options = sorted({r.sub_id for r in source_rows if r.sub_id})

    period_options_html = "".join([
        f'<option value="{escape(p)}" {"selected" if p == selected_period else ""}>{escape(p)}</option>'
        for p in all_periods
    ])

    country_options_html = '<option value="">Все</option>' + "".join([
        f'<option value="{escape(v)}" {"selected" if v == country else ""}>{escape(v)}</option>'
        for v in country_options
    ])

    subid_options_html = '<option value="">Все</option>' + "".join([
        f'<option value="{escape(v)}" {"selected" if v == sub_id else ""}>{escape(v)}</option>'
        for v in subid_options
    ])

    table_rows = ""
    for r in rows:
        has_ftd = safe_number(r.deposit_amount) > 0
        row_class = "good-row" if has_ftd else ""
        table_rows += f"""
        <tr class="{row_class}">
            <td>{escape(r.source_name or "")}</td>
            <td>{escape(r.sub_id or "")}</td>
            <td>{escape(r.player_id or "")}</td>
            <td>{escape(r.registration_date or "")}</td>
            <td>{escape(r.country or "")}</td>
            <td>{format_money(r.deposit_amount)}</td>
            <td>{format_money(r.bet_amount)}</td>
            <td>{format_money(r.company_income)}</td>
            <td>{format_money(r.cpa_amount)}</td>
            <td>{escape(r.hold_time or "")}</td>
            <td>{escape(r.blocked or "")}</td>
        </tr>
        """

    if not table_rows:
        table_rows = """
        <tr>
            <td colspan="11" style="text-align:center;padding:24px;">Нет данных по выбранному периоду</td>
        </tr>
        """

    notice_html = ""
    if run_ok:
        notice_html = f'<div class="notice">{escape(run_ok)}</div>'
    elif run_error:
        notice_html = f'<div class="notice notice-danger">{escape(run_error)}</div>'

    status_labels = {
        "idle": "Idle",
        "starting": "Starting",
        "running": "Running",
        "success": "Completed",
        "error": "Error",
        "stopped": "Stopped",
    }
    status_value = parser_state.get("status", "idle")
    status_label = status_labels.get(status_value, status_value.title())
    mode_label = "Authorization" if parser_state.get("mode") == "auth" else ("Parser Run" if parser_state.get("mode") == "run" else "Not started")
    session_label = "Saved" if parser_state.get("session_saved") else "Missing"
    log_html = escape(parser_state.get("log_tail") or "Live log will appear here after you start authorization or parser run.")
    action_hint = ""

    extra_scripts = """
    <script>
        function escapeHtml(value) {
            return String(value || '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
        }

        async function refreshOnexbetState() {
            try {
                const res = await fetch('/1xbet-report/parser-status', { credentials: 'same-origin' });
                if (!res.ok) return;
                const data = await res.json();
                const statusEl = document.getElementById('onexbetStatusValue');
                const modeEl = document.getElementById('onexbetModeValue');
                const sessionEl = document.getElementById('onexbetSessionValue');
                const messageEl = document.getElementById('onexbetMessage');
                const logEl = document.getElementById('onexbetLog');
                const actionEl = document.getElementById('onexbetActionHint');
                const authBtn = document.getElementById('onexbetAuthBtn');
                const runBtn = document.getElementById('onexbetRunBtn');
                const stopBtn = document.getElementById('onexbetStopBtn');
                const currentAccountEl = document.getElementById('onexbetCurrentAccount');
                const launchedByEl = document.getElementById('onexbetLaunchedBy');
                const historyBodyEl = document.getElementById('onexbetHistoryBody');

                const statusLabels = {
                    idle: 'Idle',
                    starting: 'Starting',
                    running: 'Running',
                    success: 'Completed',
                    error: 'Error',
                    stopped: 'Stopped'
                };

                if (statusEl) statusEl.textContent = statusLabels[data.status] || data.status || 'Idle';
                if (modeEl) modeEl.textContent = data.mode === 'auth' ? 'Authorization' : (data.mode === 'run' ? 'Parser Run' : 'Not started');
                if (sessionEl) sessionEl.textContent = data.session_saved ? 'Saved' : 'Missing';
                if (currentAccountEl) currentAccountEl.textContent = data.current_account_label || 'No active account';
                if (launchedByEl) launchedByEl.textContent = data.launched_by || data.launched_by_username || 'Unknown';
                if (messageEl) messageEl.textContent = data.message || 'No updates yet';
                if (logEl) {
                    logEl.textContent = data.log_tail || 'Live log will appear here after you start authorization or parser run.';
                    logEl.scrollTop = logEl.scrollHeight;
                }
                if (authBtn) authBtn.disabled = !!data.running;
                if (runBtn) runBtn.disabled = !!data.running;
                if (stopBtn) stopBtn.disabled = !data.running;
                if (actionEl) actionEl.style.display = 'none';
                (data.accounts || []).forEach(account => {
                    const chip = document.getElementById('session-chip-' + account.id);
                    if (chip) chip.textContent = account.session_saved ? 'Session Saved' : 'No Session';
                    const statusChip = document.getElementById('status-chip-' + account.id);
                    const accountStatus = (data.account_statuses || {})[account.id] || {};
                    if (statusChip) statusChip.textContent = accountStatus.status || (account.session_saved ? 'authorized' : 'idle');
                });
                if (historyBodyEl) {
                    const rows = (data.history || []).map(item => {
                        const accountsText = (item.selected_account_ids || []).join(', ') || 'all';
                        const msg = item.message || item.error || '';
                        return '<tr>'
                            + '<td>' + escapeHtml(item.started_at || '') + '</td>'
                            + '<td>' + escapeHtml(item.launched_by || item.launched_by_username || '') + '</td>'
                            + '<td>' + escapeHtml(item.mode || '') + '</td>'
                            + '<td>' + escapeHtml(item.status || '') + '</td>'
                            + '<td>' + escapeHtml(accountsText) + '</td>'
                            + '<td>' + escapeHtml(msg) + '</td>'
                            + '</tr>';
                    });
                    historyBodyEl.innerHTML = rows.length
                        ? rows.join('')
                        : '<tr><td colspan="6" style="text-align:center;padding:18px;">История запусков пока пуста</td></tr>';
                }
            } catch (e) {
                console.error(e);
            }
        }

        function getSelectedOnexbetAccounts() {
            return Array.from(document.querySelectorAll('.onexbet-account-checkbox:checked')).map(el => el.value);
        }

        function setAllOnexbetAccounts(checked) {
            document.querySelectorAll('.onexbet-account-checkbox').forEach(el => {
                el.checked = checked;
            });
        }

        async function resetOnexbetSessions() {
            const accountIds = getSelectedOnexbetAccounts();
            if (!accountIds.length) {
                alert('Выбери хотя бы один кабинет 1xBet');
                return;
            }
            try {
                const res = await fetch('/1xbet-report/parser/reset-sessions', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'same-origin',
                    body: JSON.stringify({ account_ids: accountIds })
                });
                const data = await res.json();
                if (!res.ok) {
                    throw new Error(data.detail || 'Не удалось сбросить сессии');
                }
            } catch (e) {
                alert(e.message || 'Не удалось сбросить сессии');
            }
            refreshOnexbetState();
        }

        async function resetSingleOnexbetSession(accountId) {
            const currentBoxes = document.querySelectorAll('.onexbet-account-checkbox');
            currentBoxes.forEach(el => { el.checked = el.value === accountId; });
            await resetOnexbetSessions();
        }

        async function startOnexbetJob(mode) {
            const endpoint = mode === 'auth'
                ? '/1xbet-report/parser/start-auth'
                : '/1xbet-report/parser/start-run';
            const accountIds = getSelectedOnexbetAccounts();
            if (!accountIds.length) {
                alert('Выбери хотя бы один кабинет 1xBet');
                return;
            }

            const authBtn = document.getElementById('onexbetAuthBtn');
            const runBtn = document.getElementById('onexbetRunBtn');
            if (authBtn) authBtn.disabled = true;
            if (runBtn) runBtn.disabled = true;

            try {
                const res = await fetch(endpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'same-origin'
                    ,
                    body: JSON.stringify({ account_ids: accountIds })
                });
                const data = await res.json();
                if (!res.ok) {
                    throw new Error(data.detail || 'Не удалось запустить процесс');
                }
            } catch (e) {
                alert(e.message || 'Не удалось запустить процесс');
            }

            refreshOnexbetState();
        }

        async function startSingleOnexbetJob(mode, accountId) {
            const currentBoxes = document.querySelectorAll('.onexbet-account-checkbox');
            currentBoxes.forEach(el => { el.checked = el.value === accountId; });
            await startOnexbetJob(mode);
        }

        async function stopOnexbetJob() {
            try {
                const res = await fetch('/1xbet-report/parser/stop', {
                    method: 'POST',
                    credentials: 'same-origin'
                });
                const data = await res.json();
                if (!res.ok) {
                    throw new Error(data.detail || 'Не удалось остановить процесс');
                }
            } catch (e) {
                alert(e.message || 'Не удалось остановить процесс');
            }
            refreshOnexbetState();
        }

        window.startOnexbetJob = startOnexbetJob;
        window.startSingleOnexbetJob = startSingleOnexbetJob;
        window.setAllOnexbetAccounts = setAllOnexbetAccounts;
        window.resetOnexbetSessions = resetOnexbetSessions;
        window.resetSingleOnexbetSession = resetSingleOnexbetSession;
        window.stopOnexbetJob = stopOnexbetJob;
        refreshOnexbetState();
        setInterval(refreshOnexbetState, 2500);
    </script>
    """

    content = f"""
    {notice_html}

    <div class="panel">
        <div class="controls-line">
            <div>
                <div class="panel-title">1xBet Report</div>
                <div class="panel-subtitle">Run headless checks and imports from here. Accounts with captcha will fail in server mode.</div>
            </div>
            <div style="display:flex;gap:10px;flex-wrap:wrap;">
                <button type="button" class="ghost-btn" id="onexbetAuthBtn" onclick="startOnexbetJob('auth')">Check Session</button>
                <button type="button" class="btn" id="onexbetRunBtn" onclick="startOnexbetJob('run')">Run Parser</button>
                <button type="button" class="ghost-btn" id="onexbetStopBtn" onclick="stopOnexbetJob()" disabled>Stop</button>
            </div>
        </div>
    </div>

    <div class="panel">
        <div class="controls-line" style="align-items:flex-start;gap:16px;flex-wrap:wrap;">
            <div style="flex:1;min-width:260px;">
                <div class="panel-title" style="font-size:16px;">Parser Monitor</div>
                <div class="panel-subtitle" id="onexbetMessage" style="font-size:12px;line-height:1.45;">{escape(parser_state.get("message") or "Парсер не запущен")}</div>
                {action_hint}
                <div class="stats-grid" style="margin-top:14px;">
                    <div class="stat-card">
                        <div class="name">Status</div>
                        <div class="value" id="onexbetStatusValue" style="font-size:13px;">{escape(status_label)}</div>
                    </div>
                    <div class="stat-card">
                        <div class="name">Mode</div>
                        <div class="value" id="onexbetModeValue" style="font-size:13px;">{escape(mode_label)}</div>
                    </div>
                    <div class="stat-card">
                        <div class="name">Session</div>
                        <div class="value" id="onexbetSessionValue" style="font-size:13px;">{escape(session_label)}</div>
                    </div>
                    <div class="stat-card">
                        <div class="name">Current Account</div>
                        <div class="value" id="onexbetCurrentAccount" style="font-size:13px;">{escape(parser_state.get("current_account_label") or "No active account")}</div>
                    </div>
                    <div class="stat-card">
                        <div class="name">Launched By</div>
                        <div class="value" id="onexbetLaunchedBy" style="font-size:13px;">{escape(parser_state.get("launched_by") or parser_state.get("launched_by_username") or "Unknown")}</div>
                    </div>
                </div>
            </div>
            <div style="flex:1.4;min-width:320px;">
                <div class="panel-subtitle" style="margin-bottom:8px;font-size:12px;">Live Log</div>
                <pre id="onexbetLog" style="margin:0;min-height:220px;max-height:320px;overflow:auto;padding:12px;border-radius:16px;background:var(--table-head);border:1px solid var(--border);white-space:pre-wrap;line-height:1.35;font-size:12px;">{log_html}</pre>
            </div>
        </div>
        <div style="margin-top:16px;display:grid;gap:10px;">
            <div class="controls-line" style="gap:10px;flex-wrap:wrap;">
                <div class="panel-subtitle" style="margin:0;">Accounts</div>
                <div style="display:flex;gap:8px;flex-wrap:wrap;">
                    <button type="button" class="ghost-btn small-btn" onclick="setAllOnexbetAccounts(true)">Select All</button>
                    <button type="button" class="ghost-btn small-btn" onclick="setAllOnexbetAccounts(false)">Clear All</button>
                    <button type="button" class="ghost-btn small-btn" onclick="resetOnexbetSessions()">Reset Session</button>
                </div>
            </div>
            {account_cards_html}
        </div>
        <div style="margin-top:16px;">
            <div class="panel-subtitle" style="margin-bottom:8px;">Recent Runs</div>
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>
                            <th>Started</th>
                            <th>By</th>
                            <th>Mode</th>
                            <th>Status</th>
                            <th>Accounts</th>
                            <th>Message</th>
                        </tr>
                    </thead>
                    <tbody id="onexbetHistoryBody">{history_rows_html}</tbody>
                </table>
            </div>
        </div>
    </div>

    <div class="panel compact-panel filters">
        <form method="get">
            <label>Период
                <select name="period" required>
                    {period_options_html}
                </select>
            </label>

            <label>Страна
                <select name="country">
                    {country_options_html}
                </select>
            </label>

            <label>Sub ID
                <select name="sub_id">
                    {subid_options_html}
                </select>
            </label>

            <label>Поиск
                <input type="text" name="search" value="{escape(search)}" placeholder="player id / sub id">
            </label>

            <button type="submit" class="btn small-btn">Show</button>
            <a href="/1xbet-report" class="ghost-btn small-btn">Reset</a>
        </form>
    </div>

    <div class="stats-grid" style="margin-bottom:16px;">
        <div class="stat-card">
            <div class="name">Players</div>
            <div class="value">{totals["players"]}</div>
        </div>
        <div class="stat-card">
            <div class="name">FTD Count</div>
            <div class="value">{int(totals["ftd_count"])}</div>
        </div>
        <div class="stat-card">
            <div class="name">Deposits</div>
            <div class="value">{format_money(totals["deposits"])}</div>
        </div>
        <div class="stat-card">
            <div class="name">Bets</div>
            <div class="value">{format_money(totals["bets"])}</div>
        </div>
        <div class="stat-card">
            <div class="name">Income</div>
            <div class="value">{format_money(totals["income"])}</div>
        </div>
        <div class="stat-card">
            <div class="name">CPA</div>
            <div class="value">{format_money(totals["cpa"])}</div>
        </div>
    </div>

    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    <th>Period</th>
                    <th>Sub ID</th>
                    <th>Player ID</th>
                    <th>Registration</th>
                    <th>Country</th>
                    <th>Deposits</th>
                    <th>Bets</th>
                    <th>Income</th>
                    <th>CPA</th>
                    <th>Hold</th>
                    <th>Blocked</th>
                </tr>
            </thead>
            <tbody>
                {table_rows}
            </tbody>
        </table>
    </div>
    """

    return HTMLResponse(
        page_shell(
            "1xBet Report",
            content,
            active_page="onexbet_report",
            extra_scripts=extra_scripts,
            current_user=user,
        )
    )
@app.get("/chatterfy", response_class=HTMLResponse)
def chatterfy_page(
    request: Request,
    status: str = Query(default=""),
    search: str = Query(default=""),
    date_filter: str = Query(default=""),
    time_filter: str = Query(default=""),
    telegram_id: str = Query(default=""),
    pp_player_id: str = Query(default=""),
    sort_by: str = Query(default="started_date"),
    order: str = Query(default="desc"),
    page: int = Query(default=1),
    message: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "chatterfy")
    import_chatterfy_from_csv_if_needed()
    import_chatterfy_ids_from_csv_if_needed()
    rows = get_chatterfy_rows(
        status=status,
        search=search,
        date_filter=date_filter,
        time_filter=time_filter,
        telegram_id=telegram_id,
        pp_player_id=pp_player_id,
    )
    allowed_sort_fields = {
        "started_date",
        "started_time",
        "name",
        "telegram_id",
        "pp_player_id",
        "chat_link",
        "username",
        "tags",
        "launch_date",
        "platform",
        "manager",
        "geo",
        "offer",
        "status",
    }
    if sort_by not in allowed_sort_fields:
        sort_by = "started_date"
    reverse = order.lower() != "asc"

    def sort_value(item):
        row = item["row"]
        values = {
            "started_date": item.get("started_date") or "",
            "started_time": item.get("started_time") or "",
            "name": row.name or "",
            "telegram_id": row.telegram_id or "",
            "pp_player_id": item.get("pp_player_id") or "",
            "chat_link": "1" if item.get("chat_link") else "0",
            "username": row.username or "",
            "tags": row.tags or "",
            "launch_date": row.launch_date or "",
            "platform": row.platform or "",
            "manager": row.manager or "",
            "geo": row.geo or "",
            "offer": row.offer or "",
            "status": row.status or "",
        }
        value = values.get(sort_by, "")
        if sort_by in {"telegram_id", "pp_player_id"}:
            digits = re.sub(r"\\D", "", safe_text(value))
            return int(digits) if digits else 0
        return safe_text(value).lower()

    rows.sort(key=sort_value, reverse=reverse)
    total_count = len(rows)
    per_page = 100
    page = max(1, int(page or 1))
    start = (page - 1) * per_page
    page_rows = rows[start:start + per_page]
    return chatterfy_page_html(
        user,
        page_rows,
        status=status,
        search=search,
        date_filter=date_filter,
        time_filter=time_filter,
        telegram_id=telegram_id,
        pp_player_id=pp_player_id,
        sort_by=sort_by,
        order=order,
        page=page,
        total_count=total_count,
        per_page=per_page,
        success_text=message,
    )


@app.post("/chatterfy/upload")
async def upload_chatterfy_file(request: Request, file: UploadFile = File(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "chatterfy")
    original_name = file.filename or "chatterfy.csv"
    ext = os.path.splitext(original_name)[1].lower() or ".csv"
    filename = f"temp_chatterfy_{uuid.uuid4()}{ext}"
    try:
        with open(filename, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(filename)
        else:
            try:
                df = pd.read_csv(filename)
            except Exception:
                df = pd.read_csv(filename, sep=";")
        import_chatterfy_dataframe(df, original_name)
        return RedirectResponse(url="/chatterfy?message=Chatterfy загружен", status_code=303)
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@app.post("/chatterfy/upload-ids")
async def upload_chatterfy_ids_file(request: Request, file: UploadFile = File(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "chatterfy")
    original_name = file.filename or "chatterfy_ids.csv"
    ext = os.path.splitext(original_name)[1].lower() or ".csv"
    filename = f"temp_chatterfy_ids_{uuid.uuid4()}{ext}"
    try:
        with open(filename, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(filename)
        else:
            try:
                df = pd.read_csv(filename)
            except Exception:
                df = pd.read_csv(filename, sep=";")
        import_chatterfy_ids_dataframe(df)
        return RedirectResponse(url="/chatterfy?message=ID file загружен", status_code=303)
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@app.get("/hold-wager", response_class=HTMLResponse)
def hold_wager_page(request: Request):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "holdwager")
    return render_dev_page("Hold/Wager", "🎯", "holdwager", current_user=user)
