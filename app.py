from fastapi import FastAPI, UploadFile, File, Query, Form, Request, HTTPException, Header
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse, Response, FileResponse
from fastapi.exception_handlers import http_exception_handler
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text, or_, inspect
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
import shutil
import uuid
import os
import json
from urllib.parse import urlencode, quote_plus
from html import escape
import io
import csv
import secrets
import hashlib
import calendar
import re
import sys
from functools import lru_cache
from datetime import datetime, timedelta, date

# =========================================
# BLOCK 1 — DATABASE
# =========================================
RAW_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db").strip()
DATABASE_URL = RAW_DATABASE_URL.replace("postgres://", "postgresql://", 1) if RAW_DATABASE_URL.startswith("postgres://") else RAW_DATABASE_URL
SESSION_COOKIE_NAME = "teambead_session"
SESSION_DURATION_DAYS = 14
DATA_UPLOAD_DIR = "./uploaded_data"
STATIC_DIR = "./static"
FINANCE_UPLOAD_PATH = os.path.join(DATA_UPLOAD_DIR, "finance_latest.csv")
PARTNER_UPLOAD_DIR = os.path.join(DATA_UPLOAD_DIR, "partner_reports")
PARTNER_IMPORT_API_KEY = os.getenv("TEAMBEAD_PARTNER_IMPORT_KEY", "8hF9sK2LmQpX91zA")
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
        "role": "superadmin",
        "display_name": os.getenv("TEAMBEAD_ADMIN2_NAME", "Dmytro"),
        "legacy_usernames": ["admin2"],
    },
]

engine_kwargs = {"pool_pre_ping": True}
if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        **engine_kwargs,
    )
else:
    engine = create_engine(
        DATABASE_URL,
        **engine_kwargs,
    )
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
Base = declarative_base()
ENSURED_TABLES = set()
RUNTIME_INDEXES_READY = False
AUTO_IMPORT_CHECKS = set()
RUNTIME_CACHE = {}


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
    cabinet_name = Column(String, default="")
    flow = Column(String, default="")
    code = Column(String, default="")
    geo = Column(String, default="")
    rate = Column(String, default="")
    baseline = Column(String, default="")
    cap_value = Column(Float, default=0)
    promo_code = Column(String, default="")
    chat_title = Column(String, default="")
    kpi = Column(String, default="")
    link = Column(String, default="")
    comments = Column(String, default="")
    agent = Column(String, default="")
    period_label = Column(String, default="")
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
    wallet_name = Column(String, default="")
    amount = Column(Float, default=0)
    from_wallet = Column(String, default="")
    paid_by = Column(String, default="")
    comment = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class FinanceIncomeRow(Base):
    __tablename__ = "finance_income_rows"

    id = Column(Integer, primary_key=True, index=True)
    income_date = Column(String, default="")
    category = Column(String, default="")
    description = Column(String, default="")
    wallet_name = Column(String, default="")
    amount = Column(Float, default=0)
    wallet = Column(String, default="")
    from_wallet = Column(String, default="")
    comment = Column(String, default="")
    reconciliation = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class FinanceTransferRow(Base):
    __tablename__ = "finance_transfer_rows"

    id = Column(Integer, primary_key=True, index=True)
    transfer_date = Column(String, default="")
    category = Column(String, default="")
    amount = Column(Float, default=0)
    from_wallet = Column(String, default="")
    to_wallet = Column(String, default="")
    comment = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class PartnerRow(Base):
    __tablename__ = "partner_rows"

    id = Column(Integer, primary_key=True, index=True)
    source_name = Column(String, default="")
    cabinet_name = Column(String, default="")
    sub_id = Column(String, index=True, default="")
    player_id = Column(String, default="")
    report_date = Column(String, default="")
    period_start = Column(String, default="")
    period_end = Column(String, default="")
    period_label = Column(String, default="")
    registration_date = Column(String, default="")
    country = Column(String, default="")
    deposit_amount = Column(Float, default=0)
    bet_amount = Column(Float, default=0)
    company_income = Column(Float, default=0)
    cpa_amount = Column(Float, default=0)
    hold_time = Column(String, default="")
    blocked = Column(String, default="")
    manual_hold = Column(Integer, default=0)
    manual_blocked = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


class CabinetRow(Base):
    __tablename__ = "cabinet_rows"

    id = Column(Integer, primary_key=True, index=True)
    advertiser = Column(String, default="")
    platform = Column(String, default="")
    name = Column(String, unique=True, index=True, default="")
    geo_list = Column(String, default="")
    brands = Column(String, default="")
    team_name = Column(String, default="")
    manager_name = Column(String, default="")
    manager_contact = Column(String, default="")
    chat_name = Column(String, default="")
    wallet = Column(String, default="")
    comments = Column(String, default="")
    status = Column(String, default="Active")
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
    report_date = Column(String, default="")
    period_start = Column(String, default="")
    period_end = Column(String, default="")
    period_label = Column(String, default="")
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


def ensure_table_once(key: str, tables, sqlite_callback=None):
    if key in ENSURED_TABLES:
        return
    Base.metadata.create_all(bind=engine, tables=tables)
    if DATABASE_URL.startswith("sqlite") and sqlite_callback:
        sqlite_callback()
    ENSURED_TABLES.add(key)


def ensure_runtime_indexes():
    global RUNTIME_INDEXES_READY
    if RUNTIME_INDEXES_READY or DATABASE_URL.startswith("sqlite"):
        RUNTIME_INDEXES_READY = True
        return
    with engine.begin() as conn:
        index_statements = [
            "CREATE INDEX IF NOT EXISTS ix_fb_rows_scope ON fb_rows (uploader, manager, geo, offer)",
            "CREATE INDEX IF NOT EXISTS ix_fb_rows_dates ON fb_rows (date_start, date_end)",
            "CREATE INDEX IF NOT EXISTS ix_cap_rows_scope ON cap_rows (buyer, geo, owner_name)",
            "CREATE INDEX IF NOT EXISTS ix_cap_rows_promo ON cap_rows (promo_code)",
            "CREATE INDEX IF NOT EXISTS ix_partner_rows_scope ON partner_rows (source_name, period_label, cabinet_name, country)",
            "CREATE INDEX IF NOT EXISTS ix_partner_rows_lookup ON partner_rows (sub_id, player_id)",
            "CREATE INDEX IF NOT EXISTS ix_chatterfy_rows_scope ON chatterfy_rows (period_label, status, manager, geo, offer)",
            "CREATE INDEX IF NOT EXISTS ix_chatterfy_rows_lookup ON chatterfy_rows (telegram_id, external_id)",
            "CREATE INDEX IF NOT EXISTS ix_chatterfy_id_rows_lookup ON chatterfy_id_rows (telegram_id, pp_player_id)",
            "CREATE INDEX IF NOT EXISTS ix_task_rows_scope ON task_rows (assigned_to_username, status, due_at)",
            "CREATE INDEX IF NOT EXISTS ix_cabinet_rows_scope ON cabinet_rows (status, name, manager_name)",
            "CREATE INDEX IF NOT EXISTS ix_finance_wallet_rows_wallet ON finance_wallet_rows (wallet)",
        ]
        for statement in index_statements:
            conn.execute(text(statement))
    RUNTIME_INDEXES_READY = True


ensure_runtime_indexes()


def clear_runtime_cache(*prefixes):
    if not prefixes:
        RUNTIME_CACHE.clear()
        return
    keys = list(RUNTIME_CACHE.keys())
    for key in keys:
        for prefix in prefixes:
            if str(key).startswith(prefix):
                RUNTIME_CACHE.pop(key, None)
                break


def sync_postgres_sequence(table_name: str, pk_column: str = "id"):
    if DATABASE_URL.startswith("sqlite"):
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                SELECT setval(
                    pg_get_serial_sequence(:table_name, :pk_column),
                    COALESCE((SELECT MAX(id) FROM """ + table_name + """), 0) + 1,
                    false
                )
                """
            ),
            {"table_name": table_name, "pk_column": pk_column},
        )


def sync_all_postgres_sequences():
    if DATABASE_URL.startswith("sqlite"):
        return
    for table_name in [
        "fb_rows",
        "users",
        "user_sessions",
        "cap_rows",
        "task_rows",
        "finance_wallet_rows",
        "finance_expense_rows",
        "finance_income_rows",
        "finance_transfer_rows",
        "partner_rows",
        "cabinet_rows",
        "chatterfy_rows",
        "chatterfy_id_rows",
    ]:
        sync_postgres_sequence(table_name)


sync_all_postgres_sequences()


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
        "fb": {"superadmin", "admin", "buyer", "operator"},
        "grouped": {"superadmin", "admin", "buyer", "operator"},
        "hierarchy": {"superadmin", "admin", "buyer", "operator"},
        "tasks": {"superadmin", "admin", "buyer", "operator", "finance"},
        "users": {"superadmin", "admin"},
        "finance": {"superadmin"},
        "caps": {"superadmin", "admin"},
        "partner": {"superadmin", "admin"},
        "cabinets": {"superadmin", "admin"},
        "chatterfy": {"superadmin", "admin"},
        "holdwager": {"superadmin", "admin"},
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


def get_scoped_filter_options(user, period_label=""):
    buyer_scope = resolve_effective_buyer(user)
    rows = get_filtered_data(buyer=buyer_scope, period_label=period_label)
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
        <link rel="icon" type="image/jpeg" href="/favicon.jpg?v=3">
        <link rel="shortcut icon" href="/favicon.ico?v=3">
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
        return RedirectResponse(url="/dashboard", status_code=302)
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
    response = RedirectResponse(url="/dashboard", status_code=302)
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
    return RedirectResponse(url="/dashboard" if get_current_user(request) else "/login", status_code=302)


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


def format_plain_number_text(value):
    text = safe_text(value)
    if not text:
        return ""
    try:
        return format_int_or_float(float(text.replace(",", ".")))
    except Exception:
        return text



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


def normalize_id_value(value):
    return re.sub(r"\D", "", safe_text(value))


def safe_cap_number(value):
    text = safe_text(value).replace(",", ".")
    try:
        return float(text) if text else 0.0
    except Exception:
        return 0.0


def split_list_tokens(value):
    raw = safe_text(value)
    if not raw:
        return []
    parts = re.split(r"[,;\n]+", raw)
    result = []
    seen = set()
    for part in parts:
        item = safe_text(part)
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def normalize_geo_value(value):
    raw = safe_text(value)
    if not raw:
        return ""
    normalized = raw.strip().upper()
    geo_aliases = {
        "SPAIN": "ES",
        "ESPANA": "ES",
        "ESPAÑA": "ES",
        "PERU": "PE",
        "COLOMBIA": "CO",
        "CHILE": "CL",
        "ECUADOR": "EC",
        "BOLIVIA": "BO",
        "PARAGUAY": "PY",
        "URUGUAY": "UY",
        "VENEZUELA": "VE",
        "MEXICO": "MX",
        "BRAZIL": "BR",
        "PORTUGAL": "PT",
        "ARGENTINA": "AR",
        "INDIA": "IN",
    }
    return geo_aliases.get(normalized, normalized)


def geo_display_name(value):
    normalized = normalize_geo_value(value)
    display_map = {
        "ES": "Spain",
        "PE": "Peru",
        "CO": "Colombia",
        "CL": "Chile",
        "EC": "Ecuador",
        "BO": "Bolivia",
        "PY": "Paraguay",
        "UY": "Uruguay",
        "VE": "Venezuela",
        "MX": "Mexico",
        "BR": "Brazil",
        "PT": "Portugal",
        "AR": "Argentina",
        "IN": "India",
    }
    return display_map.get(normalized, safe_text(value).strip() or normalized)


def format_geo_list_codes(value):
    return ", ".join(split_geo_tokens(value))


def format_geo_list_names(value):
    return ", ".join(geo_display_name(token) for token in split_geo_tokens(value))


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


def parse_datetime_flexible(value):
    text = safe_text(value)
    if not text:
        return None
    for pattern in (
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(text, pattern)
        except Exception:
            continue
    return parse_chatterfy_datetime(text)


def get_half_month_period_from_date(value):
    dt = parse_datetime_flexible(value) if not isinstance(value, datetime) else value
    if not dt:
        return {"report_date": "", "period_start": "", "period_end": "", "period_label": ""}
    day_value = dt.day
    year = dt.year
    month = dt.month
    if day_value <= 15:
        start_day = 1
        end_day = 15
    else:
        start_day = 16
        end_day = calendar.monthrange(year, month)[1]
    period_start = date(year, month, start_day)
    period_end = date(year, month, end_day)
    return {
        "report_date": dt.strftime("%Y-%m-%d"),
        "period_start": period_start.strftime("%Y-%m-%d"),
        "period_end": period_end.strftime("%Y-%m-%d"),
        "period_label": f"{period_start.strftime('%d.%m.%Y')} - {period_end.strftime('%d.%m.%Y')}",
    }


@lru_cache(maxsize=8)
def build_period_options(start_year=2026, end_year=2027):
    options = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            last_day = calendar.monthrange(year, month)[1]
            first = date(year, month, 1)
            mid = date(year, month, 15)
            second = date(year, month, 16)
            end = date(year, month, last_day)
            options.append(f"{first.strftime('%d.%m.%Y')} - {mid.strftime('%d.%m.%Y')}")
            options.append(f"{second.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}")
    return options


def get_current_period_label(today=None):
    base_date = today or datetime.utcnow().date()
    period = get_half_month_period(today=base_date)
    return f"{datetime.strptime(period['date_start'], '%Y-%m-%d').strftime('%d.%m.%Y')} - {datetime.strptime(period['date_end'], '%Y-%m-%d').strftime('%d.%m.%Y')}"


def resolve_period_label(period_view="", period_label=""):
    clean_view = safe_text(period_view)
    clean_label = safe_text(period_label)
    if clean_view == "current":
        return get_current_period_label()
    if clean_view == "period":
        return clean_label
    return ""


def get_previous_period_label(period_label=""):
    clean_label = safe_text(period_label)
    if not clean_label:
        return ""
    options = build_period_options()
    try:
        index = options.index(clean_label)
    except ValueError:
        return ""
    if index <= 0:
        return ""
    return options[index - 1]


def fb_row_period_label(row):
    try:
        start_dt = datetime.strptime(safe_text(row.date_start), "%Y-%m-%d")
        end_dt = datetime.strptime(safe_text(row.date_end), "%Y-%m-%d")
        return f"{start_dt.strftime('%d.%m.%Y')} - {end_dt.strftime('%d.%m.%Y')}"
    except Exception:
        return ""


def partner_row_period_label(row):
    stored_label = safe_text(getattr(row, "period_label", ""))
    if stored_label:
        return stored_label
    period_start = safe_text(getattr(row, "period_start", ""))
    period_end = safe_text(getattr(row, "period_end", ""))
    if period_start and period_end:
        try:
            start_dt = datetime.strptime(period_start, "%Y-%m-%d")
            end_dt = datetime.strptime(period_end, "%Y-%m-%d")
            return f"{start_dt.strftime('%d.%m.%Y')} - {end_dt.strftime('%d.%m.%Y')}"
        except Exception:
            return ""
    return ""


def chatterfy_row_period_label(row):
    return safe_text(getattr(row, "period_label", "")) or get_half_month_period_from_date(getattr(row, "started", "")).get("period_label", "")


def cap_fill_percent(current_ftd, cap_value):
    cap_value = safe_number(cap_value)
    current_ftd = safe_number(current_ftd)
    if cap_value <= 0:
        return 0.0
    return (current_ftd / cap_value) * 100


def ensure_upload_dir():
    os.makedirs(DATA_UPLOAD_DIR, exist_ok=True)
    os.makedirs(PARTNER_UPLOAD_DIR, exist_ok=True)


def ensure_caps_table():
    def sqlite_migration():
        with engine.begin() as conn:
            columns = [row[1] for row in conn.execute(text("PRAGMA table_info(cap_rows)")).fetchall()]
            if "cabinet_name" not in columns:
                conn.execute(text("ALTER TABLE cap_rows ADD COLUMN cabinet_name VARCHAR DEFAULT ''"))
                conn.execute(text("UPDATE cap_rows SET cabinet_name = buyer WHERE COALESCE(cabinet_name, '') = ''"))
            if "period_label" not in columns:
                conn.execute(text("ALTER TABLE cap_rows ADD COLUMN period_label VARCHAR DEFAULT ''"))
                default_period = get_current_period_label()
                conn.execute(text("UPDATE cap_rows SET period_label = :period_label WHERE COALESCE(period_label, '') = ''"), {"period_label": default_period})
            if "chat_title" not in columns:
                conn.execute(text("ALTER TABLE cap_rows ADD COLUMN chat_title VARCHAR DEFAULT ''"))
    ensure_table_once("cap_rows", [CapRow.__table__], sqlite_migration)
    if not DATABASE_URL.startswith("sqlite"):
        inspector = inspect(engine)
        columns = {item.get("name") for item in inspector.get_columns("cap_rows")}
        migration_statements = []
        if "cabinet_name" not in columns:
            migration_statements.append(text("ALTER TABLE cap_rows ADD COLUMN IF NOT EXISTS cabinet_name VARCHAR DEFAULT ''"))
        if "period_label" not in columns:
            migration_statements.append(text("ALTER TABLE cap_rows ADD COLUMN IF NOT EXISTS period_label VARCHAR DEFAULT ''"))
        if "chat_title" not in columns:
            migration_statements.append(text("ALTER TABLE cap_rows ADD COLUMN IF NOT EXISTS chat_title VARCHAR DEFAULT ''"))
        if migration_statements:
            default_period = get_current_period_label()
            with engine.begin() as conn:
                for statement in migration_statements:
                    conn.execute(statement)
                if "cabinet_name" not in columns:
                    conn.execute(text("UPDATE cap_rows SET cabinet_name = buyer WHERE COALESCE(cabinet_name, '') = ''"))
                if "period_label" not in columns:
                    conn.execute(
                        text("UPDATE cap_rows SET period_label = :period_label WHERE COALESCE(period_label, '') = ''"),
                        {"period_label": default_period},
                    )


def ensure_task_table():
    ensure_table_once("task_rows", [TaskRow.__table__])


# =========================================
# BLOCK 5 — DATA ACCESS
# =========================================
def get_all_rows():
    db = SessionLocal()
    try:
        return db.query(FBRow).all()
    finally:
        db.close()



def get_filtered_data(buyer="", manager="", geo="", offer="", search="", period_label=""):
    db = SessionLocal()
    try:
        query = db.query(FBRow)
        if buyer:
            query = query.filter(FBRow.uploader == buyer)
        if manager:
            query = query.filter(FBRow.manager == manager)
        if geo:
            query = query.filter(FBRow.geo == geo)
        if offer:
            query = query.filter(FBRow.offer == offer)
        if search:
            search_pattern = f"%{safe_text(search).strip()}%"
            query = query.filter(or_(
                FBRow.ad_name.ilike(search_pattern),
                FBRow.platform.ilike(search_pattern),
                FBRow.manager.ilike(search_pattern),
                FBRow.geo.ilike(search_pattern),
                FBRow.offer.ilike(search_pattern),
                FBRow.creative.ilike(search_pattern),
                FBRow.uploader.ilike(search_pattern),
            ))
        rows = query.all()
    finally:
        db.close()

    filtered = []

    for row in rows:
        if period_label and fb_row_period_label(row) != period_label:
            continue
        filtered.append(row)
    return filtered



def get_filter_options():
    db = SessionLocal()
    try:
        buyers = sorted(value[0] for value in db.query(FBRow.uploader).distinct().all() if value[0])
        managers = sorted(value[0] for value in db.query(FBRow.manager).distinct().all() if value[0])
        geos = sorted(value[0] for value in db.query(FBRow.geo).distinct().all() if value[0])
        offers = sorted(value[0] for value in db.query(FBRow.offer).distinct().all() if value[0])
        return buyers, managers, geos, offers
    finally:
        db.close()


def get_caps_rows(search="", buyer="", code="", owner_name="", period_label=""):
    ensure_caps_table()
    ensure_partner_table()
    db = SessionLocal()
    try:
        query = db.query(CapRow)
        if period_label:
            query = query.filter(CapRow.period_label == period_label)
        if buyer:
            query = query.filter(CapRow.cabinet_name == buyer)
        if code:
            query = query.filter(CapRow.code == normalize_geo_value(code))
        if owner_name:
            query = query.filter(CapRow.owner_name == owner_name)
        if search:
            search_pattern = f"%{safe_text(search).strip()}%"
            query = query.filter(or_(
                CapRow.advertiser.ilike(search_pattern),
                CapRow.owner_name.ilike(search_pattern),
                CapRow.cabinet_name.ilike(search_pattern),
                CapRow.buyer.ilike(search_pattern),
                CapRow.flow.ilike(search_pattern),
                CapRow.code.ilike(search_pattern),
                CapRow.geo.ilike(search_pattern),
                CapRow.promo_code.ilike(search_pattern),
                CapRow.comments.ilike(search_pattern),
                CapRow.agent.ilike(search_pattern),
            ))
        return query.order_by(CapRow.cabinet_name.asc(), CapRow.code.asc(), CapRow.id.desc()).all()
    finally:
        db.close()


def get_caps_filter_options(period_label=""):
    ensure_caps_table()
    ensure_partner_table()
    ensure_cabinet_table()
    db = SessionLocal()
    try:
        cabinet_rows = db.query(CabinetRow).order_by(CabinetRow.name.asc(), CabinetRow.id.asc()).all()
        cabinets = []
        seen_cabinets = set()
        for row in cabinet_rows:
            value = safe_text(row.name).strip()
            if not value:
                continue
            key = value.lower()
            if key in seen_cabinets:
                continue
            seen_cabinets.add(key)
            cabinets.append(value)

        owners_query = db.query(CapRow)
        if period_label:
            owners_query = owners_query.filter(CapRow.period_label == period_label)
        caps = owners_query.all()
        codes = sorted({normalize_geo_value(value.code) for value in caps if value.code})
        owners = sorted({value.owner_name for value in caps if value.owner_name})
        return cabinets, codes, owners
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

    try:
        cache_key = f"finance_snapshot::{source_path}::{os.path.getmtime(source_path)}"
        cached = RUNTIME_CACHE.get(cache_key)
        if cached is not None:
            return cached
    except Exception:
        cache_key = ""

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

    if cache_key:
        clear_runtime_cache("finance_snapshot::")
        RUNTIME_CACHE[cache_key] = result
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
    ensure_task_table()
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
    def sqlite_migration():
        with engine.begin() as conn:
            columns = [row[1] for row in conn.execute(text("PRAGMA table_info(partner_rows)")).fetchall()]
            if "cabinet_name" not in columns:
                conn.execute(text("ALTER TABLE partner_rows ADD COLUMN cabinet_name VARCHAR DEFAULT ''"))
            if "report_date" not in columns:
                conn.execute(text("ALTER TABLE partner_rows ADD COLUMN report_date VARCHAR DEFAULT ''"))
            if "period_start" not in columns:
                conn.execute(text("ALTER TABLE partner_rows ADD COLUMN period_start VARCHAR DEFAULT ''"))
            if "period_end" not in columns:
                conn.execute(text("ALTER TABLE partner_rows ADD COLUMN period_end VARCHAR DEFAULT ''"))
            if "period_label" not in columns:
                conn.execute(text("ALTER TABLE partner_rows ADD COLUMN period_label VARCHAR DEFAULT ''"))
            if "manual_hold" not in columns:
                conn.execute(text("ALTER TABLE partner_rows ADD COLUMN manual_hold INTEGER DEFAULT 0"))
            if "manual_blocked" not in columns:
                conn.execute(text("ALTER TABLE partner_rows ADD COLUMN manual_blocked INTEGER DEFAULT 0"))
    ensure_table_once("partner_rows", [PartnerRow.__table__], sqlite_migration)


def ensure_cabinet_table():
    def sqlite_migration():
        with engine.begin() as conn:
            columns = [row[1] for row in conn.execute(text("PRAGMA table_info(cabinet_rows)")).fetchall()]
            if "advertiser" not in columns:
                conn.execute(text("ALTER TABLE cabinet_rows ADD COLUMN advertiser VARCHAR DEFAULT ''"))
            if "platform" not in columns:
                conn.execute(text("ALTER TABLE cabinet_rows ADD COLUMN platform VARCHAR DEFAULT ''"))
            if "name" not in columns:
                conn.execute(text("ALTER TABLE cabinet_rows ADD COLUMN name VARCHAR DEFAULT ''"))
                if "cabinet_name" in columns:
                    conn.execute(text("UPDATE cabinet_rows SET name = cabinet_name WHERE COALESCE(name, '') = ''"))
            if "geo_list" not in columns:
                conn.execute(text("ALTER TABLE cabinet_rows ADD COLUMN geo_list VARCHAR DEFAULT ''"))
            if "brands" not in columns:
                conn.execute(text("ALTER TABLE cabinet_rows ADD COLUMN brands VARCHAR DEFAULT ''"))
            if "team_name" not in columns:
                conn.execute(text("ALTER TABLE cabinet_rows ADD COLUMN team_name VARCHAR DEFAULT ''"))
            if "manager_name" not in columns:
                conn.execute(text("ALTER TABLE cabinet_rows ADD COLUMN manager_name VARCHAR DEFAULT ''"))
            if "manager_contact" not in columns:
                conn.execute(text("ALTER TABLE cabinet_rows ADD COLUMN manager_contact VARCHAR DEFAULT ''"))
            if "chat_name" not in columns:
                conn.execute(text("ALTER TABLE cabinet_rows ADD COLUMN chat_name VARCHAR DEFAULT ''"))
            if "wallet" not in columns:
                conn.execute(text("ALTER TABLE cabinet_rows ADD COLUMN wallet VARCHAR DEFAULT ''"))
                if "wallets" in columns:
                    conn.execute(text("UPDATE cabinet_rows SET wallet = wallets WHERE COALESCE(wallet, '') = ''"))
            if "comments" not in columns:
                conn.execute(text("ALTER TABLE cabinet_rows ADD COLUMN comments VARCHAR DEFAULT ''"))
            if "status" not in columns:
                conn.execute(text("ALTER TABLE cabinet_rows ADD COLUMN status VARCHAR DEFAULT 'Active'"))
                if "is_active" in columns:
                    conn.execute(text("UPDATE cabinet_rows SET status = CASE WHEN is_active = 1 THEN 'Active' ELSE 'Archived' END WHERE COALESCE(status, '') = ''"))
    ensure_table_once("cabinet_rows", [CabinetRow.__table__], sqlite_migration)
    if not DATABASE_URL.startswith("sqlite"):
        inspector = inspect(engine)
        columns = {item.get("name") for item in inspector.get_columns("cabinet_rows")}
        migration_statements = []
        if "advertiser" not in columns:
            migration_statements.append(text("ALTER TABLE cabinet_rows ADD COLUMN IF NOT EXISTS advertiser VARCHAR DEFAULT ''"))
        if "platform" not in columns:
            migration_statements.append(text("ALTER TABLE cabinet_rows ADD COLUMN IF NOT EXISTS platform VARCHAR DEFAULT ''"))
        if "name" not in columns:
            migration_statements.append(text("ALTER TABLE cabinet_rows ADD COLUMN IF NOT EXISTS name VARCHAR DEFAULT ''"))
        if "geo_list" not in columns:
            migration_statements.append(text("ALTER TABLE cabinet_rows ADD COLUMN IF NOT EXISTS geo_list VARCHAR DEFAULT ''"))
        if "brands" not in columns:
            migration_statements.append(text("ALTER TABLE cabinet_rows ADD COLUMN IF NOT EXISTS brands VARCHAR DEFAULT ''"))
        if "team_name" not in columns:
            migration_statements.append(text("ALTER TABLE cabinet_rows ADD COLUMN IF NOT EXISTS team_name VARCHAR DEFAULT ''"))
        if "manager_name" not in columns:
            migration_statements.append(text("ALTER TABLE cabinet_rows ADD COLUMN IF NOT EXISTS manager_name VARCHAR DEFAULT ''"))
        if "manager_contact" not in columns:
            migration_statements.append(text("ALTER TABLE cabinet_rows ADD COLUMN IF NOT EXISTS manager_contact VARCHAR DEFAULT ''"))
        if "chat_name" not in columns:
            migration_statements.append(text("ALTER TABLE cabinet_rows ADD COLUMN IF NOT EXISTS chat_name VARCHAR DEFAULT ''"))
        if "wallet" not in columns:
            migration_statements.append(text("ALTER TABLE cabinet_rows ADD COLUMN IF NOT EXISTS wallet VARCHAR DEFAULT ''"))
        if "comments" not in columns:
            migration_statements.append(text("ALTER TABLE cabinet_rows ADD COLUMN IF NOT EXISTS comments VARCHAR DEFAULT ''"))
        if "status" not in columns:
            migration_statements.append(text("ALTER TABLE cabinet_rows ADD COLUMN IF NOT EXISTS status VARCHAR DEFAULT 'Active'"))
        if migration_statements:
            with engine.begin() as conn:
                for statement in migration_statements:
                    conn.execute(statement)
                if "name" not in columns and "cabinet_name" in columns:
                    conn.execute(text("UPDATE cabinet_rows SET name = cabinet_name WHERE COALESCE(name, '') = ''"))
                if "wallet" not in columns and "wallets" in columns:
                    conn.execute(text("UPDATE cabinet_rows SET wallet = wallets WHERE COALESCE(wallet, '') = ''"))
                if "status" not in columns and "is_active" in columns:
                    conn.execute(text("UPDATE cabinet_rows SET status = CASE WHEN is_active = 1 THEN 'Active' ELSE 'Archived' END WHERE COALESCE(status, '') = ''"))


def ensure_chatterfy_table():
    def sqlite_migration():
        with engine.begin() as conn:
            columns = [row[1] for row in conn.execute(text("PRAGMA table_info(chatterfy_rows)")).fetchall()]
            if "report_date" not in columns:
                conn.execute(text("ALTER TABLE chatterfy_rows ADD COLUMN report_date VARCHAR DEFAULT ''"))
            if "period_start" not in columns:
                conn.execute(text("ALTER TABLE chatterfy_rows ADD COLUMN period_start VARCHAR DEFAULT ''"))
            if "period_end" not in columns:
                conn.execute(text("ALTER TABLE chatterfy_rows ADD COLUMN period_end VARCHAR DEFAULT ''"))
            if "period_label" not in columns:
                conn.execute(text("ALTER TABLE chatterfy_rows ADD COLUMN period_label VARCHAR DEFAULT ''"))
    ensure_table_once("chatterfy_rows", [ChatterfyRow.__table__], sqlite_migration)
    if not DATABASE_URL.startswith("sqlite"):
        inspector = inspect(engine)
        columns = {item.get("name") for item in inspector.get_columns("chatterfy_rows")}
        migration_statements = []
        if "report_date" not in columns:
            migration_statements.append(text("ALTER TABLE chatterfy_rows ADD COLUMN IF NOT EXISTS report_date VARCHAR DEFAULT ''"))
        if "period_start" not in columns:
            migration_statements.append(text("ALTER TABLE chatterfy_rows ADD COLUMN IF NOT EXISTS period_start VARCHAR DEFAULT ''"))
        if "period_end" not in columns:
            migration_statements.append(text("ALTER TABLE chatterfy_rows ADD COLUMN IF NOT EXISTS period_end VARCHAR DEFAULT ''"))
        if "period_label" not in columns:
            migration_statements.append(text("ALTER TABLE chatterfy_rows ADD COLUMN IF NOT EXISTS period_label VARCHAR DEFAULT ''"))
        if migration_statements:
            with engine.begin() as conn:
                for statement in migration_statements:
                    conn.execute(statement)


def ensure_chatterfy_id_table():
    ensure_table_once("chatterfy_id_rows", [ChatterfyIdRow.__table__])


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


def build_partner_source_name(date_start: str, date_end: str, prefix: str = "partner_players"):
    try:
        start_dt = datetime.strptime((date_start or "").strip(), "%Y-%m-%d")
        end_dt = datetime.strptime((date_end or "").strip(), "%Y-%m-%d")
        return f"{prefix}|{start_dt.strftime('%d')}-{end_dt.strftime('%d.%m.%Y')}"
    except Exception:
        if date_start and date_end:
            return f"{prefix}|{date_start}_{date_end}"
        return prefix


def detect_partner_period_from_text(value):
    text_value = safe_text(value)
    if not text_value:
        return None
    dotted_pattern = re.compile(r"(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})")
    dotted_match = dotted_pattern.search(text_value)
    if dotted_match:
        try:
            start_dt = datetime.strptime(dotted_match.group(1), "%d.%m.%Y")
            end_dt = datetime.strptime(dotted_match.group(2), "%d.%m.%Y")
            return {
                "date_start": start_dt.strftime("%Y-%m-%d"),
                "date_end": end_dt.strftime("%Y-%m-%d"),
                "period_label": f"{start_dt.strftime('%d')}-{end_dt.strftime('%d.%m.%Y')}",
            }
        except Exception:
            pass

    mdy_pattern = re.compile(
        r"(?<!\d)"
        r"(\d{1,2})-(\d{1,2})-(\d{2,4})"
        r"(?:\D+)"
        r"(\d{1,2})-(\d{1,2})-(\d{2,4})"
        r"(?!\d)"
    )
    mdy_match = mdy_pattern.search(text_value)
    if mdy_match:
        try:
            start_month, start_day, start_year, end_month, end_day, end_year = mdy_match.groups()
            start_year = int(start_year)
            end_year = int(end_year)
            if start_year < 100:
                start_year += 2000
            if end_year < 100:
                end_year += 2000
            start_dt = date(start_year, int(start_month), int(start_day))
            end_dt = date(end_year, int(end_month), int(end_day))
            return {
                "date_start": start_dt.strftime("%Y-%m-%d"),
                "date_end": end_dt.strftime("%Y-%m-%d"),
                "period_label": f"{start_dt.strftime('%d')}-{end_dt.strftime('%d.%m.%Y')}",
            }
        except Exception:
            pass
    return None


def detect_partner_period_from_dataframe(df):
    try:
        for row in df.itertuples(index=False):
            for value in row:
                period_data = detect_partner_period_from_text(value)
                if period_data:
                    return period_data
    except Exception:
        pass
    return None


def detect_partner_period_from_raw_dataframe(df, preview_limit=12):
    try:
        for idx in range(min(len(df.index), preview_limit)):
            row_values = [safe_text(value) for value in df.iloc[idx].tolist()]
            normalized_values = [normalize_dataframe_header(value) for value in row_values]
            for col_idx, label in enumerate(normalized_values):
                if label not in {"период", "period"}:
                    continue
                candidate_values = row_values[col_idx + 1 :]
                for candidate in candidate_values:
                    period_data = detect_partner_period_from_text(candidate)
                    if period_data:
                        return period_data
    except Exception:
        pass
    return detect_partner_period_from_dataframe(df)


def detect_period_from_dataframe_dates(df, column_name: str):
    if column_name not in list(df.columns):
        return None
    try:
        series = pd.to_datetime(df[column_name], errors="coerce", utc=True).dropna()
        if series.empty:
            return None
        start_dt = series.min().date()
        end_dt = series.max().date()
        return {
            "date_start": start_dt.strftime("%Y-%m-%d"),
            "date_end": end_dt.strftime("%Y-%m-%d"),
            "period_label": f"{start_dt.strftime('%d')}-{end_dt.strftime('%d.%m.%Y')}",
        }
    except Exception:
        return None


def detect_half_month_period_from_dataframe_dates(df, column_name: str):
    if column_name not in list(df.columns):
        return None
    try:
        series = pd.to_datetime(df[column_name], errors="coerce", utc=True).dropna()
        if series.empty:
            return None
        unique_periods = []
        seen_labels = set()
        for item in series:
            period_info = get_half_month_period_from_date(item.date().isoformat())
            label = safe_text(period_info.get("period_label"))
            if not label or label in seen_labels:
                continue
            seen_labels.add(label)
            unique_periods.append({
                "date_start": safe_text(period_info.get("period_start")),
                "date_end": safe_text(period_info.get("period_end")),
                "period_label": label,
            })
        if len(unique_periods) == 1:
            return unique_periods[0]
    except Exception:
        return None
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


def detect_partner_upload_period(df, partner_platform="1xbet", fallback_text=""):
    period_data = get_dataframe_detected_period(df) or detect_partner_period_from_text(fallback_text)
    if period_data:
        return period_data
    if normalize_partner_platform(partner_platform) == "cellxpert":
        return (
            detect_half_month_period_from_dataframe_dates(df, "Registration Date")
            or detect_period_from_dataframe_dates(df, "Registration Date")
            or detect_period_from_dataframe_dates(df, "Дата регистрации")
            or get_half_month_period()
        )
    return None


def get_partner_platform_options():
    rows = get_cabinet_rows()
    options = []
    seen = set()
    for row in rows:
        raw_platform = safe_text(getattr(row, "platform", "")).strip()
        if not raw_platform:
            continue
        key = raw_platform.lower()
        if key in seen:
            continue
        seen.add(key)
        options.append((raw_platform, raw_platform))
    if not options:
        return [("1xBet", "1xBet"), ("CellXpert", "CellXpert")]
    options.sort(key=lambda item: item[1].lower())
    return options


def get_partner_upload_cabinet_catalog():
    rows = get_cabinet_rows()
    catalog = []
    seen = set()
    for row in rows:
        cabinet_name = safe_text(getattr(row, "name", "")).strip()
        platform_name = safe_text(getattr(row, "platform", "")).strip()
        if not cabinet_name:
            continue
        key = (cabinet_name.lower(), platform_name.lower())
        if key in seen:
            continue
        seen.add(key)
        catalog.append({
            "cabinet_name": cabinet_name,
            "platform_name": platform_name,
        })
    catalog.sort(key=lambda item: (safe_text(item.get("platform_name")).lower(), safe_text(item.get("cabinet_name")).lower()))
    return catalog


def normalize_partner_platform(value: str = ""):
    text_value = safe_text(value).strip().lower()
    if "cell" in text_value:
        return "cellxpert"
    return "1xbet"


def get_cabinet_platform_map():
    rows = get_cabinet_rows()
    result = {}
    for row in rows:
        cabinet_name = safe_text(row.name)
        if cabinet_name:
            result[cabinet_name] = normalize_partner_platform(row.platform or "")
    return result


def partner_row_platform(row, cabinet_platform_map=None):
    cabinet_platform_map = cabinet_platform_map or {}
    cabinet_name = safe_text(getattr(row, "cabinet_name", ""))
    if cabinet_name and cabinet_name in cabinet_platform_map:
        return cabinet_platform_map[cabinet_name]
    source_name = safe_text(getattr(row, "source_name", ""))
    return normalize_partner_platform(source_name)


def normalize_dataframe_header(value):
    text_value = safe_text(value).strip().lower().replace("\n", " ").replace("\r", " ")
    text_value = re.sub(r"\s+", " ", text_value)
    return text_value


def build_dataframe_column_alias_map(df):
    return {
        normalize_dataframe_header(column): column
        for column in list(df.columns)
    }


def resolve_dataframe_column(alias_map, aliases):
    for alias in aliases:
        resolved = alias_map.get(normalize_dataframe_header(alias))
        if resolved is not None:
            return resolved
    return None


def get_dataframe_detected_period(df):
    attrs = getattr(df, "attrs", {}) or {}
    period_data = attrs.get("detected_period")
    if not isinstance(period_data, dict):
        return None
    if not safe_text(period_data.get("date_start")) or not safe_text(period_data.get("date_end")):
        return None
    return {
        "date_start": safe_text(period_data.get("date_start")),
        "date_end": safe_text(period_data.get("date_end")),
        "period_label": safe_text(period_data.get("period_label")),
    }


def build_partner_storage_period(period_data=None, fallback_value=""):
    if isinstance(period_data, dict):
        end_value = safe_text(period_data.get("date_end"))
        if end_value:
            return get_half_month_period_from_date(end_value)
    if fallback_value:
        return get_half_month_period_from_date(fallback_value)
    return {"report_date": "", "period_start": "", "period_end": "", "period_label": ""}


def read_csv_with_auto_separator(path: str, header="infer"):
    read_kwargs = {
        "sep": None,
        "engine": "python",
        "encoding": "utf-8-sig",
    }
    if header != "infer":
        read_kwargs["header"] = header
    try:
        return pd.read_csv(path, **read_kwargs)
    except Exception as first_exc:
        last_exc = first_exc
    for separator in (",", ";", "\t"):
        try:
            fallback_kwargs = {"sep": separator, "encoding": "utf-8-sig"}
            if header != "infer":
                fallback_kwargs["header"] = header
            return pd.read_csv(path, **fallback_kwargs)
        except Exception as fallback_exc:
            last_exc = fallback_exc
    raise last_exc


def parse_1xbet_partner_dataframe(df, source_name="", cabinet_name="", upload_period_data=None):
    alias_map = build_dataframe_column_alias_map(df)
    row_number_col = resolve_dataframe_column(alias_map, ["№", "#", "No", "Row", "Row Number"])
    sub_id_col = resolve_dataframe_column(alias_map, ["SubId", "SubID", "Sub Id", "Sub ID"])
    player_id_col = resolve_dataframe_column(alias_map, ["ID игрока", "Player ID", "ID Player", "PlayerId"])
    country_col = resolve_dataframe_column(alias_map, ["Страна", "Country", "Geo"])
    deposit_col = resolve_dataframe_column(alias_map, ["Сумма депозитов", "Deposit Sum", "Deposits", "First Time Deposit Amount"])
    bet_col = resolve_dataframe_column(alias_map, ["Сумма ставок", "Bet Sum", "Betting Sum", "Bets"])
    income_col = resolve_dataframe_column(alias_map, ["Доход компании (общий)", "NGR", "Income", "Commissions"])
    cpa_col = resolve_dataframe_column(alias_map, ["CPA"])
    registration_col = resolve_dataframe_column(alias_map, ["Дата регистрации", "Registration Date", "Reg Date"])
    hold_col = resolve_dataframe_column(alias_map, ["Hold time", "Hold Time", "Activity Count"])
    blocked_col = resolve_dataframe_column(alias_map, ["Заблокирован", "Blocked", "Status"])
    upload_period = build_partner_storage_period(upload_period_data or get_dataframe_detected_period(df))

    records = []
    for _, row in df.iterrows():
        row_number = safe_text(row.get(row_number_col)) if row_number_col else ""
        sub_id = safe_text(row.get(sub_id_col)) if sub_id_col else ""
        player_id = safe_text(row.get(player_id_col)) if player_id_col else ""
        country = normalize_geo_value(row.get(country_col)) if country_col else ""
        if sub_id in {"SUBID", "ID ПАРТНЕРА", "ПЕРИОД", "ВАЛЮТА", "КАМПАНИЯ", "ТОЛЬКО НОВЫЕ ИГРОКИ", "ТОЛЬКО ИГРОКИ БЕЗ ДЕПОЗИТОВ"}:
            continue
        if row_number in {"№", "#", "NO", "ROW", "ROW NUMBER"}:
            continue
        if not player_id or player_id == "ID игрока":
            continue
        if not country or country in {"СТРАНА"}:
            continue
        deposit_amount = safe_number(row.get(deposit_col)) if deposit_col else 0.0
        company_income = safe_number(row.get(income_col)) if income_col else 0.0
        cpa_amount = safe_number(row.get(cpa_col)) if cpa_col else 0.0
        registration_value = row.get(registration_col) if registration_col else ""
        period_info = upload_period if safe_text(upload_period.get("period_label")) else {"report_date": "", "period_start": "", "period_end": "", "period_label": ""}
        records.append(PartnerRow(
            source_name=source_name,
            cabinet_name=safe_text(cabinet_name),
            sub_id=sub_id,
            player_id=player_id,
            report_date=period_info["report_date"],
            period_start=period_info["period_start"],
            period_end=period_info["period_end"],
            period_label=period_info["period_label"],
            registration_date=safe_text(registration_value),
            country=country,
            deposit_amount=deposit_amount,
            bet_amount=safe_number(row.get(bet_col)) if bet_col else 0.0,
            company_income=company_income,
            cpa_amount=cpa_amount,
            hold_time=safe_text(row.get(hold_col)) if hold_col else "",
            blocked=safe_text(row.get(blocked_col)) if blocked_col else "",
        ))
    return records


def parse_cellxpert_partner_dataframe(df, source_name="", cabinet_name="", upload_period_data=None):
    alias_map = build_dataframe_column_alias_map(df)
    player_id_col = resolve_dataframe_column(alias_map, ["User ID", "Player ID", "UserID"])
    country_col = resolve_dataframe_column(alias_map, ["Country", "Geo"])
    registration_col = resolve_dataframe_column(alias_map, ["Registration Date", "Reg Date"])
    ftd_amount_col = resolve_dataframe_column(alias_map, ["First Time Deposit Amount", "FTD Amount", "Deposit Sum"])
    deposits_col = resolve_dataframe_column(alias_map, ["Deposits", "Net Deposits", "Deposit Amount"])
    commissions_col = resolve_dataframe_column(alias_map, ["Commissions", "NGR", "Income"])
    activity_count_col = resolve_dataframe_column(alias_map, ["Activity Count", "Hold Time", "Hold time"])
    status_col = resolve_dataframe_column(alias_map, ["Status", "Blocked"])
    upload_period = build_partner_storage_period(upload_period_data)

    records = []
    for _, row in df.iterrows():
        player_id = safe_text(row.get(player_id_col)) if player_id_col else ""
        country = normalize_geo_value(row.get(country_col)) if country_col else ""
        if not player_id or not country:
            continue
        ftd_amount = safe_number(row.get(ftd_amount_col)) if ftd_amount_col else 0.0
        total_deposits = safe_number(row.get(deposits_col)) if deposits_col else 0.0
        deposit_amount = ftd_amount if ftd_amount > 0 else total_deposits
        registration_value = row.get(registration_col) if registration_col else ""
        period_info = upload_period if safe_text(upload_period.get("period_label")) else {"report_date": "", "period_start": "", "period_end": "", "period_label": ""}
        records.append(PartnerRow(
            source_name=source_name,
            cabinet_name=safe_text(cabinet_name),
            sub_id=player_id,
            player_id=player_id,
            report_date=period_info["report_date"],
            period_start=period_info["period_start"],
            period_end=period_info["period_end"],
            period_label=period_info["period_label"],
            registration_date=safe_text(registration_value),
            country=country,
            deposit_amount=deposit_amount,
            bet_amount=0.0,
            company_income=safe_number(row.get(commissions_col)) if commissions_col else 0.0,
            cpa_amount=0.0,
            hold_time=safe_text(row.get(activity_count_col)) if activity_count_col else "",
            blocked=safe_text(row.get(status_col)) if status_col else "",
        ))
    return records


def parse_partner_dataframe(df, source_name="", cabinet_name="", partner_platform="1xbet", upload_period_data=None):
    platform_key = normalize_partner_platform(partner_platform)
    if platform_key == "cellxpert":
        return parse_cellxpert_partner_dataframe(df, source_name=source_name, cabinet_name=cabinet_name, upload_period_data=upload_period_data)
    return parse_1xbet_partner_dataframe(df, source_name=source_name, cabinet_name=cabinet_name, upload_period_data=upload_period_data)


def build_partner_row_identity(row):
    player_key = normalize_id_value(getattr(row, "player_id", "")) or safe_text(getattr(row, "player_id", "")).strip().upper()
    return (
        safe_text(getattr(row, "cabinet_name", "")),
        player_key,
        safe_text(getattr(row, "registration_date", "")),
    )


def build_partner_row_merge_identity(row):
    cabinet_key = safe_text(getattr(row, "cabinet_name", "")).strip().lower()
    player_key = normalize_id_value(getattr(row, "player_id", "")) or safe_text(getattr(row, "player_id", "")).strip().upper()
    registration_key = safe_text(getattr(row, "registration_date", "")).strip()
    return (cabinet_key, player_key, registration_key)


def cleanup_partner_duplicates(period_label="", preferred_source_name="", preferred_cabinet=""):
    ensure_partner_table()
    clean_period = safe_text(period_label)
    clean_cabinet = safe_text(preferred_cabinet)
    if not clean_period:
        return
    db = SessionLocal()
    try:
        query = db.query(PartnerRow)
        if clean_cabinet and clean_period:
            query = query.filter(
                PartnerRow.cabinet_name == clean_cabinet,
                PartnerRow.period_label == clean_period,
            )
        elif clean_cabinet:
            query = query.filter(PartnerRow.cabinet_name == clean_cabinet)
        else:
            query = query.filter(PartnerRow.period_label == clean_period)
        rows = query.order_by(PartnerRow.id.desc()).all()
        buckets = {}
        for row in rows:
            merge_key = build_partner_row_merge_identity(row)
            if not any(merge_key[1:]):
                continue
            buckets.setdefault(merge_key, []).append(row)

        for duplicate_rows in buckets.values():
            if len(duplicate_rows) <= 1:
                continue

            keeper = None
            for row in duplicate_rows:
                if preferred_source_name and safe_text(row.source_name) == safe_text(preferred_source_name):
                    keeper = row
                    break
            if keeper is None:
                for row in duplicate_rows:
                    if preferred_cabinet and safe_text(row.cabinet_name) == safe_text(preferred_cabinet):
                        keeper = row
                        break
            if keeper is None:
                keeper = duplicate_rows[0]

            keeper.manual_hold = 1 if any(safe_number(getattr(row, "manual_hold", 0)) > 0 for row in duplicate_rows) else 0
            keeper.manual_blocked = 1 if any(safe_number(getattr(row, "manual_blocked", 0)) > 0 for row in duplicate_rows) else 0
            db.add(keeper)

            for row in duplicate_rows:
                if row.id != keeper.id:
                    db.delete(row)
        db.commit()
    finally:
        db.close()


def detect_partner_header_index(df) -> int:
    preview_limit = min(len(df.index), 15)
    for idx in range(preview_limit):
        row_values = [normalize_dataframe_header(value) for value in df.iloc[idx].tolist()]
        if "subid" in row_values and any(("игрок" in value) or ("player" in value) for value in row_values):
            return idx
    return 0


def read_partner_uploaded_dataframe(path: str, ext: str):
    if ext in [".xlsx", ".xls"]:
        raw_df = pd.read_excel(path, header=None)
    else:
        raw_df = read_csv_with_auto_separator(path, header=None)
    detected_period = detect_partner_period_from_raw_dataframe(raw_df)

    header_index = detect_partner_header_index(raw_df)
    headers = [safe_text(value) for value in raw_df.iloc[header_index].tolist()]
    data_df = raw_df.iloc[header_index + 1 :].copy()
    data_df.columns = headers
    data_df = data_df.reset_index(drop=True)
    data_df.attrs["detected_period"] = detected_period or {}
    return data_df


def read_cellxpert_uploaded_dataframe(path: str, ext: str):
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    return read_csv_with_auto_separator(path)


def get_cabinet_rows(search="", status=""):
    ensure_cabinet_table()
    db = SessionLocal()
    try:
        query = db.query(CabinetRow)
        if status:
            query = query.filter(CabinetRow.status == status)
        if search:
            search_pattern = f"%{safe_text(search)}%"
            query = query.filter(or_(
                CabinetRow.advertiser.ilike(search_pattern),
                CabinetRow.platform.ilike(search_pattern),
                CabinetRow.name.ilike(search_pattern),
                CabinetRow.geo_list.ilike(search_pattern),
                CabinetRow.brands.ilike(search_pattern),
                CabinetRow.team_name.ilike(search_pattern),
                CabinetRow.manager_name.ilike(search_pattern),
                CabinetRow.manager_contact.ilike(search_pattern),
                CabinetRow.wallet.ilike(search_pattern),
                CabinetRow.comments.ilike(search_pattern),
                CabinetRow.status.ilike(search_pattern),
            ))
        return query.order_by(CabinetRow.name.asc(), CabinetRow.id.asc()).all()
    finally:
        db.close()


def get_cabinet_names(active_only=False):
    rows = get_cabinet_rows()
    names = []
    for row in rows:
        if active_only and safe_text(row.status).lower() == "archived":
            continue
        name = safe_text(row.name)
        if name:
            names.append(name)
    return names


def get_active_cabinet_name_set():
    return set(get_cabinet_names(active_only=True))


def get_partner_cabinet_options(period_label=""):
    ensure_partner_table()
    db = SessionLocal()
    try:
        query = db.query(PartnerRow.cabinet_name)
        if period_label:
            query = query.filter(PartnerRow.period_label == period_label)
        values = query.distinct().all()
    finally:
        db.close()
    result = []
    for item in values:
        value = safe_text(item[0])
        if value:
            result.append(value)
    return sorted(set(result))


def get_partner_country_options():
    ensure_partner_table()
    db = SessionLocal()
    try:
        values = db.query(PartnerRow.country).distinct().all()
    finally:
        db.close()
    result = []
    for item in values:
        value = safe_text(item[0])
        if value:
            result.append(value)
    return sorted(set(result))


def get_chatterfy_linkage_maps(period_label=""):
    ensure_chatterfy_table()
    ensure_chatterfy_id_table()
    db = SessionLocal()
    try:
        id_rows = db.query(ChatterfyIdRow).all()
        chatter_rows = db.query(ChatterfyRow).order_by(ChatterfyRow.id.desc()).all()
    finally:
        db.close()

    id_by_player = {}
    for item in id_rows:
        player_key = normalize_id_value(item.pp_player_id)
        if not player_key:
            continue
        current = id_by_player.get(player_key, {})
        telegram_id = safe_text(item.telegram_id)
        chat_link = safe_text(item.chat_link)
        candidate_score = (
            1 if chat_link else 0,
            1 if telegram_id else 0,
            safe_text(item.source_date),
            safe_number(getattr(item, "id", 0)),
        )
        current_score = (
            1 if safe_text(current.get("chat_link")) else 0,
            1 if safe_text(current.get("telegram_id")) else 0,
            safe_text(current.get("source_date")) if current else "",
            safe_number(current.get("row_id", 0)) if current else 0,
        )
        if candidate_score >= current_score:
            id_by_player[player_key] = {
                "row_id": safe_number(getattr(item, "id", 0)),
                "telegram_id": telegram_id,
                "pp_player_id": safe_text(item.pp_player_id),
                "chat_link": chat_link,
                "source_date": safe_text(item.source_date),
            }

    chatter_by_telegram = {}
    chatter_by_telegram_period = {}
    for item in chatter_rows:
        telegram_key = safe_text(item.telegram_id)
        telegram_digits = normalize_id_value(item.telegram_id)
        payload = {
            "status": safe_text(item.status),
            "step": safe_text(item.step),
            "started": safe_text(item.started),
            "period_label": safe_text(item.period_label),
            "row_id": safe_number(getattr(item, "id", 0)),
        }
        for key in [telegram_key, telegram_digits]:
            if not key:
                continue
            chatter_by_telegram.setdefault(key, payload)
            if period_label and safe_text(item.period_label) == safe_text(period_label):
                chatter_by_telegram_period.setdefault(key, payload)
    return id_by_player, chatter_by_telegram_period, chatter_by_telegram


def build_cap_scope_key(cabinet_name="", geo_value=""):
    return (
        safe_text(cabinet_name).strip().upper(),
        normalize_geo_value(geo_value),
    )


def build_cap_match_maps(caps):
    caps_by_promo = {}
    caps_by_scope = {}
    for cap in caps:
        promo_key = safe_text(cap.promo_code).upper()
        if promo_key:
            caps_by_promo.setdefault(promo_key, []).append(cap)
        scope_key = build_cap_scope_key(getattr(cap, "cabinet_name", "") or getattr(cap, "buyer", ""), getattr(cap, "geo", "") or getattr(cap, "code", ""))
        if all(scope_key):
            caps_by_scope.setdefault(scope_key, []).append(cap)
    return caps_by_promo, caps_by_scope


def get_caps_for_partner_row(row, caps_by_promo, caps_by_scope, cabinet_platform_map=None):
    platform_key = partner_row_platform(row, cabinet_platform_map)
    if platform_key == "cellxpert":
        return caps_by_scope.get(build_cap_scope_key(getattr(row, "cabinet_name", ""), getattr(row, "country", "")), [])
    promo_key = safe_text(getattr(row, "sub_id", "")).upper()
    matched = caps_by_promo.get(promo_key, [])
    if matched:
        return matched
    return caps_by_scope.get(build_cap_scope_key(getattr(row, "cabinet_name", ""), getattr(row, "country", "")), [])


def build_chatterfy_player_context(player_id="", period_label=""):
    id_by_player, chatter_by_telegram_period, chatter_by_telegram = get_chatterfy_linkage_maps(period_label=period_label)
    link = id_by_player.get(normalize_id_value(player_id), {})
    telegram_id = safe_text(link.get("telegram_id"))
    chatter_info = (
        chatter_by_telegram_period.get(telegram_id)
        or chatter_by_telegram_period.get(normalize_id_value(telegram_id))
        or chatter_by_telegram.get(telegram_id, {})
        or chatter_by_telegram.get(normalize_id_value(telegram_id), {})
    )
    return {
        "telegram_id": telegram_id,
        "pp_player_id": safe_text(link.get("pp_player_id")) or safe_text(player_id),
        "chat_link": safe_text(link.get("chat_link")),
        "chatter_status": safe_text(chatter_info.get("status")),
        "chatter_step": safe_text(chatter_info.get("step")),
    }


def get_hold_wager_rows(period_label="", cabinet_name="", search=""):
    ensure_partner_table()
    ensure_caps_table()
    ensure_chatterfy_id_table()
    active_cabinets = get_active_cabinet_name_set()
    db = SessionLocal()
    try:
        partner_query = db.query(PartnerRow)
        if period_label:
            partner_query = partner_query.filter(PartnerRow.period_label == period_label)
        if cabinet_name:
            partner_query = partner_query.filter(PartnerRow.cabinet_name == cabinet_name)
        partner_rows = partner_query.order_by(PartnerRow.registration_date.desc(), PartnerRow.id.desc()).all()
        caps = db.query(CapRow).all()
    finally:
        db.close()

    caps_by_promo = {}
    cabinet_platform_map = get_cabinet_platform_map()
    caps_by_promo, caps_by_scope = build_cap_match_maps(caps)

    id_by_player, chatter_by_telegram_period, chatter_by_telegram = get_chatterfy_linkage_maps(period_label=period_label)

    search_lower = safe_text(search).lower()
    result = []
    for row in partner_rows:
        if active_cabinets and safe_text(row.cabinet_name) not in active_cabinets:
            continue
        if safe_number(row.deposit_amount) <= 0:
            continue
        matched_caps = get_caps_for_partner_row(row, caps_by_promo, caps_by_scope, cabinet_platform_map)
        if not matched_caps:
            continue
        cap = matched_caps[0]
        deposit_amount = safe_number(row.deposit_amount)
        bet_amount = safe_number(row.bet_amount)
        baseline = safe_cap_number(cap.baseline)
        fail_baseline = baseline > 0 and deposit_amount < baseline
        fail_wager = baseline > 0 and bet_amount < baseline
        if not fail_baseline and not fail_wager:
            continue

        reason_parts = []
        if fail_baseline:
            reason_parts.append("Baseline")
        if fail_wager:
            reason_parts.append("Wager")
        player_key = normalize_id_value(row.player_id)
        linked = id_by_player.get(player_key, {})
        telegram_id = safe_text(linked.get("telegram_id"))
        chat_link = safe_text(linked.get("chat_link"))
        pp_player_id = safe_text(linked.get("pp_player_id")) or safe_text(row.player_id)
        chatter_info = (
            chatter_by_telegram_period.get(telegram_id)
            or chatter_by_telegram_period.get(normalize_id_value(telegram_id))
            or chatter_by_telegram.get(telegram_id, {})
            or chatter_by_telegram.get(normalize_id_value(telegram_id), {})
        )
        item = {
            "row_id": row.id,
            "report_date": safe_text(getattr(row, "report_date", "")) or safe_text(getattr(row, "period_end", "")),
            "period_label": partner_row_period_label(row),
            "registration_date": safe_text(row.registration_date),
            "cabinet_name": safe_text(row.cabinet_name),
            "sub_id": safe_text(row.sub_id),
            "player_id": safe_text(row.player_id),
            "country": safe_text(row.country),
            "deposit_amount": deposit_amount,
            "bet_amount": bet_amount,
            "baseline": baseline,
            "rate": safe_text(cap.rate),
            "flow": safe_text(cap.flow),
            "promo_code": safe_text(cap.promo_code),
            "telegram_id": telegram_id,
            "pp_player_id": pp_player_id,
            "chat_link": chat_link,
            "chatter_status": safe_text(chatter_info.get("status")),
            "chatter_step": safe_text(chatter_info.get("step")),
            "reason": " + ".join(reason_parts),
            "missing_baseline": max(0.0, baseline - deposit_amount) if fail_baseline else 0.0,
            "missing_wager": max(0.0, baseline - bet_amount) if fail_wager else 0.0,
        }
        if search_lower:
            haystack = " | ".join([
                item["cabinet_name"],
                item["sub_id"],
                item["player_id"],
                item["telegram_id"],
                item["pp_player_id"],
                item["country"],
                item["reason"],
                item["flow"],
                item["promo_code"],
                item["chatter_status"],
            ]).lower()
            if search_lower not in haystack:
                continue
        result.append(item)
    return result


def replace_partner_rows(source_name, rows_to_insert):
    ensure_partner_table()
    target_cabinet = ""
    target_period_label = ""
    if rows_to_insert:
        target_cabinet = safe_text(getattr(rows_to_insert[0], "cabinet_name", ""))
        target_period_label = safe_text(getattr(rows_to_insert[0], "period_label", ""))
    db = SessionLocal()
    try:
        scope_query = db.query(PartnerRow)
        if source_name:
            scope_query = scope_query.filter(PartnerRow.source_name == source_name)
        elif target_cabinet and target_period_label:
            scope_query = scope_query.filter(
                PartnerRow.cabinet_name == target_cabinet,
                PartnerRow.period_label == target_period_label,
            )
        existing_rows = scope_query.all()
        manual_flags = {
            build_partner_row_identity(item): {
                "manual_hold": 1 if safe_number(getattr(item, "manual_hold", 0)) > 0 else 0,
                "manual_blocked": 1 if safe_number(getattr(item, "manual_blocked", 0)) > 0 else 0,
            }
            for item in existing_rows
        }
        for item in rows_to_insert:
            flags = manual_flags.get(build_partner_row_identity(item), {})
            item.manual_hold = int(flags.get("manual_hold", 0))
            item.manual_blocked = int(flags.get("manual_blocked", 0))
        if source_name:
            db.query(PartnerRow).filter(PartnerRow.source_name == source_name).delete()
        elif target_cabinet and target_period_label:
            db.query(PartnerRow).filter(
                PartnerRow.cabinet_name == target_cabinet,
                PartnerRow.period_label == target_period_label,
            ).delete()
        else:
            db.query(PartnerRow).delete()
        db.commit()
        sync_postgres_sequence("partner_rows")
        for item in rows_to_insert:
            db.add(item)
        db.commit()
    finally:
        db.close()
    cleanup_partner_duplicates(
        period_label=target_period_label,
        preferred_source_name=source_name,
        preferred_cabinet=target_cabinet,
    )
    clear_runtime_cache("stat_support::")
    refresh_cap_current_ftd_from_partner()


def import_chatterfy_dataframe(df, source_name=""):
    ensure_chatterfy_table()
    records = []
    for _, row in df.iterrows():
        tags = safe_text(row.get("Tags"))
        parsed = parse_chatterfy_tags(tags)
        period_info = get_half_month_period_from_date(row.get("Started"))
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
            report_date=period_info["report_date"],
            period_start=period_info["period_start"],
            period_end=period_info["period_end"],
            period_label=period_info["period_label"],
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
    clear_runtime_cache("stat_support::")
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


def get_chatterfy_rows(status="", search="", date_filter="", time_filter="", telegram_id="", pp_player_id="", period_label=""):
    ensure_chatterfy_table()
    ensure_chatterfy_id_table()
    telegram_digits = normalize_id_value(telegram_id)
    pp_digits = normalize_id_value(pp_player_id)
    db = SessionLocal()
    try:
        query = db.query(ChatterfyRow)
        if status:
            query = query.filter(ChatterfyRow.status == status)
        if period_label:
            query = query.filter(ChatterfyRow.period_label == period_label)
        if search:
            search_pattern = f"%{safe_text(search)}%"
            query = query.filter(or_(
                ChatterfyRow.name.ilike(search_pattern),
                ChatterfyRow.username.ilike(search_pattern),
                ChatterfyRow.telegram_id.ilike(search_pattern),
                ChatterfyRow.tags.ilike(search_pattern),
                ChatterfyRow.status.ilike(search_pattern),
                ChatterfyRow.offer.ilike(search_pattern),
                ChatterfyRow.manager.ilike(search_pattern),
                ChatterfyRow.geo.ilike(search_pattern),
            ))
        rows = query.order_by(ChatterfyRow.id.desc()).all()
        telegram_ids = sorted({safe_text(row.telegram_id) for row in rows if safe_text(row.telegram_id)})
        id_rows = db.query(ChatterfyIdRow).filter(ChatterfyIdRow.telegram_id.in_(telegram_ids)).all() if telegram_ids else []
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
        row_period_label = chatterfy_row_period_label(row)
        row_report_date = safe_text(getattr(row, "report_date", "")) or (started_dt.strftime("%Y-%m-%d") if started_dt else "")
        if date_filter and date_filter != started_date:
            continue
        if time_filter and not started_time.startswith(time_filter):
            continue
        if telegram_digits and normalize_id_value(row.telegram_id) != telegram_digits:
            continue
        if pp_digits and normalize_id_value(linked_pp) != pp_digits:
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
            "report_date": row_report_date,
            "period_label": row_period_label,
        })
    return filtered


def import_chatterfy_from_csv_if_needed():
    if "chatterfy_rows" in AUTO_IMPORT_CHECKS:
        return
    ensure_chatterfy_table()
    db = SessionLocal()
    try:
        if db.query(ChatterfyRow).count() > 0:
            AUTO_IMPORT_CHECKS.add("chatterfy_rows")
            return
    finally:
        db.close()
    source_path = "/Users/ivansviderko/Downloads/Выгрузка (16.03-31.03.26) - Chatterfy.csv"
    if not os.path.exists(source_path):
        AUTO_IMPORT_CHECKS.add("chatterfy_rows")
        return
    df = pd.read_csv(source_path)
    import_chatterfy_dataframe(df, os.path.basename(source_path))
    AUTO_IMPORT_CHECKS.add("chatterfy_rows")


def import_chatterfy_ids_from_csv_if_needed():
    if "chatterfy_id_rows" in AUTO_IMPORT_CHECKS:
        return
    ensure_chatterfy_id_table()
    db = SessionLocal()
    try:
        if db.query(ChatterfyIdRow).count() > 0:
            AUTO_IMPORT_CHECKS.add("chatterfy_id_rows")
            return
    finally:
        db.close()
    source_path = "/Users/ivansviderko/Downloads/ID_Chatterfy.csv"
    if not os.path.exists(source_path):
        AUTO_IMPORT_CHECKS.add("chatterfy_id_rows")
        return
    df = pd.read_csv(source_path)
    import_chatterfy_ids_dataframe(df)
    AUTO_IMPORT_CHECKS.add("chatterfy_id_rows")

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


def get_partner_upload_summaries():
    ensure_partner_table()
    db = SessionLocal()
    try:
        rows = db.query(PartnerRow).order_by(PartnerRow.id.desc()).all()
    finally:
        db.close()

    grouped = {}
    cabinet_platform_map = get_cabinet_platform_map()
    for row in rows:
        source_key = safe_text(getattr(row, "source_name", ""))
        if not source_key:
            continue
        item = grouped.setdefault(source_key, {
            "source_name": source_key,
            "cabinet_name": safe_text(getattr(row, "cabinet_name", "")),
            "period_label": partner_row_period_label(row),
            "platform_label": "CellXpert" if partner_row_platform(row, cabinet_platform_map) == "cellxpert" else "1xBet",
            "rows_count": 0,
            "players_count": 0,
            "ftd_count": 0,
            "date_start": safe_text((detect_partner_period_from_text(source_key) or {}).get("date_start")) or safe_text(getattr(row, "period_start", "")),
            "date_end": safe_text((detect_partner_period_from_text(source_key) or {}).get("date_end")) or safe_text(getattr(row, "period_end", "")),
        })
        item["rows_count"] += 1
        item["players_count"] += 1
        if safe_number(getattr(row, "deposit_amount", 0)) > 0:
            item["ftd_count"] += 1

    result = list(grouped.values())
    result.sort(key=lambda item: (safe_text(item.get("period_label")), safe_text(item.get("cabinet_name")), safe_text(item.get("source_name"))), reverse=True)
    return result


def get_partner_rows_by_period(period_value="", period_label="", cabinet_name="", country="", search=""):
    ensure_partner_table()
    ensure_caps_table()
    active_cabinets = get_active_cabinet_name_set()
    cabinet_platform_map = get_cabinet_platform_map()
    cabinet_rows = get_cabinet_rows()
    cabinet_meta_map = {
        safe_text(getattr(item, "name", "")): item
        for item in cabinet_rows
        if safe_text(getattr(item, "name", ""))
    }
    db = SessionLocal()
    try:
        query = db.query(PartnerRow)
        caps_query = db.query(CapRow)
        if period_value:
            query = query.filter(PartnerRow.source_name == period_value)
        if period_label:
            query = query.filter(PartnerRow.period_label == period_label)
            caps_query = caps_query.filter(CapRow.period_label == period_label)
        if cabinet_name:
            query = query.filter(PartnerRow.cabinet_name == cabinet_name)
        if country:
            query = query.filter(PartnerRow.country == country)
        if search:
            search_pattern = f"%{safe_text(search)}%"
            query = query.filter(or_(
                PartnerRow.sub_id.ilike(search_pattern),
                PartnerRow.player_id.ilike(search_pattern),
                PartnerRow.country.ilike(search_pattern),
                PartnerRow.source_name.ilike(search_pattern),
                PartnerRow.cabinet_name.ilike(search_pattern),
                PartnerRow.registration_date.ilike(search_pattern),
            ))
        rows = query.order_by(PartnerRow.id.desc()).all()
        caps = caps_query.order_by(CapRow.id.desc()).all()
    finally:
        db.close()

    if active_cabinets:
        rows = [row for row in rows if safe_text(getattr(row, "cabinet_name", "")) in active_cabinets]

    caps_by_scope_period = {}
    for cap in caps:
        period_key = safe_text(getattr(cap, "period_label", ""))
        scope_key = build_cap_scope_key(getattr(cap, "cabinet_name", "") or getattr(cap, "buyer", ""), getattr(cap, "geo", "") or getattr(cap, "code", ""))
        if not period_key or not all(scope_key):
            continue
        caps_by_scope_period.setdefault((period_key, scope_key[0], scope_key[1]), []).append(cap)

    id_by_player, chatter_by_telegram_period, chatter_by_telegram = get_chatterfy_linkage_maps(period_label=period_label)
    for row in rows:
        linked = id_by_player.get(normalize_id_value(getattr(row, "player_id", "")), {})
        telegram_id = safe_text(linked.get("telegram_id"))
        chatter_info = (
            chatter_by_telegram_period.get(telegram_id)
            or chatter_by_telegram_period.get(normalize_id_value(telegram_id))
            or chatter_by_telegram.get(telegram_id, {})
            or chatter_by_telegram.get(normalize_id_value(telegram_id), {})
        )
        row.partner_platform = partner_row_platform(row, cabinet_platform_map)
        row.telegram_id = telegram_id
        row.pp_player_id = safe_text(linked.get("pp_player_id")) or safe_text(getattr(row, "player_id", ""))
        row.chat_link = safe_text(linked.get("chat_link"))
        row.chatter_status = safe_text(chatter_info.get("status"))
        row.chatter_step = safe_text(chatter_info.get("step"))
        cabinet_item = cabinet_meta_map.get(safe_text(getattr(row, "cabinet_name", "")))
        row.brand_name = safe_text(getattr(cabinet_item, "brands", "")) if cabinet_item else ""
        row.geo_name = geo_display_name(getattr(row, "country", "") or "")
        row_period_label = partner_row_period_label(row)
        scope_key = build_cap_scope_key(getattr(row, "cabinet_name", ""), getattr(row, "country", ""))
        matched_caps = caps_by_scope_period.get((row_period_label, scope_key[0], scope_key[1]), [])
        matched_cap = matched_caps[0] if matched_caps else None
        row.is_qualified_ftd = bool(matched_cap and is_partner_row_qualified_for_cap(row, matched_cap, cabinet_platform_map))
        row.cpa_amount = safe_cap_number(getattr(matched_cap, "rate", 0)) if row.is_qualified_ftd else 0.0
    return rows


def aggregate_partner_totals(rows):
    return {
        "players": len(rows),
        "deposits": sum(safe_number(r.deposit_amount) for r in rows),
        "bets": sum(safe_number(r.bet_amount) for r in rows),
        "income": sum(safe_number(r.company_income) for r in rows),
        "cpa": sum(safe_number(r.cpa_amount) for r in rows),
        "ftd_count": sum(1 for r in rows if safe_number(r.deposit_amount) > 0),
        "qualified_ftd_count": sum(1 for r in rows if bool(getattr(r, "is_qualified_ftd", False))),
    }
def refresh_cap_current_ftd_from_partner():
    ensure_partner_table()
    ensure_caps_table()
    db = SessionLocal()
    try:
        current_period_label = get_current_period_label()
        current_caps_count = db.query(CapRow).filter(CapRow.period_label == current_period_label).count()
        if current_caps_count == 0:
            previous_period_label = get_previous_period_label(current_period_label)
            if previous_period_label:
                previous_caps = db.query(CapRow).filter(CapRow.period_label == previous_period_label).all()
                for source_cap in previous_caps:
                    db.add(CapRow(
                        advertiser=source_cap.advertiser,
                        owner_name=source_cap.owner_name,
                        buyer=source_cap.cabinet_name or source_cap.buyer,
                        cabinet_name=source_cap.cabinet_name,
                        flow=source_cap.flow,
                        code=source_cap.code,
                        geo=source_cap.geo,
                        rate=source_cap.rate,
                        baseline=source_cap.baseline,
                        cap_value=source_cap.cap_value,
                        promo_code=source_cap.promo_code,
                        kpi=source_cap.kpi,
                        link=source_cap.link,
                        comments=source_cap.comments,
                        agent=source_cap.agent,
                        chat_title=source_cap.chat_title,
                        period_label=current_period_label,
                        current_ftd=0,
                    ))
                if previous_caps:
                    db.commit()
        caps = db.query(CapRow).all()
        partner_rows = db.query(PartnerRow).all()
        cabinet_platform_map = get_cabinet_platform_map()
        caps_by_promo, caps_by_scope = build_cap_match_maps(caps)
        for cap in caps:
            matched = [
                row for row in partner_rows
                if safe_text(getattr(row, "period_label", "")) == safe_text(getattr(cap, "period_label", ""))
                if cap in get_caps_for_partner_row(row, caps_by_promo, caps_by_scope, cabinet_platform_map)
            ]
            cap.current_ftd = float(sum(1 for item in matched if safe_number(item.deposit_amount) > 0))
            db.add(cap)
        db.commit()
    finally:
        db.close()


def is_partner_row_qualified_for_cap(row, cap, cabinet_platform_map=None):
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


def get_statistic_support_maps(period_label=""):
    cache_key = f"stat_support::{period_label or 'all'}"
    cached = RUNTIME_CACHE.get(cache_key)
    if cached is not None:
        return cached
    import_chatterfy_from_csv_if_needed()
    ensure_partner_table()
    ensure_chatterfy_table()
    ensure_caps_table()
    db = SessionLocal()
    try:
        caps_query = db.query(CapRow)
        partner_query = db.query(PartnerRow)
        chatterfy_query = db.query(ChatterfyRow)
        if period_label:
            caps_query = caps_query.filter(CapRow.period_label == period_label)
            partner_query = partner_query.filter(PartnerRow.period_label == period_label)
            chatterfy_query = chatterfy_query.filter(ChatterfyRow.period_label == period_label)
        caps = caps_query.all()
        partner_rows = partner_query.all()
        chatterfy_rows = chatterfy_query.all()
    finally:
        db.close()

    cabinet_platform_map = get_cabinet_platform_map()
    caps_by_promo, caps_by_scope = build_cap_match_maps(caps)

    partner_by_flow = {}
    for row in partner_rows:
        matched_caps = get_caps_for_partner_row(row, caps_by_promo, caps_by_scope, cabinet_platform_map)
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
            if is_partner_row_qualified_for_cap(row, cap, cabinet_platform_map):
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
    result = (partner_by_flow, chatterfy_by_ad, chatterfy_by_flow)
    RUNTIME_CACHE[cache_key] = result
    return result


def enrich_statistic_rows(rows, period_label=""):
    partner_by_flow, chatterfy_by_ad, chatterfy_by_flow = get_statistic_support_maps(period_label=period_label)
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
    def sqlite_migration():
        with engine.begin() as conn:
            expense_columns = [row[1] for row in conn.execute(text("PRAGMA table_info(finance_expense_rows)")).fetchall()]
            if "wallet_name" not in expense_columns:
                conn.execute(text("ALTER TABLE finance_expense_rows ADD COLUMN wallet_name VARCHAR DEFAULT ''"))
            if "from_wallet" not in expense_columns:
                conn.execute(text("ALTER TABLE finance_expense_rows ADD COLUMN from_wallet VARCHAR DEFAULT ''"))
                if "paid_by" in expense_columns:
                    conn.execute(text("UPDATE finance_expense_rows SET from_wallet = paid_by WHERE COALESCE(from_wallet, '') = ''"))

            income_columns = [row[1] for row in conn.execute(text("PRAGMA table_info(finance_income_rows)")).fetchall()]
            if "wallet_name" not in income_columns:
                conn.execute(text("ALTER TABLE finance_income_rows ADD COLUMN wallet_name VARCHAR DEFAULT ''"))
                if "wallet" in income_columns:
                    conn.execute(text("UPDATE finance_income_rows SET wallet_name = wallet WHERE COALESCE(wallet_name, '') = ''"))
            if "from_wallet" not in income_columns:
                conn.execute(text("ALTER TABLE finance_income_rows ADD COLUMN from_wallet VARCHAR DEFAULT ''"))
            if "comment" not in income_columns:
                conn.execute(text("ALTER TABLE finance_income_rows ADD COLUMN comment VARCHAR DEFAULT ''"))
    ensure_table_once(
        "finance_tables",
        [
            FinanceWalletRow.__table__,
            FinanceExpenseRow.__table__,
            FinanceIncomeRow.__table__,
            FinanceTransferRow.__table__,
        ],
        sqlite_migration,
    )


def load_manual_finance():
    ensure_finance_tables()
    db = SessionLocal()
    try:
        return {
            "wallets": db.query(FinanceWalletRow).order_by(FinanceWalletRow.id.desc()).all(),
            "expenses": db.query(FinanceExpenseRow).order_by(FinanceExpenseRow.id.desc()).all(),
            "income": db.query(FinanceIncomeRow).order_by(FinanceIncomeRow.id.desc()).all(),
            "transfers": db.query(FinanceTransferRow).order_by(FinanceTransferRow.id.desc()).all(),
        }
    finally:
        db.close()

def import_tasks_dataframe(df, assigned_user, created_by_user):
    ensure_task_table()
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


def build_finance_form_data(wallet_item=None, expense_item=None, income_item=None, transfer_item=None):
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
            "income_wallet_name": income_item.wallet_name or income_item.wallet or "",
            "income_amount": format_int_or_float(income_item.amount),
            "income_from_wallet": income_item.from_wallet or income_item.reconciliation or "",
            "income_comment": income_item.comment or "",
        })
    if transfer_item:
        data.update({
            "transfer_edit_id": str(transfer_item.id),
            "transfer_date": transfer_item.transfer_date or "",
            "transfer_category": transfer_item.category or "",
            "transfer_amount": format_int_or_float(transfer_item.amount),
            "transfer_from_wallet": transfer_item.from_wallet or "",
            "transfer_to_wallet": transfer_item.to_wallet or "",
            "transfer_comment": transfer_item.comment or "",
        })
    return data


def normalize_date_for_compare(value):
    dt = parse_datetime_flexible(value)
    return dt.strftime("%Y-%m-%d") if dt else ""


def get_finance_year_options(manual):
    years = set()
    for item in manual.get("expenses", []):
        dt = parse_datetime_flexible(item.expense_date)
        if dt:
            years.add(str(dt.year))
    for item in manual.get("income", []):
        dt = parse_datetime_flexible(item.income_date)
        if dt:
            years.add(str(dt.year))
    for item in manual.get("transfers", []):
        dt = parse_datetime_flexible(item.transfer_date)
        if dt:
            years.add(str(dt.year))
    years.update({"2026", "2027"})
    return sorted(years)


def date_matches_filters(value, date_from="", date_to="", year=""):
    normalized = normalize_date_for_compare(value)
    if not normalized:
        return not (date_from or date_to or year)
    if year and not normalized.startswith(f"{year}-"):
        return False
    if date_from and normalized < date_from:
        return False
    if date_to and normalized > date_to:
        return False
    return True


def finance_date_matches_period(value, period_label=""):
    clean_period = safe_text(period_label)
    if not clean_period:
        return True
    return safe_text(get_half_month_period_from_date(value).get("period_label", "")) == clean_period


def filter_finance_manual_rows(manual, date_from="", date_to="", year="", period_label=""):
    return {
        "wallets": manual.get("wallets", []),
        "expenses": [item for item in manual.get("expenses", []) if date_matches_filters(item.expense_date, date_from, date_to, year) and finance_date_matches_period(item.expense_date, period_label)],
        "income": [item for item in manual.get("income", []) if date_matches_filters(item.income_date, date_from, date_to, year) and finance_date_matches_period(item.income_date, period_label)],
        "transfers": [item for item in manual.get("transfers", []) if date_matches_filters(item.transfer_date, date_from, date_to, year) and finance_date_matches_period(item.transfer_date, period_label)],
    }


def compute_finance_balances(snapshot, manual):
    balance_map = {}

    for item in snapshot.get("wallets", []):
        key = safe_text(item.get("wallet")) or safe_text(item.get("description")) or safe_text(item.get("owner")) or "Unknown"
        balance_map[key] = balance_map.get(key, 0.0) + safe_number(item.get("amount"))

    for item in manual.get("wallets", []):
        key = safe_text(item.wallet) or safe_text(item.description) or safe_text(item.owner_name) or f"Wallet {item.id}"
        balance_map[key] = balance_map.get(key, 0.0) + safe_number(item.amount)

    for item in manual.get("income", []):
        key = safe_text(item.wallet_name) or safe_text(item.wallet) or "Unassigned"
        balance_map[key] = balance_map.get(key, 0.0) + safe_number(item.amount)

    for item in manual.get("expenses", []):
        key = safe_text(item.wallet_name) or safe_text(item.from_wallet) or safe_text(item.paid_by) or "Unassigned"
        balance_map[key] = balance_map.get(key, 0.0) - safe_number(item.amount)

    for item in manual.get("transfers", []):
        from_key = safe_text(item.from_wallet)
        to_key = safe_text(item.to_wallet)
        if from_key:
            balance_map[from_key] = balance_map.get(from_key, 0.0) - safe_number(item.amount)
        if to_key:
            balance_map[to_key] = balance_map.get(to_key, 0.0) + safe_number(item.amount)

    rows = [{"wallet_name": key, "balance": value} for key, value in balance_map.items()]
    rows.sort(key=lambda x: x["balance"], reverse=True)
    return {
        "total": sum(item["balance"] for item in rows),
        "rows": rows,
    }


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
    fb_icon = '''
    <svg viewBox="0 0 64 64" aria-hidden="true" focusable="false">
        <defs>
            <linearGradient id="fbGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stop-color="#35c2ff"/>
                <stop offset="100%" stop-color="#1d4ed8"/>
            </linearGradient>
        </defs>
        <rect x="6" y="6" width="52" height="52" rx="12" fill="url(#fbGrad)"/>
        <path d="M38 18h6v8h-5c-1.5 0-2 0.7-2 2.1V33h7l-1.2 8H37v15h-8V41h-6v-8h6v-6c0-5.7 3.4-9 9-9Z" fill="#ffffff"/>
    </svg>
    '''
    finance_icon = '''
    <svg viewBox="0 0 64 64" aria-hidden="true" focusable="false">
        <defs>
            <linearGradient id="financeGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stop-color="#34d399"/>
                <stop offset="100%" stop-color="#059669"/>
            </linearGradient>
        </defs>
        <rect x="6" y="6" width="52" height="52" rx="12" fill="url(#financeGrad)"/>
        <g fill="none" stroke="#ffffff" stroke-width="3.1" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21 19.5h22"/>
            <path d="M32 18.5v26"/>
            <ellipse cx="32" cy="27.2" rx="12.5" ry="3.8"/>
            <path d="M24 33.8c2.8 1.4 5.5 2 8 2s5.2-.6 8-2"/>
        </g>
    </svg>
    '''
    chatterfy_icon = '''
    <svg viewBox="0 0 64 64" aria-hidden="true" focusable="false">
        <rect x="4" y="4" width="56" height="56" rx="12" fill="#16243f"/>
        <g fill="none" stroke="#dfe9ff" stroke-width="3.1" stroke-linecap="round" stroke-linejoin="round">
            <path d="M22 17.5l2.8-4.6"/>
            <path d="M42 17.5l2.8-4.6"/>
            <circle cx="24.8" cy="12.9" r="2.3" fill="#dfe9ff" stroke="none"/>
            <circle cx="44.8" cy="12.9" r="2.3" fill="#dfe9ff" stroke="none"/>
            <path d="M20 18h23c10.7 0 17 6.9 17 16.6 0 8.7-5.6 14.8-14.4 16.5l4 7-10.5-6.5H29.5C19 51.6 12 44.1 12 34.6 12 24.4 18.7 18 29 18"/>
        </g>
        <ellipse cx="25.5" cy="31.5" rx="3.2" ry="4.8" fill="#dfe9ff"/>
        <ellipse cx="38.8" cy="31.5" rx="3.2" ry="4.8" fill="#dfe9ff"/>
        <path d="M28.3 42.3c2.2 1.9 4.8 2.4 7.4 0" fill="none" stroke="#dfe9ff" stroke-width="3" stroke-linecap="round"/>
    </svg>
    '''
    items = [
        ("dashboard", "/dashboard", "📊", "Dashboard", []),
        ("fb", "/fb", fb_icon, "FB", []),
        ("export", "/partner-report", "📩", "Export", [
            ("/chatterfy", "Chatterfy", active_page == "chatterfy", chatterfy_icon),
            ("/partner-report", "Players", active_page == "partner", "👤"),
            ("/grouped", "Export FB", active_page == "grouped", "📈"),
        ]),
        ("finance", "/finance", finance_icon, "Finance", []),
        ("caps", "/caps", "📌", "Caps", []),
        ("cabinets", "/cabinets", "🛠", "Partners", []),
        ("holdwager", "/hold-wager", "✏️", "Hold", []),
    ]

    html = '''
    <aside class="sidebar">
        <div class="sidebar-brand">
            <div class="sidebar-brand-main">
                <span class="brand-mark"></span>
                <span class="sidebar-brand-text">TEAMbead CRM</span>
            </div>
            <button type="button" class="sidebar-collapse-btn" id="sidebarCollapseBtn" aria-label="Toggle sidebar" aria-expanded="true">‹</button>
        </div>
    '''

    for key, href, icon, title, children in items:
        if key == "export":
            export_children = []
            if can_access_page(current_user, "chatterfy"):
                export_children.append(("/chatterfy", "Chatterfy", active_page == "chatterfy", chatterfy_icon))
            if can_access_page(current_user, "partner"):
                export_children.append(("/partner-report", "Players", active_page == "partner", "👤"))
            if can_access_page(current_user, "grouped"):
                export_children.append(("/grouped", "Export FB", active_page == "grouped", "📈"))
            if not export_children:
                continue
            children = export_children
        elif key == "fb":
            if not can_access_page(current_user, "fb"):
                continue
        elif key == "grouped":
            if not can_access_page(current_user, "grouped"):
                continue
            children = [
                child for child in children
                if can_access_page(current_user, "grouped")
            ]
        elif key == "dashboard":
            if not can_access_page(current_user, "hierarchy"):
                continue
        elif not can_access_page(current_user, key):
            continue

        if children:
            open_attr = "open" if active_page in ["grouped", "partner", "chatterfy"] else ""
            html += f'''
            <details class="sidebar-group" {open_attr}>
                <summary><span class="side-emoji">{icon}</span><span class="side-label">{title}</span></summary>
                <div class="sidebar-links">
            '''
            for child_href, child_title, active, child_icon in children:
                active_class = "active-link" if active else ""
                icon_html = child_icon if safe_text(child_icon).startswith("<") else escape(safe_text(child_icon))
                html += f'<a href="{child_href}" class="{active_class}"><span class="side-emoji side-sub-emoji">{icon_html}</span><span class="side-label">{child_title}</span></a>'
            html += '</div></details>'
        else:
            active_class = "sidebar-standalone active-link" if active_page == key else "sidebar-standalone"
            html += f'<a href="{href}" class="{active_class}"><span class="side-emoji">{icon}</span><span class="side-label">{title}</span></a>'

    bottom_links = ""
    if can_access_page(current_user, "tasks"):
        active_class = "sidebar-standalone subtle-link active-link" if active_page == "tasks" else "sidebar-standalone subtle-link"
        bottom_links += f'<a href="/tasks" class="{active_class}"><span class="side-emoji">✅</span><span class="side-label">Tasks</span></a>'
    if can_access_page(current_user, "users"):
        active_class = "sidebar-standalone subtle-link active-link" if active_page == "users" else "sidebar-standalone subtle-link"
        bottom_links += f'<a href="/users" class="{active_class}"><span class="side-emoji">🧑</span><span class="side-label">Users</span></a>'

    html += f'''
        <div class="sidebar-bottom">
            <div class="sidebar-bottom-links">{bottom_links}</div>
            <div class="sidebar-user">
                <div class="user-chip sidebar-user-chip">👤 {escape((current_user or {}).get("display_name", "Гость"))} · {escape(role_label((current_user or {}).get("role", "guest")))}</div>
                <div class="sidebar-datetime" id="sidebarDateTime">28.03.2026 · 00:00 Kyiv GMT+2</div>
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
    storage_namespace = json.dumps(safe_text((current_user or {}).get("username") or "guest"))
    return f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <link rel="icon" type="image/jpeg" href="/favicon.jpg?v=3">
        <link rel="shortcut icon" href="/favicon.ico?v=3">
        <title>{escape(title)}</title>
        <script>
            (function() {{
                try {{
                    var key = 'teambead:' + {storage_namespace} + ':theme';
                    if (localStorage.getItem(key) === 'light') {{
                        document.documentElement.classList.add('light');
                    }}
                }} catch (error) {{}}
                document.documentElement.classList.add('theme-ready');
            }})();
        </script>
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
            html.light {{
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
            html:not(.theme-ready) body {{ visibility: hidden; }}
            body {{
                margin: 0;
                background: radial-gradient(circle at top right, rgba(56,189,248,0.10), transparent 23%), var(--bg);
                color: var(--text);
                font-family: "Avenir Next", "Nunito", "Trebuchet MS", "Segoe UI", Arial, sans-serif;
                color-scheme: light;
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
            html.light .sidebar {{ background: linear-gradient(180deg, #ffffff, #f3f8ff); }}
            .sidebar-brand {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 12px;
                margin-bottom: 18px;
                color: var(--text);
            }}
            .sidebar-brand-main {{
                display: flex;
                align-items: center;
                gap: 10px;
                min-width: 0;
                font-size: 18px;
                font-weight: 900;
            }}
            .sidebar-brand-text {{
                white-space: nowrap;
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
            .sidebar-collapse-btn {{
                width: 34px;
                min-width: 34px;
                height: 34px;
                border-radius: 12px;
                border: 1px solid var(--border);
                background: var(--panel);
                color: var(--text);
                display: inline-flex;
                align-items: center;
                justify-content: center;
                cursor: pointer;
                box-shadow: var(--shadow);
                font-size: 18px;
                font-weight: 900;
                transition: transform .18s ease, background .18s ease;
            }}
            .sidebar-collapse-btn:hover {{
                background: var(--soft);
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
                user-select: none;
                -webkit-user-select: none;
            }}
            .sidebar-group summary::-webkit-details-marker {{ display: none; }}
            .sidebar-group summary::after {{
                content: "▾";
                margin-left: auto;
                opacity: .72;
                font-size: 13px;
            }}
            .sidebar-group[open] summary::after {{ transform: rotate(180deg); }}
            .side-emoji {{
                width: 22px;
                height: 22px;
                text-align: center;
                font-size: 18px;
                flex-shrink: 0;
                display: inline-flex;
                align-items: center;
                justify-content: center;
            }}
            .side-emoji svg {{
                width: 22px;
                height: 22px;
                display: block;
            }}
            .side-sub-emoji {{
                width: 20px;
                height: 20px;
                font-size: 16px;
            }}
            .side-sub-emoji svg {{
                width: 20px;
                height: 20px;
            }}
            .sidebar-links {{ display: flex; flex-direction: column; gap: 8px; padding: 0 10px 10px; }}
            .sidebar-links a {{
                text-decoration: none;
                color: var(--text);
                padding: 10px 12px;
                border-radius: 12px;
                font-weight: 800;
                display: flex;
                align-items: center;
                gap: 10px;
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
            .sidebar-datetime {{
                border: 1px solid var(--border);
                background: var(--panel-2);
                border-radius: 14px;
                padding: 10px 12px;
                text-align: center;
                font-size: 12px;
                font-weight: 800;
                color: var(--muted);
                letter-spacing: 0.02em;
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
            .app.sidebar-collapsed .sidebar {{
                width: 92px;
                padding-left: 10px;
                padding-right: 10px;
            }}
            .app.sidebar-collapsed .sidebar-brand {{
                justify-content: center;
            }}
            .app.sidebar-collapsed .sidebar-brand-main {{
                justify-content: center;
            }}
            .app.sidebar-collapsed .sidebar-brand-text,
            .app.sidebar-collapsed .side-label,
            .app.sidebar-collapsed .sidebar-group summary::after,
            .app.sidebar-collapsed .sidebar-links,
            .app.sidebar-collapsed .sidebar-user-chip,
            .app.sidebar-collapsed .sidebar-datetime,
            .app.sidebar-collapsed .sidebar-mini-actions {{
                display: none !important;
            }}
            .app.sidebar-collapsed .sidebar-group,
            .app.sidebar-collapsed .sidebar-standalone {{
                border-radius: 14px;
            }}
            .app.sidebar-collapsed .sidebar-group summary,
            .app.sidebar-collapsed .sidebar-standalone {{
                justify-content: center;
                padding: 12px 10px;
                gap: 0;
            }}
            .app.sidebar-collapsed .side-emoji {{
                width: 24px;
                height: 24px;
                font-size: 20px;
            }}
            .app.sidebar-collapsed .sidebar-bottom {{
                gap: 10px;
            }}
            .app.sidebar-collapsed .sidebar-bottom-links {{
                display: grid;
                grid-template-columns: 1fr;
                gap: 10px;
            }}
            .app.sidebar-collapsed .sidebar-bottom-links .sidebar-standalone {{
                padding: 10px;
            }}
            .app.sidebar-collapsed .sidebar-user {{
                padding: 10px;
                justify-items: center;
            }}
            .app.sidebar-collapsed .sidebar-collapse-btn {{
                transform: rotate(180deg);
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
                user-select: none;
                -webkit-user-select: none;
            }}
            .upload-menu > summary::-webkit-details-marker {{ display:none; }}
            .upload-menu[open] > summary {{
                background: linear-gradient(90deg, rgba(37,99,235,0.95), rgba(56,189,248,0.92));
                color: #ffffff;
            }}
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
            .upload-menu-left .upload-menu-list {{
                left: 0;
                right: auto;
            }}
            .upload-menu-right .upload-menu-list {{
                right: 0;
                left: auto;
            }}
            .upload-menu-list form {{
                display:grid;
                gap:10px;
                align-items:end;
            }}
            .cap-menu-list {{
                width: min(500px, calc(100vw - 48px));
                gap: 10px;
                padding: 12px;
                border-radius: 16px;
            }}
            .cap-menu-list .caps-form {{
                gap: 8px;
            }}
            .cap-menu-list .caps-grid-2 {{
                gap: 8px;
            }}
            .cap-menu-list .panel-subtitle {{
                margin: 0;
                padding-bottom: 2px;
            }}
            .cap-menu-list label {{
                gap: 5px;
                font-size: 11px;
            }}
            .cap-menu-list input,
            .cap-menu-list select,
            .cap-menu-list textarea {{
                padding: 9px 10px;
                border-radius: 11px;
                font-size: 13px;
            }}
            .cap-menu-list textarea {{
                min-height: 84px;
            }}
            .cap-edit-drawer {{
                position: fixed;
                top: 92px;
                right: 16px;
                bottom: 16px;
                width: min(430px, calc(100vw - 32px));
                display: none;
                z-index: 95;
            }}
            .cap-edit-drawer.open {{
                display: block;
            }}
            .cap-edit-drawer .cap-edit-card {{
                height: 100%;
                overflow: auto;
                padding: 14px;
                border-radius: 20px;
                border: 1px solid var(--border);
                background: linear-gradient(180deg, var(--panel), var(--panel-2));
                box-shadow: var(--shadow);
            }}
            .cap-edit-head {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 10px;
                margin-bottom: 12px;
                position: sticky;
                top: 0;
                padding-bottom: 8px;
                background: linear-gradient(180deg, var(--panel), var(--panel-2));
                z-index: 2;
            }}
            .cap-edit-title {{
                font-size: 18px;
                font-weight: 900;
            }}
            .cap-edit-close {{
                width: 36px;
                min-width: 36px;
                height: 36px;
                padding: 0 !important;
                border-radius: 12px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                font-size: 18px;
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
            .upload-menu-list select,
            .upload-menu-list textarea {{
                width:100%;
                min-width:0;
                border-radius:12px;
                border:1px solid var(--border);
                background: var(--panel-3);
                color: var(--text);
                padding: 11px 12px;
                outline:none;
            }}
            .upload-menu-list textarea {{
                min-height: 96px;
                resize: vertical;
            }}
            select {{
                width: 100%;
                min-width: 0;
                min-height: 40px;
                border-radius: 14px;
                border: 1px solid var(--border);
                background-color: var(--panel-3);
                color: var(--text);
                padding: 10px 38px 10px 12px;
                outline: none;
                font: inherit;
                font-size: 14px;
                font-weight: 800;
                line-height: 1.2;
                box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
                appearance: auto;
                -webkit-appearance: menulist;
                box-sizing: border-box;
            }}
            select:hover {{
                border-color: rgba(56,189,248,0.3);
                background-color: var(--panel-3);
            }}
            select:focus,
            select:focus-visible {{
                border-color: rgba(56,189,248,0.58);
                box-shadow: 0 0 0 3px rgba(56,189,248,0.14);
            }}
            select option {{
                background: #f5f8fc;
                color: #12213d;
                font-weight: 700;
            }}
            html.light select option {{
                background: #f5f8fc;
                color: #12213d;
            }}
            input[type="file"]::file-selector-button {{
                border: 1px solid var(--border);
                background: var(--panel-2);
                color: var(--text);
                border-radius: 10px;
                padding: 8px 10px;
                margin-right: 10px;
                font-weight: 800;
                cursor: pointer;
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
                min-width:38px;
                min-height:38px;
                padding:0;
                border-radius:12px;
                display:inline-flex;
                align-items:center;
                justify-content:center;
                align-self:flex-start;
                flex: 0 0 auto;
                position: relative;
                overflow:hidden;
                user-select: none;
                -webkit-user-select: none;
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
            details[open] > summary.toggle-indicator::before {{
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
                font-family: inherit;
                letter-spacing: 0;
                white-space: nowrap;
                appearance: none;
                -webkit-appearance: none;
                text-rendering: geometricPrecision;
                user-select: none;
                -webkit-user-select: none;
            }}
            .ghost-btn {{ background: var(--panel-2); color: var(--text); }}
            .small-btn {{ padding: 10px 14px; font-size: 13px; }}
            .filters button, .filters a {{
                font-family: inherit !important;
                font-size: 13px !important;
                font-weight: 900 !important;
                line-height: 1 !important;
            }}
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
            .panel > summary {{
                user-select: none;
                -webkit-user-select: none;
            }}
            .compact-panel {{ padding: 14px 16px; }}
            .panel.compact-panel.filters {{
                padding: 0 0 8px;
                border-radius: 0;
                margin-bottom: 12px;
                background: transparent;
                border: 0;
                box-shadow: none;
            }}
            .panel.compact-panel.filters .panel-title {{
                font-size: 13px;
                margin-bottom: 6px;
                color: var(--muted);
                text-transform: uppercase;
                letter-spacing: 0.3px;
            }}
            .panel.compact-panel.filters form {{
                display: flex;
                gap: 8px;
                flex-wrap: wrap;
                align-items: end;
                padding: 8px 10px;
                border: 1px solid var(--border);
                border-radius: 14px;
                background: linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01));
                box-shadow: none;
            }}
            .panel.compact-panel.filters label {{
                min-width: 112px;
            }}
            .period-picker {{
                display: flex;
                align-items: end;
                gap: 4px;
                min-width: 0;
                flex: 0 1 auto;
            }}
            .period-picker label {{
                min-width: 0 !important;
                flex: 1 1 auto;
            }}
            .period-jump-btn {{
                width: 38px !important;
                min-width: 38px !important;
                max-width: 38px !important;
                height: 38px !important;
                min-height: 38px !important;
                padding: 0 !important;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                border-radius: 10px;
                font-size: 15px;
                line-height: 1;
            }}
            .period-picker select {{
                min-width: 200px !important;
            }}
            .filter-actions-stack {{
                display: flex;
                gap: 8px;
                align-self: end;
                align-items: center;
            }}
            .filter-actions-inline {{
                display: flex;
                gap: 8px;
                align-self: end;
                align-items: center;
            }}
            .filter-actions-stack .btn,
            .filter-actions-stack .ghost-btn,
            .filter-actions-inline .btn,
            .filter-actions-inline .ghost-btn {{
                width: 104px;
                min-width: 104px;
            }}
            .filter-actions-stack .filter-reset-btn {{
                width: 38px !important;
                min-width: 38px !important;
                max-width: 38px !important;
                height: 38px !important;
                min-height: 38px !important;
                padding: 0 !important;
                border-radius: 10px;
                font-size: 18px !important;
                line-height: 1 !important;
            }}
            .filter-actions-inline .filter-reset-btn {{
                width: 38px !important;
                min-width: 38px !important;
                max-width: 38px !important;
                height: 38px !important;
                min-height: 38px !important;
                padding: 0 !important;
                border-radius: 10px;
                font-size: 18px !important;
                line-height: 1 !important;
            }}
            .panel.compact-panel.filters input,
            .panel.compact-panel.filters select {{
                min-width: 128px;
                height: 38px;
            }}
            .panel.compact-panel.filters .btn,
            .panel.compact-panel.filters .ghost-btn {{
                height: 38px;
            }}
            .toolbar-actions {{
                display:flex;
                gap:10px;
                align-items:center;
                flex-wrap:wrap;
                justify-content:flex-end;
                width:100%;
            }}
            .toolbar-actions .panel.compact-panel.filters {{
                order:1;
                flex: 1 1 620px;
                min-width: min(620px, 100%);
                margin:0;
                align-self: flex-start;
            }}
            .toolbar-actions .upload-menu {{
                order:4;
                flex: 0 0 auto;
                align-self: flex-start;
            }}
            .players-toolbar {{
                align-items: flex-start;
            }}
            .players-toolbar .panel.compact-panel.filters {{
                order: 1;
                flex: 1 1 760px;
                min-width: min(760px, 100%);
            }}
            .players-toolbar .caps-toolbar-stats {{
                order: 2;
            }}
            .players-toolbar .players-filter-form {{
                display: flex;
                gap: 8px;
                align-items: end;
                flex-wrap: nowrap;
            }}
            .players-toolbar .players-filter-form label {{
                min-width: 0;
            }}
            .players-toolbar .players-filter-form label.period-label {{
                flex: 1 1 260px;
            }}
            .players-toolbar .players-filter-form label.cabinet-label {{
                flex: 1 1 200px;
            }}
            .players-toolbar .players-filter-form label.country-label {{
                flex: 0 0 150px;
            }}
            .players-toolbar .players-filter-form label.search-label {{
                flex: 1 1 190px;
            }}
            .players-toolbar .players-side-tools {{
                order: 3;
                display: grid;
                gap: 8px;
                align-items: start;
                justify-items: end;
                margin-left: auto;
                flex: 0 0 auto;
            }}
            .players-toolbar .players-upload-filter {{
                width: 170px;
                padding: 8px 10px;
                border-radius: 14px;
                border: 1px solid var(--border);
                background: linear-gradient(180deg, var(--panel), var(--panel-3));
                box-shadow: var(--shadow);
            }}
            .players-toolbar .players-upload-filter label {{
                display: grid;
                gap: 5px;
                font-size: 12px;
                font-weight: 800;
            }}
            .players-toolbar .players-upload-filter select {{
                width: 100%;
            }}
            .players-toolbar .players-icon-stack {{
                display: flex;
                gap: 8px;
                justify-content: flex-end;
                width: 100%;
            }}
            .toolbar-actions .caps-toolbar-stats {{
                order:2;
                flex: 0 1 auto;
                display: flex;
                align-items: stretch;
                gap: 8px;
                flex-wrap: wrap;
                justify-content: flex-end;
                align-self: flex-start;
            }}
            .caps-toolbar-stats .mini-stat {{
                min-width: 108px;
                padding: 9px 12px;
                border-radius: 14px;
                border: 1px solid var(--border);
                background: linear-gradient(180deg, var(--panel), var(--panel-3));
                display: grid;
                gap: 3px;
            }}
            .caps-toolbar-stats .mini-stat .name {{
                font-size: 11px;
                color: var(--muted);
                font-weight: 700;
                text-transform: none;
                letter-spacing: 0;
            }}
            .caps-toolbar-stats .mini-stat .value {{
                font-size: 22px;
                font-weight: 900;
                line-height: 1.05;
            }}
            .toolbar-actions .column-menu-wrap {{
                order:3;
                flex: 0 0 auto;
            }}
            .panel-title {{ font-size: 15px; font-weight: 900; margin-bottom: 12px; }}
            .panel-subtitle {{ color: var(--muted); font-size: 13px; }}
            .toolbar-grid {{ display: grid; grid-template-columns: 1.35fr 1fr; gap: 16px; align-items: start; margin-bottom: 16px; }}
            .upload-form, .filters form {{ display: flex; gap: 8px; flex-wrap: wrap; align-items: end; }}
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
                overflow-x: auto;
                overflow-y: visible;
                border-radius: 18px;
                border: 1px solid var(--border);
                box-shadow: var(--shadow);
                background: var(--panel);
            }}
            .table-sticky-header {{
                position: fixed;
                top: 10px;
                left: 0;
                z-index: 64;
                display: none;
                pointer-events: none;
            }}
            .table-sticky-header.open {{
                display: block;
            }}
            .table-sticky-header .table-wrap {{
                overflow: hidden;
                border-radius: 16px 16px 0 0;
            }}
            .table-sticky-header table {{
                min-width: 0;
                margin: 0;
            }}
            .table-sticky-header th {{
                position: static !important;
                top: auto !important;
                box-shadow: none !important;
            }}
            table {{ border-collapse: separate; border-spacing: 0; width: 100%; min-width: 1450px; background: var(--panel); color: var(--text); }}
            th, td {{
                border-right: 1px solid var(--border);
                border-bottom: 1px solid var(--border);
                padding: 11px 12px;
                text-align: left;
                font-size: 13px;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                position: relative;
                line-height: 1.25;
                vertical-align: middle;
            }}
            tbody tr {{ height: 52px; }}
            .table-wrap form {{ margin: 0; }}
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
            .th-inner {{ display: flex; align-items: center; justify-content: space-between; gap: 8px; padding-right: 10px; font-size: 12px; font-weight: 800; line-height: 1.15; }}
            th, th a, th span {{ font-size: 12px; font-weight: 800; line-height: 1.15; }}
            .drag-handle {{ cursor: grab; opacity: 0.72; font-size: 11px; }}
            .dragging {{ opacity: 0.45; }}
            .stat-cell-right {{ text-align: right; }}
            .stat-cell-wrap {{ white-space: normal; min-width: 180px; }}
            .flow-badge {{
                display: inline-flex;
                flex-wrap: wrap;
                gap: 6px;
                align-items: center;
            }}
            .flow-badge span {{
                display:inline-flex;
                align-items:center;
                padding: 6px 10px;
                border-radius: 999px;
                background: var(--chip);
                border: 1px solid var(--border);
                font-size: 12px;
                font-weight: 800;
            }}
            .status-chip {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                padding: 6px 10px;
                border-radius: 999px;
                border: 1px solid var(--border);
                background: var(--chip);
                font-size: 12px;
                font-weight: 800;
                white-space: nowrap;
            }}
            .status-chip-blocked {{
                background: rgba(239, 68, 68, 0.16);
                color: #dc2626;
                border-color: rgba(239, 68, 68, 0.28);
            }}
            .status-chip-waiting {{
                background: rgba(245, 158, 11, 0.16);
                color: #b45309;
                border-color: rgba(245, 158, 11, 0.28);
            }}
            .status-chip-manual {{
                background: rgba(249, 115, 22, 0.16);
                color: #c2410c;
                border-color: rgba(249, 115, 22, 0.28);
            }}
            .status-chip-active {{
                background: rgba(34, 197, 94, 0.14);
                color: #15803d;
                border-color: rgba(34, 197, 94, 0.24);
            }}
            .status-icon {{
                display:inline-flex;
                align-items:center;
                justify-content:center;
                width:30px;
                height:30px;
                border-radius:999px;
                border:1px solid var(--border);
                font-size:14px;
                font-weight:900;
                line-height:1;
                user-select:none;
                -webkit-user-select:none;
            }}
            .status-icon-blocked {{
                background: linear-gradient(180deg, rgba(220, 38, 38, 0.22), rgba(127, 29, 29, 0.18));
                color: #b91c1c;
                border-color: rgba(220, 38, 38, 0.42);
                box-shadow: inset 0 0 0 1px rgba(255,255,255,0.08);
            }}
            .status-icon-waiting {{
                background: rgba(245, 158, 11, 0.18);
                color: #b45309;
                border-color: rgba(245, 158, 11, 0.3);
            }}
            .status-icon-manual {{
                background: rgba(250, 204, 21, 0.22);
                color: #7c5c00;
                border-color: rgba(250, 204, 21, 0.38);
            }}
            .status-icon-active {{
                background: rgba(34, 197, 94, 0.16);
                color: #15803d;
                border-color: rgba(34, 197, 94, 0.28);
            }}
            .flag-form {{
                display: flex;
                align-items: center;
                gap: 8px;
                min-width: 0;
                flex-wrap: nowrap;
            }}
            .flag-form .panel-subtitle {{ display: none; }}
            .flag-check {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                font-weight: 800;
                font-size: 13px;
                white-space: nowrap;
            }}
            .flag-check input {{
                width: 16px;
                height: 16px;
                accent-color: var(--accent1);
            }}
            .caps-actions {{
                display: grid;
                gap: 8px;
                align-items: stretch;
                justify-items: stretch;
                min-width: 0;
                width: 100%;
            }}
            .caps-actions form {{
                margin: 0;
                width: 100%;
                flex: 0 0 auto;
            }}
            .caps-actions .ghost-btn,
            .caps-actions .btn,
            .caps-actions button {{
                width: 100%;
                min-width: 0;
                text-align: center;
            }}
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
            .caps-table {{ min-width: 1000px; table-layout: fixed; width: 100%; }}
            .caps-table-wrap {{
                overflow-x: auto;
                overflow-y: visible;
            }}
            .caps-sticky-header {{
                position: fixed;
                top: 10px;
                left: 0;
                z-index: 70;
                display: none;
                pointer-events: none;
            }}
            .caps-sticky-header.open {{
                display: block;
            }}
            .caps-sticky-header .table-wrap {{
                overflow: hidden;
                border-radius: 16px 16px 0 0;
                box-shadow: var(--shadow);
            }}
            .caps-sticky-header table {{
                min-width: 0;
                margin: 0;
            }}
            .caps-sticky-header th {{
                position: static !important;
                top: auto !important;
                box-shadow: none !important;
            }}
            .caps-table th, .caps-table td {{
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                vertical-align: middle;
                line-height: 1.2;
                padding: 7px 6px;
                font-size: 12px;
                box-sizing: border-box;
            }}
            .caps-table thead th {{
                font-size: 11px;
                padding: 7px 6px;
                position: sticky;
                top: 0;
                z-index: 6;
                background: var(--table-head);
                box-shadow: 0 1px 0 var(--border);
            }}
            .caps-table tbody tr {{ height: 40px; }}
            .caps-table .advertiser-col {{ width: 82px; min-width: 82px; }}
            .caps-table .owner-col {{ width: 82px; min-width: 82px; }}
            .caps-table .buyer-col {{ width: 92px; min-width: 92px; }}
            .caps-table .code-col {{ width: 54px; min-width: 54px; }}
            .caps-table .geo-col {{ width: 84px; min-width: 84px; }}
            .caps-table .rate-col {{ width: 56px; min-width: 56px; }}
            .caps-table .baseline-col {{ width: 62px; min-width: 62px; }}
            .caps-table .cap-col {{ width: 56px; min-width: 56px; }}
            .caps-table .current-col {{ width: 72px; min-width: 72px; }}
            .caps-table .remaining-col {{ width: 72px; min-width: 72px; }}
            .caps-table .fill-col {{ width: 86px; min-width: 86px; }}
            .caps-table .promo-col {{ width: 78px; min-width: 78px; }}
            .caps-table .agent-col {{ width: 70px; min-width: 70px; }}
            .caps-table .chat-col {{ width: 108px; min-width: 108px; }}
            .caps-table .kpi-col {{ width: 72px; min-width: 72px; }}
            .caps-table .comment-col {{ width: 72px; min-width: 72px; }}
            .caps-table .link-col {{ width: 70px; min-width: 70px; }}
            .caps-table .action-col {{ width: 76px; min-width: 76px; }}
            .caps-table th .th-inner {{
                justify-content: flex-start;
                gap: 6px;
                padding-right: 14px;
            }}
            .caps-table th .resizer {{
                right: 0;
            }}
            .caps-table .ghost-btn,
            .caps-table .btn,
            .caps-table button {{
                min-height: 30px;
                padding: 7px 8px;
                font-size: 11px;
                border-radius: 10px;
            }}
            .caps-table td.link-col {{
                text-align: center;
            }}
            .caps-table td.link-col .cap-copy-link {{
                margin: 0 auto;
                display: inline-flex;
                justify-content: center;
                align-items: center;
                width: 32px;
                min-width: 32px;
                max-width: 32px;
                height: 32px;
                padding: 0;
                border-radius: 10px;
                font-size: 14px;
                font-weight: 800;
                line-height: 1;
            }}
            .caps-table td.link-col .cap-copy-link.is-copied {{
                border-color: rgba(34, 197, 94, 0.36) !important;
                background: linear-gradient(90deg, rgba(74, 222, 128, 0.92), rgba(34, 197, 94, 0.9)) !important;
                color: #ffffff !important;
            }}
            .partners-table th {{
                text-align: center;
            }}
            .partners-table {{
                min-width: 0;
                table-layout: fixed;
                width: 100%;
            }}
            .partners-table th,
            .partners-table td {{
                vertical-align: middle;
                padding: 9px 8px;
                font-size: 12px;
                line-height: 1.2;
                box-sizing: border-box;
            }}
            .partners-table thead th {{
                font-size: 11px;
                padding: 8px 8px;
                position: sticky;
                top: 0;
                z-index: 6;
                background: var(--table-head);
                box-shadow: 0 1px 0 var(--border);
            }}
            .partners-table tbody tr {{
                height: 46px;
            }}
            .partners-table th .th-inner {{
                justify-content: center;
                gap: 0;
                padding: 0 18px;
                position: relative;
            }}
            .partners-table th .drag-handle {{
                position: absolute;
                left: 0;
            }}
            .partners-table th .resizer {{
                right: 0;
            }}
            .partners-table .advertiser-col {{ width: 76px; min-width: 76px; }}
            .partners-table .platform-col {{ width: 74px; min-width: 74px; }}
            .partners-table .cabinet-col {{ width: 108px; min-width: 108px; }}
            .partners-table .code-col {{ width: 78px; min-width: 78px; }}
            .partners-table .geo-col {{ width: 118px; min-width: 118px; }}
            .partners-table .brands-col {{ width: 84px; min-width: 84px; }}
            .partners-table .tg-col {{ width: 62px; min-width: 62px; }}
            .partners-table .manager-col {{ width: 78px; min-width: 78px; }}
            .partners-table .manager-contact-col {{ width: 108px; min-width: 108px; }}
            .partners-table .chat-name-col {{ width: 104px; min-width: 104px; }}
            .partners-table .wallet-col {{ width: 150px; min-width: 150px; }}
            .partners-table .comments-col {{ width: 112px; min-width: 112px; }}
            .partners-table .actions-col {{ width: 78px; min-width: 78px; text-align: center; }}
            .partners-table .wallet-col,
            .partners-table .comments-col,
            .partners-table .geo-col,
            .partners-table .brands-col,
            .partners-table .chat-name-col {{
                white-space: normal;
                overflow-wrap: anywhere;
                word-break: break-word;
            }}
            .players-report-table thead th,
            .players-report-table thead th a {{
                text-align: center !important;
            }}
            .table-icon-actions {{
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 8px;
                flex-wrap: nowrap;
            }}
            .table-icon-actions form {{
                margin: 0;
            }}
            .action-icon-btn {{
                width: 34px;
                min-width: 34px;
                max-width: 34px;
                height: 34px;
                padding: 0 !important;
                border-radius: 10px !important;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                font-size: 16px !important;
                line-height: 1;
            }}
            .partners-toolbar {{
                display: flex;
                gap: 12px;
                align-items: flex-start;
                flex-wrap: wrap;
                margin-bottom: 14px;
            }}
            .partners-toolbar .panel.compact-panel.filters {{
                flex: 1 1 620px;
                min-width: min(620px, 100%);
                margin: 0;
                align-self: flex-start;
            }}
            .partners-toolbar .upload-menu {{
                flex: 0 0 auto;
                align-self: flex-start;
                display: flex;
                align-items: flex-start;
            }}
            .partners-toolbar .upload-menu > summary.toggle-indicator {{
                width: 58px !important;
                min-width: 58px !important;
                max-width: 58px !important;
                height: 58px !important;
                min-height: 58px !important;
                padding: 0 !important;
            }}
            .toolbar-square-trigger {{
                width: 58px !important;
                min-width: 58px !important;
                max-width: 58px !important;
                height: 58px !important;
                min-height: 58px !important;
                padding: 0 !important;
                border-radius: 14px !important;
            }}
            .toolbar-square-icon-btn {{
                width: 58px !important;
                min-width: 58px !important;
                max-width: 58px !important;
                height: 58px !important;
                min-height: 58px !important;
                padding: 0 !important;
                border-radius: 14px !important;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                font-size: 24px !important;
                line-height: 1 !important;
            }}
            .caps-toolbar-panel {{
                padding-top: 12px;
                padding-bottom: 8px;
            }}
            .progress-shell {{
                min-width: 0;
                display:grid;
                gap:5px;
                font-size: 12px;
                line-height: 1.25;
            }}
            .fill-meta {{
                min-width: 0;
                display: flex;
                align-items: center;
                gap: 4px;
                white-space: nowrap;
                overflow: hidden;
            }}
            .fill-meta strong {{
                flex: 0 0 auto;
            }}
            .fill-status {{
                min-width: 0;
                overflow: hidden;
                text-overflow: ellipsis;
                font-size: 11px;
                font-weight: 700;
            }}
            .progress-bar {{ height: 8px; }}
            .confirm-overlay {{
                position: fixed;
                inset: 0;
                background: rgba(7, 16, 31, 0.64);
                display: none;
                align-items: center;
                justify-content: center;
                z-index: 120;
                padding: 20px;
            }}
            .confirm-overlay.open {{ display: flex; }}
            .confirm-card {{
                width: min(100%, 460px);
                border-radius: 22px;
                border: 1px solid var(--border);
                background: linear-gradient(180deg, var(--panel), var(--panel-2));
                box-shadow: var(--shadow);
                padding: 22px;
                display: grid;
                gap: 16px;
            }}
            .confirm-title {{ font-size: 22px; font-weight: 900; }}
            .confirm-text {{ color: var(--muted); font-size: 14px; line-height: 1.5; }}
            .confirm-actions {{ display: flex; justify-content: flex-end; gap: 10px; flex-wrap: wrap; }}
            .danger-btn {{
                border: 1px solid rgba(239,68,68,0.62) !important;
                background: linear-gradient(90deg, rgba(185,28,28,0.96), rgba(239,68,68,0.92)) !important;
                color: #ffffff !important;
            }}
            .danger-btn:hover {{
                background: linear-gradient(90deg, rgba(153,27,27,1), rgba(220,38,38,0.96)) !important;
            }}
            .finance-grid {{ display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:16px; }}
            .finance-table {{ min-width: 980px; }}
            .finance-table th, .finance-table td {{
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                vertical-align: middle;
            }}
            .wallet-code {{
                display: block;
                max-width: 240px;
                font-family: "Menlo", "Monaco", monospace;
                font-size: 12px;
                line-height: 1.35;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }}
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
            .progress-shell.progress-free {{
                color: #4e9b72;
            }}
            .progress-shell.progress-free .progress-bar {{
                background: rgba(34, 197, 94, 0.12);
            }}
            .progress-shell.progress-free .progress-bar > span {{
                background: linear-gradient(90deg, #9fe3b7, #ccefd8);
            }}
            .progress-shell.progress-filling {{
                color: #b78a1b;
            }}
            .progress-shell.progress-filling .progress-bar {{
                background: rgba(245, 186, 59, 0.13);
            }}
            .progress-shell.progress-filling .progress-bar > span {{
                background: linear-gradient(90deg, #d7a13a, #e7c46d);
            }}
            .progress-shell.progress-overflow {{
                color: #c96f72;
            }}
            .progress-shell.progress-overflow .progress-bar {{
                background: rgba(226, 128, 128, 0.13);
            }}
            .progress-shell.progress-overflow .progress-bar > span {{
                background: linear-gradient(90deg, #d58b8d, #c96f72);
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
            window.teambeadStorageNamespace = {storage_namespace};
            window.teambeadStorageKey = function(base) {{
                return 'teambead:' + window.teambeadStorageNamespace + ':' + base;
            }};
            function setTheme(mode) {{
                if (mode === 'light') {{
                    document.documentElement.classList.add('light');
                    document.body.classList.add('light');
                }} else {{
                    document.documentElement.classList.remove('light');
                    document.body.classList.remove('light');
                }}
                localStorage.setItem(window.teambeadStorageKey('theme'), mode === 'light' ? 'light' : 'dark');
                document.documentElement.classList.add('theme-ready');
            }}
            (function initSidebarCollapse() {{
                const appRoot = document.querySelector('.app');
                const toggleButton = document.getElementById('sidebarCollapseBtn');
                if (!appRoot || !toggleButton) return;
                const storageKey = window.teambeadStorageKey('sidebar-collapsed');
                function applyState(collapsed) {{
                    appRoot.classList.toggle('sidebar-collapsed', !!collapsed);
                    toggleButton.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
                    toggleButton.setAttribute('title', collapsed ? 'Expand sidebar' : 'Collapse sidebar');
                }}
                let collapsed = false;
                try {{
                    collapsed = localStorage.getItem(storageKey) === '1';
                }} catch (error) {{}}
                applyState(collapsed);
                toggleButton.addEventListener('click', function() {{
                    collapsed = !appRoot.classList.contains('sidebar-collapsed');
                    applyState(collapsed);
                    try {{
                        localStorage.setItem(storageKey, collapsed ? '1' : '0');
                    }} catch (error) {{}}
                    window.dispatchEvent(new Event('resize'));
                }});
            }})();
            (function initTheme() {{
                const saved = localStorage.getItem(window.teambeadStorageKey('theme'));
                if (saved === 'light') {{
                    document.documentElement.classList.add('light');
                    document.body.classList.add('light');
                }} else {{
                    document.documentElement.classList.remove('light');
                    document.body.classList.remove('light');
                }}
                document.documentElement.classList.add('theme-ready');
            }})();
            (function initSidebarDateTime() {{
                const target = document.getElementById('sidebarDateTime');
                if (!target) return;
                function render() {{
                    try {{
                        const now = new Date();
                        const parts = new Intl.DateTimeFormat('uk-UA', {{
                            timeZone: 'Europe/Kiev',
                            day: '2-digit',
                            month: '2-digit',
                            year: 'numeric',
                            hour: '2-digit',
                            minute: '2-digit',
                            second: '2-digit',
                            hour12: false,
                        }}).formatToParts(now);
                        const data = Object.fromEntries(parts.filter(function(part) {{
                            return part.type !== 'literal';
                        }}).map(function(part) {{
                            return [part.type, part.value];
                        }}));
                        target.textContent = data.day + '.' + data.month + '.' + data.year + ' · ' + data.hour + ':' + data.minute + ':' + data.second + ' Kyiv GMT+2';
                    }} catch (error) {{}}
                }}
                render();
                setInterval(render, 1000);
            }})();
            (function initPersistentFilters() {{
                function getFormKey(form) {{
                    const scope = form.getAttribute('data-persist-filters');
                    if (!scope) return '';
                    return window.teambeadStorageKey('filters:' + scope);
                }}
                function getTrackedFields(form) {{
                    return Array.from(form.elements || []).filter(function(el) {{
                        return el.name && !el.disabled && !['submit', 'button', 'file'].includes((el.type || '').toLowerCase());
                    }});
                }}
                function saveFormState(form) {{
                    const key = getFormKey(form);
                    if (!key) return;
                    const payload = {{}};
                    getTrackedFields(form).forEach(function(el) {{
                        if ((el.type || '').toLowerCase() === 'checkbox') payload[el.name] = !!el.checked;
                        else if ((el.type || '').toLowerCase() === 'radio') {{
                            if (el.checked) payload[el.name] = el.value;
                        }} else {{
                            payload[el.name] = el.value ?? '';
                        }}
                    }});
                    localStorage.setItem(key, JSON.stringify(payload));
                }}
                function restoreFormState(form) {{
                    const key = getFormKey(form);
                    if (!key) return;
                    const raw = localStorage.getItem(key);
                    if (!raw) return;
                    let payload = null;
                    try {{
                        payload = JSON.parse(raw);
                    }} catch (err) {{
                        return;
                    }}
                    const tracked = getTrackedFields(form);
                    const params = new URLSearchParams(window.location.search);
                    const hasExplicitQuery = tracked.some(function(el) {{
                        return params.has(el.name);
                    }});
                    if (hasExplicitQuery) return;
                    tracked.forEach(function(el) {{
                        if (!(el.name in payload)) return;
                        const value = payload[el.name];
                        const type = (el.type || '').toLowerCase();
                        if (type === 'checkbox') el.checked = !!value;
                        else if (type === 'radio') el.checked = el.value === value;
                        else el.value = value ?? '';
                    }});
                }}
                document.querySelectorAll('form[data-persist-filters]').forEach(function(form) {{
                    restoreFormState(form);
                    getTrackedFields(form).forEach(function(el) {{
                        const eventName = ['select-one', 'select-multiple', 'checkbox', 'radio'].includes((el.type || '').toLowerCase()) ? 'change' : 'input';
                        el.addEventListener(eventName, function() {{
                            saveFormState(form);
                        }});
                        if (eventName !== 'change') {{
                            el.addEventListener('change', function() {{
                                saveFormState(form);
                            }});
                        }}
                    }});
                    form.addEventListener('submit', function() {{
                        saveFormState(form);
                    }});
                }});
                document.querySelectorAll('[data-reset-filters]').forEach(function(link) {{
                    link.addEventListener('click', function() {{
                        const scope = link.getAttribute('data-reset-filters');
                        if (!scope) return;
                        localStorage.removeItem(window.teambeadStorageKey('filters:' + scope));
                    }});
                }});
            }})();
            (function initUnifiedFilterUi() {{
                function createPeriodJumpButton(step, label) {{
                    const button = document.createElement('button');
                    button.type = 'button';
                    button.className = 'ghost-btn small-btn period-jump-btn';
                    button.setAttribute('data-period-jump', String(step));
                    button.setAttribute('aria-label', label);
                    button.textContent = step < 0 ? '‹' : '›';
                    return button;
                }}

                function wirePeriodPicker(form, picker, periodSelect) {{
                    const viewSelect = form.querySelector('select[name=\"period_view\"]');
                    const viewInput = form.querySelector('input[name=\"period_view\"]');
                    const buttons = picker.querySelectorAll('[data-period-jump]');

                    function setPeriodMode() {{
                        if (viewSelect) {{
                            const hasPeriod = Array.from(viewSelect.options || []).some(function(option) {{
                                return option.value === 'period';
                            }});
                            if (hasPeriod) viewSelect.value = 'period';
                        }}
                        if (viewInput) viewInput.value = 'period';
                    }}

                    function updateButtons() {{
                        const options = Array.from(periodSelect.options || []);
                        const currentIndex = options.findIndex(function(option) {{
                            return option.value === periodSelect.value;
                        }});
                        buttons.forEach(function(button) {{
                            const step = Number(button.getAttribute('data-period-jump') || '0');
                            const nextIndex = currentIndex + step;
                            button.disabled = currentIndex < 0 || nextIndex < 0 || nextIndex >= options.length;
                            button.style.opacity = button.disabled ? '0.45' : '1';
                        }});
                    }}

                    buttons.forEach(function(button) {{
                        button.addEventListener('click', function() {{
                            const options = Array.from(periodSelect.options || []);
                            const currentIndex = options.findIndex(function(option) {{
                                return option.value === periodSelect.value;
                            }});
                            const step = Number(button.getAttribute('data-period-jump') || '0');
                            const nextIndex = currentIndex + step;
                            if (currentIndex < 0 || nextIndex < 0 || nextIndex >= options.length) return;
                            periodSelect.value = options[nextIndex].value;
                            setPeriodMode();
                            form.requestSubmit();
                        }});
                    }});

                    periodSelect.addEventListener('change', updateButtons);
                    if (viewSelect) viewSelect.addEventListener('change', updateButtons);
                    updateButtons();
                }}

                document.querySelectorAll('.panel.compact-panel.filters form').forEach(function(form) {{
                    const resetLink = form.querySelector('a[data-reset-filters]');
                    const submitButton = form.querySelector('button[type=\"submit\"]');
                    if (resetLink) {{
                        resetLink.classList.add('filter-reset-btn');
                        resetLink.textContent = '×';
                        resetLink.setAttribute('aria-label', 'Reset filters');
                        resetLink.setAttribute('title', 'Reset filters');
                    }}
                    if (submitButton && resetLink && !submitButton.closest('.filter-actions-stack') && !submitButton.closest('.filter-actions-inline')) {{
                        const actions = document.createElement('div');
                        actions.className = 'filter-actions-inline';
                        const resetParent = resetLink.parentNode;
                        const submitParent = submitButton.parentNode;
                        if (submitParent) submitParent.insertBefore(actions, submitButton);
                        actions.appendChild(submitButton);
                        actions.appendChild(resetLink);
                    }}

                    const periodSelect = form.querySelector('select[name=\"period_label\"]');
                    if (!periodSelect || periodSelect.closest('.period-picker')) return;
                    const label = periodSelect.closest('label');
                    if (!label) return;
                    const picker = document.createElement('div');
                    picker.className = 'period-picker';
                    const prevButton = createPeriodJumpButton(-1, 'Previous period');
                    const nextButton = createPeriodJumpButton(1, 'Next period');
                    label.parentNode.insertBefore(picker, label);
                    picker.appendChild(prevButton);
                    picker.appendChild(label);
                    picker.appendChild(nextButton);
                    wirePeriodPicker(form, picker, periodSelect);
                }});
            }})();
            (function initGenericTableEnhancements() {{
                const skipLayoutIds = new Set(['capsTable', 'groupedTable', 'chatterfyTable']);
                const skipStickyIds = new Set(['capsTable']);

                function slugify(text, fallback) {{
                    const value = (text || '').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
                    return value || fallback;
                }}

                function ensureHeaderMarkup(th) {{
                    if (th.querySelector('.th-inner')) return;
                    const wrapper = document.createElement('div');
                    wrapper.className = 'th-inner';
                    const drag = document.createElement('span');
                    drag.className = 'drag-handle';
                    drag.textContent = '⋮⋮';
                    wrapper.appendChild(drag);
                    while (th.firstChild) {{
                        wrapper.appendChild(th.firstChild);
                    }}
                    th.appendChild(wrapper);
                    if (!th.querySelector('.resizer')) {{
                        const resizer = document.createElement('span');
                        resizer.className = 'resizer';
                        th.appendChild(resizer);
                    }}
                }}

                function annotateTable(table, tableIndex) {{
                    if (skipLayoutIds.has(table.id || '')) return null;
                    const headerRow = table.querySelector('thead tr');
                    if (!headerRow) return null;
                    const headers = Array.from(headerRow.children || []);
                    if (!headers.length) return null;
                    const used = new Set();
                    headers.forEach(function(th, index) {{
                        let key = th.dataset.col || slugify(th.textContent, 'col_' + index);
                        while (used.has(key)) key += '_' + index;
                        used.add(key);
                        th.dataset.col = key;
                        th.setAttribute('draggable', 'true');
                        ensureHeaderMarkup(th);
                    }});
                    table.querySelectorAll('tbody tr').forEach(function(row) {{
                        const cells = Array.from(row.children || []);
                        if (cells.length !== headers.length) return;
                        cells.forEach(function(cell, index) {{
                            cell.dataset.col = headers[index].dataset.col;
                        }});
                    }});
                    return {{
                        keyBase: window.teambeadStorageKey('table:' + window.location.pathname + ':' + (table.id || ('generic_' + tableIndex))),
                        headers: headers,
                    }};
                }}

                function initLayout(table, meta) {{
                    const ORDER_KEY = meta.keyBase + ':order';
                    const WIDTH_KEY = meta.keyBase + ':widths';

                    function getCurrentOrder() {{
                        return Array.from(table.querySelectorAll('thead th[data-col]')).map(function(th) {{ return th.dataset.col; }});
                    }}

                    function reorderCells(order) {{
                        table.querySelectorAll('tr').forEach(function(row) {{
                            const children = Array.from(row.children || []);
                            if (!children.length) return;
                            const cells = {{}};
                            children.forEach(function(cell) {{
                                if (cell.dataset.col) cells[cell.dataset.col] = cell;
                            }});
                            if (!Object.keys(cells).length) return;
                            order.forEach(function(key) {{
                                if (cells[key]) row.appendChild(cells[key]);
                            }});
                        }});
                    }}

                    function applyOrder() {{
                        let saved = [];
                        try {{
                            saved = JSON.parse(localStorage.getItem(ORDER_KEY) || '[]');
                        }} catch (error) {{
                            saved = [];
                        }}
                        const current = getCurrentOrder();
                        if (!saved.length) return;
                        const merged = saved.filter(function(item) {{ return current.includes(item); }}).concat(current.filter(function(item) {{ return !saved.includes(item); }}));
                        reorderCells(merged);
                    }}

                    function applyWidths() {{
                        let widths = {{}};
                        try {{
                            widths = JSON.parse(localStorage.getItem(WIDTH_KEY) || '{{}}');
                        }} catch (error) {{
                            widths = {{}};
                        }}
                        Object.entries(widths).forEach(function(entry) {{
                            const key = entry[0];
                            const width = entry[1];
                            table.querySelectorAll('[data-col=\"' + key + '\"]').forEach(function(el) {{
                                el.style.width = width + 'px';
                                el.style.minWidth = width + 'px';
                            }});
                        }});
                    }}

                    function saveWidth(key, width) {{
                        let widths = {{}};
                        try {{
                            widths = JSON.parse(localStorage.getItem(WIDTH_KEY) || '{{}}');
                        }} catch (error) {{
                            widths = {{}};
                        }}
                        widths[key] = Math.max(80, Math.round(width));
                        localStorage.setItem(WIDTH_KEY, JSON.stringify(widths));
                    }}

                    applyOrder();
                    applyWidths();

                    table.querySelectorAll('th[data-col]').forEach(function(th) {{
                        const resizer = th.querySelector('.resizer');
                        if (!resizer) return;
                        let startX = 0;
                        let startWidth = 0;
                        let resizing = false;
                        const key = th.dataset.col;
                        resizer.addEventListener('mousedown', function(e) {{
                            e.preventDefault();
                            e.stopPropagation();
                            resizing = true;
                            startX = e.clientX;
                            startWidth = th.getBoundingClientRect().width;
                            document.body.style.cursor = 'col-resize';
                        }});
                        document.addEventListener('mousemove', function(e) {{
                            if (!resizing) return;
                            const newWidth = Math.max(80, startWidth + (e.clientX - startX));
                            table.querySelectorAll('[data-col=\"' + key + '\"]').forEach(function(el) {{
                                el.style.width = newWidth + 'px';
                                el.style.minWidth = newWidth + 'px';
                            }});
                        }});
                        document.addEventListener('mouseup', function() {{
                            if (!resizing) return;
                            resizing = false;
                            document.body.style.cursor = '';
                            saveWidth(key, th.getBoundingClientRect().width);
                        }});
                    }});

                    let dragged = null;
                    table.querySelectorAll('th[data-col]').forEach(function(th) {{
                        th.addEventListener('dragstart', function(e) {{
                            if (e.target.classList.contains('resizer')) {{
                                e.preventDefault();
                                return;
                            }}
                            dragged = th;
                            th.classList.add('dragging');
                        }});
                        th.addEventListener('dragend', function() {{
                            table.querySelectorAll('th[data-col]').forEach(function(item) {{
                                item.classList.remove('dragging', 'drag-target-left', 'drag-target-right');
                            }});
                            dragged = null;
                        }});
                        th.addEventListener('dragover', function(e) {{
                            e.preventDefault();
                            if (!dragged || dragged === th) return;
                            const rect = th.getBoundingClientRect();
                            const before = (e.clientX - rect.left) < rect.width / 2;
                            th.classList.toggle('drag-target-left', before);
                            th.classList.toggle('drag-target-right', !before);
                        }});
                        th.addEventListener('dragleave', function() {{
                            th.classList.remove('drag-target-left', 'drag-target-right');
                        }});
                        th.addEventListener('drop', function(e) {{
                            e.preventDefault();
                            if (!dragged || dragged === th) return;
                            const rect = th.getBoundingClientRect();
                            const before = (e.clientX - rect.left) < rect.width / 2;
                            if (before) th.parentNode.insertBefore(dragged, th);
                            else th.parentNode.insertBefore(dragged, th.nextSibling);
                            const order = getCurrentOrder();
                            reorderCells(order);
                            localStorage.setItem(ORDER_KEY, JSON.stringify(order));
                            table.querySelectorAll('th[data-col]').forEach(function(item) {{
                                item.classList.remove('drag-target-left', 'drag-target-right');
                            }});
                        }});
                    }});
                }}

                function initStickyHeader(table) {{
                    if (skipStickyIds.has(table.id || '')) return;
                    const wrap = table.closest('.table-wrap');
                    const thead = table.querySelector('thead');
                    if (!wrap || !thead) return;
                    const stickyHost = document.createElement('div');
                    stickyHost.className = 'table-sticky-header';
                    wrap.parentNode.insertBefore(stickyHost, wrap);
                    let stickyTable = null;
                    let stickyCells = [];

                    function build() {{
                        stickyHost.innerHTML = '';
                        const shell = document.createElement('div');
                        shell.className = 'table-wrap';
                        stickyTable = document.createElement('table');
                        stickyTable.className = table.className;
                        const clonedHead = thead.cloneNode(true);
                        stickyTable.appendChild(clonedHead);
                        shell.appendChild(stickyTable);
                        stickyHost.appendChild(shell);
                        stickyCells = Array.from(clonedHead.querySelectorAll('th'));
                        stickyCells.forEach(function(cell) {{
                            cell.removeAttribute('draggable');
                            const resizer = cell.querySelector('.resizer');
                            if (resizer) resizer.remove();
                        }});
                    }}

                    function sync() {{
                        if (!stickyTable || stickyCells.length !== thead.querySelectorAll('th').length) build();
                        const sourceCells = Array.from(thead.querySelectorAll('th'));
                        const wrapRect = wrap.getBoundingClientRect();
                        stickyHost.style.left = wrapRect.left + 'px';
                        stickyHost.style.width = wrapRect.width + 'px';
                        stickyTable.style.width = table.getBoundingClientRect().width + 'px';
                        stickyTable.style.transform = 'translateX(' + (-wrap.scrollLeft) + 'px)';
                        sourceCells.forEach(function(cell, index) {{
                            const width = Math.round(cell.getBoundingClientRect().width);
                            const clone = stickyCells[index];
                            if (!clone) return;
                            clone.style.width = width + 'px';
                            clone.style.minWidth = width + 'px';
                            clone.style.maxWidth = width + 'px';
                        }});
                    }}

                    function update() {{
                        const tableRect = table.getBoundingClientRect();
                        const headRect = thead.getBoundingClientRect();
                        const shouldShow = headRect.top < 10 && tableRect.bottom > 90;
                        stickyHost.classList.toggle('open', shouldShow);
                        if (shouldShow) sync();
                    }}

                    build();
                    sync();
                    update();
                    wrap.addEventListener('scroll', function() {{
                        if (stickyHost.classList.contains('open')) sync();
                    }}, {{ passive: true }});
                    window.addEventListener('scroll', update, {{ passive: true }});
                    window.addEventListener('resize', function() {{
                        sync();
                        update();
                    }});
                    if (window.ResizeObserver) {{
                        const observer = new ResizeObserver(function() {{
                            sync();
                            update();
                        }});
                        observer.observe(table);
                        observer.observe(wrap);
                    }}
                }}

                document.querySelectorAll('.table-wrap table').forEach(function(table, index) {{
                    const meta = annotateTable(table, index);
                    if (meta) initLayout(table, meta);
                    initStickyHeader(table);
                }});
            }})();
            document.addEventListener('click', function(e) {{
                const wrap = document.querySelector('.column-menu-wrap');
                const menu = document.getElementById('columnMenu');
                if (wrap && menu && !wrap.contains(e.target)) {{
                    menu.classList.remove('open');
                }}
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
        ("Payout", format_money(totals.get("stat_income", 0))),
        ("Profit", format_money(totals.get("stat_profit", 0))),
        ("ROI", format_percent(totals.get("stat_roi", 0))),
        ("FB FTD", format_int_or_float(totals["ftd"])),
        ("Total FTD", format_int_or_float(totals.get("stat_total_ftd", 0))),
        ("Qual FTD", format_int_or_float(totals.get("stat_qual_ftd", 0))),
        ("Chatterfy", format_int_or_float(totals.get("stat_chatterfy", 0))),
        ("CPA", format_money(totals.get("cpa_real", 0))),
        ("Payout / Qual FTD", format_money(totals.get("stat_rate", 0))),
        ("Cap Fill", format_percent(totals.get("stat_cap_fill", 0))),
    ]
    html = '<div class="panel compact-panel"><div class="stats-grid">'
    for title, value in cards:
        html += f'<div class="stat-card"><div class="name">{title}</div><div class="value">{value}</div></div>'
    html += '</div></div>'
    return html


def render_analytics_spotlight(rows):
    geo_rows = aggregate_stat_rows_by_keys(rows, ["geo"])
    buyer_rows = aggregate_stat_rows_by_keys(rows, ["buyer"])
    manager_rows = aggregate_stat_rows_by_keys(rows, ["manager"])
    offer_rows = aggregate_stat_rows_by_keys(rows, ["offer"])

    top_geo = max(geo_rows, key=lambda item: item.get("spend", 0), default={})
    top_buyer = max(buyer_rows, key=lambda item: item.get("spend", 0), default={})
    top_manager = max(manager_rows, key=lambda item: item.get("stat_income", 0), default={})
    top_offer = max(offer_rows, key=lambda item: item.get("stat_income", 0), default={})

    roi_candidates = [item for item in geo_rows if safe_number(item.get("spend", 0)) > 0]
    best_geo_roi = max(roi_candidates, key=lambda item: item.get("stat_roi", 0), default={})

    cards = [
        ("Top Geo", safe_text(top_geo.get("geo")) or "—"),
        ("Top Buyer", safe_text(top_buyer.get("buyer")) or "—"),
        ("Top Manager", safe_text(top_manager.get("manager")) or "—"),
        ("Top Offer", safe_text(top_offer.get("offer")) or "—"),
        ("Best Geo ROI", f"{safe_text(best_geo_roi.get('geo')) or '—'} · {format_percent(best_geo_roi.get('stat_roi', 0))}"),
        ("Top Geo Payout", format_money(top_geo.get("stat_income", 0))),
        ("Top Buyer Spend", format_money(top_buyer.get("spend", 0))),
        ("Top Manager Profit", format_money(top_manager.get("stat_profit", 0))),
    ]
    html = '<div class="panel compact-panel"><div class="panel-title" style="margin-bottom:12px;">Analytics Snapshot</div><div class="stats-grid">'
    for title, value in cards:
        html += f'<div class="stat-card"><div class="name">{escape(title)}</div><div class="value">{escape(str(value))}</div></div>'
    html += '</div></div>'
    return html


def split_geo_tokens(value):
    tokens = re.split(r"[,/;|]+", safe_text(value))
    return [normalize_geo_value(token) for token in tokens if normalize_geo_value(token)]


def cabinet_matches_filters(row, manager="", geo="", search=""):
    if manager and safe_text(getattr(row, "manager_name", "")).strip() != safe_text(manager).strip():
        return False
    if geo:
        target_geo = normalize_geo_value(geo)
        cabinet_geos = split_geo_tokens(getattr(row, "geo_list", ""))
        if target_geo and target_geo not in cabinet_geos:
            return False
    if search:
        search_lower = safe_text(search).strip().lower()
        haystack = " | ".join([
            getattr(row, "advertiser", "") or "",
            getattr(row, "platform", "") or "",
            getattr(row, "name", "") or "",
            getattr(row, "geo_list", "") or "",
            getattr(row, "brands", "") or "",
            getattr(row, "team_name", "") or "",
            getattr(row, "manager_name", "") or "",
            getattr(row, "comments", "") or "",
        ]).lower()
        if search_lower not in haystack:
            return False
    return True


def partner_matches_filters(row, geo="", search=""):
    if geo and normalize_geo_value(getattr(row, "country", "")) != normalize_geo_value(geo):
        return False
    if search:
        search_lower = safe_text(search).strip().lower()
        haystack = " | ".join([
            getattr(row, "source_name", "") or "",
            getattr(row, "cabinet_name", "") or "",
            getattr(row, "sub_id", "") or "",
            getattr(row, "player_id", "") or "",
            getattr(row, "country", "") or "",
            getattr(row, "registration_date", "") or "",
        ]).lower()
        if search_lower not in haystack:
            return False
    return True


def chatterfy_matches_filters(row, manager="", geo="", offer="", search=""):
    if manager and safe_text(getattr(row, "manager", "")).strip() != safe_text(manager).strip():
        return False
    if geo and normalize_geo_value(getattr(row, "geo", "")) != normalize_geo_value(geo):
        return False
    if offer and safe_text(getattr(row, "offer", "")).strip() != safe_text(offer).strip():
        return False
    if search:
        search_lower = safe_text(search).strip().lower()
        haystack = " | ".join([
            getattr(row, "source_name", "") or "",
            getattr(row, "name", "") or "",
            getattr(row, "telegram_id", "") or "",
            getattr(row, "username", "") or "",
            getattr(row, "tags", "") or "",
            getattr(row, "status", "") or "",
            getattr(row, "platform", "") or "",
            getattr(row, "manager", "") or "",
            getattr(row, "geo", "") or "",
            getattr(row, "offer", "") or "",
        ]).lower()
        if search_lower not in haystack:
            return False
    return True


def build_dashboard_overview(user, rows, buyer="", manager="", geo="", offer="", search="", period_label=""):
    ensure_partner_table()
    ensure_cabinet_table()
    ensure_chatterfy_table()
    ensure_caps_table()
    db = SessionLocal()
    try:
        partner_query = db.query(PartnerRow)
        chatterfy_query = db.query(ChatterfyRow)
        caps_query = db.query(CapRow)
        cabinets_query = db.query(CabinetRow)

        if period_label:
            partner_query = partner_query.filter(PartnerRow.period_label == period_label)
            chatterfy_query = chatterfy_query.filter(ChatterfyRow.period_label == period_label)
        if period_label:
            caps_query = caps_query.filter(CapRow.period_label == period_label)
        if buyer:
            caps_query = caps_query.filter(CapRow.buyer == buyer)
        if manager:
            caps_query = caps_query.filter(CapRow.owner_name == manager)
        if geo:
            caps_query = caps_query.filter(CapRow.geo == normalize_geo_value(geo))

        partner_rows = [row for row in partner_query.all() if partner_matches_filters(row, geo=geo, search=search)]
        chatterfy_rows = [row for row in chatterfy_query.all() if chatterfy_matches_filters(row, manager=manager, geo=geo, offer=offer, search=search)]
        caps_rows = caps_query.order_by(CapRow.id.desc()).all()
        cabinets_rows = [row for row in cabinets_query.all() if cabinet_matches_filters(row, manager=manager, geo=geo, search=search)]
    finally:
        db.close()

    caps_rows = [row for row in caps_rows if not search or safe_text(search).strip().lower() in " | ".join([
        row.advertiser or "",
        row.owner_name or "",
        row.buyer or "",
        row.flow or "",
        row.code or "",
        row.geo or "",
        row.promo_code or "",
        row.comments or "",
    ]).lower()]

    partner_totals = aggregate_partner_totals(partner_rows)
    flow_rows = aggregate_stat_rows_by_keys(rows, ["platform", "manager", "geo"])
    total_flows = len(flow_rows)
    flows_with_caps = sum(1 for item in flow_rows if safe_number(item.get("stat_has_cap", 0)) > 0 or safe_number(item.get("stat_cap_limit", 0)) > 0)
    flows_with_players = sum(1 for item in flow_rows if safe_number(item.get("stat_total_ftd", 0)) > 0 or safe_number(item.get("stat_income", 0)) > 0)
    flows_with_chatterfy = sum(1 for item in flow_rows if safe_number(item.get("stat_chatterfy", 0)) > 0)
    fully_linked_flows = sum(
        1 for item in flow_rows
        if (safe_number(item.get("stat_has_cap", 0)) > 0 or safe_number(item.get("stat_cap_limit", 0)) > 0)
        and safe_number(item.get("stat_total_ftd", 0)) > 0
        and safe_number(item.get("stat_chatterfy", 0)) > 0
    )
    linked_campaigns = sum(
        1 for item in rows
        if safe_number(item.get("stat_total_ftd", 0)) > 0
        or safe_number(item.get("stat_chatterfy", 0)) > 0
        or safe_number(item.get("stat_has_cap", 0)) > 0
    )

    overview = {
        "cards": [
            ("FB Campaigns", format_int_or_float(len(rows))),
            ("Linked Campaigns", format_int_or_float(linked_campaigns)),
            ("Linked Flows", f"{fully_linked_flows}/{total_flows}" if total_flows else "0/0"),
            ("Players", format_int_or_float(partner_totals["players"])),
            ("Chatterfy", format_int_or_float(len(chatterfy_rows))),
            ("Active Cabinets", format_int_or_float(sum(1 for row in cabinets_rows if safe_text(row.status).lower() == "active"))),
            ("Active Caps", format_int_or_float(sum(1 for row in caps_rows if safe_number(row.cap_value) > 0))),
        ],
        "rows": [
            {
                "source": "FB",
                "tracked": len(rows),
                "coverage": f"{linked_campaigns}/{len(rows)}" if rows else "0/0",
                "primary_value": format_money(sum(safe_number(item.get('spend', 0)) for item in rows)),
                "secondary_value": format_int_or_float(sum(safe_number(item.get('ftd', 0)) for item in rows)),
                "notes": "Campaign rows inside current dashboard filters.",
            },
            {
                "source": "Players",
                "tracked": partner_totals["players"],
                "coverage": f"{flows_with_players}/{total_flows}" if total_flows else "0/0",
                "primary_value": format_money(partner_totals["income"]),
                "secondary_value": format_int_or_float(partner_totals["ftd_count"]),
                "notes": "Players and FTD from the selected partner period.",
            },
            {
                "source": "Chatterfy",
                "tracked": len(chatterfy_rows),
                "coverage": f"{flows_with_chatterfy}/{total_flows}" if total_flows else "0/0",
                "primary_value": format_int_or_float(len([row for row in chatterfy_rows if safe_text(getattr(row, 'status', ''))])),
                "secondary_value": format_int_or_float(len([row for row in chatterfy_rows if safe_text(getattr(row, 'external_id', '')) or safe_text(getattr(row, 'telegram_id', ''))])),
                "notes": "Dialog rows mapped by manager, geo, offer and period.",
            },
            {
                "source": "Caps",
                "tracked": len(caps_rows),
                "coverage": f"{flows_with_caps}/{total_flows}" if total_flows else "0/0",
                "primary_value": format_int_or_float(sum(safe_number(getattr(row, 'cap_value', 0)) for row in caps_rows)),
                "secondary_value": format_int_or_float(sum(safe_number(getattr(row, 'current_ftd', 0)) for row in caps_rows)),
                "notes": "Cap volume and current FTD for the scoped buyer / manager / geo.",
            },
            {
                "source": "Cabinets",
                "tracked": len(cabinets_rows),
                "coverage": format_int_or_float(sum(1 for row in cabinets_rows if safe_text(row.status).lower() == "active")),
                "primary_value": format_int_or_float(len({safe_text(row.manager_name) for row in cabinets_rows if safe_text(row.manager_name)})),
                "secondary_value": format_int_or_float(len({geo_code for row in cabinets_rows for geo_code in split_geo_tokens(getattr(row, 'geo_list', ''))})),
                "notes": "Active partner cabinets matching the selected manager and geo.",
            },
        ],
    }

    if (user or {}).get("role") == "superadmin":
        ensure_finance_tables()
        snapshot = load_finance_snapshot()
        manual = load_manual_finance()
        balances = compute_finance_balances(snapshot, manual)
        overview["cards"].append(("Balance", format_money(balances["total"])))
        overview["rows"].append({
            "source": "Finance",
            "tracked": len(balances["rows"]),
            "coverage": format_money(snapshot.get("totals", {}).get("pending", 0)),
            "primary_value": format_money(balances["total"]),
            "secondary_value": format_money(snapshot.get("totals", {}).get("income", 0) + sum(safe_number(item.amount) for item in manual.get("income", []))),
            "notes": "Current wallet balance, pending amount and total income snapshot.",
        })

    return overview


def render_dashboard_overview(overview):
    cards_html = '<div class="panel compact-panel"><div class="stats-grid">'
    for title, value in overview.get("cards", []):
        cards_html += f'<div class="stat-card"><div class="name">{escape(title)}</div><div class="value">{escape(str(value))}</div></div>'
    cards_html += '</div></div>'

    table_columns = [
        {"key": "source", "label": "Source"},
        {"key": "tracked", "label": "Tracked", "align": "right", "formatter": format_int_or_float},
        {"key": "coverage", "label": "Coverage", "align": "right"},
        {"key": "primary_value", "label": "Primary Metric", "align": "right"},
        {"key": "secondary_value", "label": "Secondary Metric", "align": "right"},
        {"key": "notes", "label": "What It Means", "wrap": True},
    ]
    return cards_html + render_stat_table(
        "CRM Overview",
        "Single place to see whether all CRM layers are actually feeding the dashboard.",
        overview.get("rows", []),
        table_columns,
        empty_text="No CRM overview data yet",
    )


def render_active_period_banner(period_label=""):
    active_period = safe_text(period_label) or get_current_period_label()
    return f'<div class="user-chip" style="margin-bottom:12px;">Active Period: {escape(active_period)}</div>'


def aggregate_stat_rows_by_keys(rows, keys):
    buckets = {}
    for row in rows:
        identity = tuple((safe_text(row.get(key)) or "—") for key in keys)
        if identity not in buckets:
            buckets[identity] = {
                "rows": [],
                "campaigns": set(),
                "offers": set(),
                "creatives": set(),
            }
        bucket = buckets[identity]
        bucket["rows"].append(row)
        if safe_text(row.get("ad_name")):
            bucket["campaigns"].add(safe_text(row.get("ad_name")))
        if safe_text(row.get("offer")):
            bucket["offers"].add(safe_text(row.get("offer")))
        if safe_text(row.get("creative")):
            bucket["creatives"].add(safe_text(row.get("creative")))

    result = []
    for identity, bucket in buckets.items():
        metrics = aggregate_totals(bucket["rows"])
        item = {key: identity[index] for index, key in enumerate(keys)}
        item.update(metrics)
        item["campaign_count"] = len(bucket["campaigns"])
        item["offer_count"] = len(bucket["offers"])
        item["creative_count"] = len(bucket["creatives"])
        result.append(item)
    return result


def render_stat_table(title, subtitle, rows, columns, empty_text="No data"):
    head_html = "".join([f"<th>{escape(col['label'])}</th>" for col in columns])
    body_html = ""
    for row in rows:
        cell_html = ""
        for col in columns:
            raw_value = row.get(col["key"], "")
            formatted = col.get("formatter", lambda value: value)(raw_value)
            if not col.get("html"):
                formatted = escape(str(formatted))
            align_class = " stat-cell-right" if col.get("align") == "right" else ""
            allow_wrap = " stat-cell-wrap" if col.get("wrap") else ""
            cell_html += f'<td class="{align_class}{allow_wrap}">{formatted}</td>'
        body_html += f"<tr>{cell_html}</tr>"

    return f"""
    <div class="panel compact-panel">
        <div class="panel-title" style="margin-bottom:4px;">{escape(title)}</div>
        <div class="table-wrap" style="margin-top:14px;">
            <table style="min-width:1200px;">
                <thead><tr>{head_html}</tr></thead>
                <tbody>{body_html if body_html else f'<tr><td colspan="{len(columns)}">{escape(empty_text)}</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    """


def render_flow_badge(platform, manager, geo):
    return f'<div class="flow-badge"><span>{escape(platform or "—")}</span><span>{escape(manager or "—")}</span><span>{escape(geo or "—")}</span></div>'


def render_chatterfy_status_badge(status):
    status_text = safe_text(status)
    status_lower = status_text.lower()
    if not status_text:
        return '<span class="status-chip">—</span>'
    if "block" in status_lower:
        return f'<span class="status-chip status-chip-blocked">🛑 {escape(status_text)}</span>'
    if "wait" in status_lower:
        return f'<span class="status-chip status-chip-waiting">⏳ {escape(status_text)}</span>'
    if "manual" in status_lower:
        return f'<span class="status-chip status-chip-manual">! {escape(status_text)}</span>'
    return f'<span class="status-chip status-chip-active">• {escape(status_text)}</span>'


def render_chatterfy_status_icon(status):
    status_text = safe_text(status)
    status_lower = status_text.lower()
    if not status_text:
        return '<span class="status-icon" title="No status">—</span>'
    if "block" in status_lower:
        return f'<span class="status-icon status-icon-blocked" title="{escape(status_text)}">🛑</span>'
    if "wait" in status_lower:
        return f'<span class="status-icon status-icon-waiting" title="{escape(status_text)}">⏳</span>'
    if "manual" in status_lower:
        return f'<span class="status-icon status-icon-manual" title="{escape(status_text)}">👤</span>'
    return f'<span class="status-icon status-icon-active" title="{escape(status_text)}">•</span>'


def render_statistic_dashboard(rows):
    geo_rows = aggregate_stat_rows_by_keys(rows, ["geo"])
    geo_rows.sort(key=lambda item: item.get("spend", 0), reverse=True)

    buyer_rows = aggregate_stat_rows_by_keys(rows, ["buyer"])
    buyer_rows.sort(key=lambda item: item.get("spend", 0), reverse=True)

    manager_rows = aggregate_stat_rows_by_keys(rows, ["manager"])
    manager_rows.sort(key=lambda item: item.get("spend", 0), reverse=True)

    flow_rows = aggregate_stat_rows_by_keys(rows, ["platform", "manager", "geo"])
    flow_rows.sort(key=lambda item: item.get("spend", 0), reverse=True)

    campaign_rows = list(rows)
    campaign_rows.sort(key=lambda item: item.get("spend", 0), reverse=True)

    buyer_columns = [
        {"key": "buyer", "label": "Buyer"},
        {"key": "campaign_count", "label": "Campaigns", "align": "right", "formatter": format_int_or_float},
        {"key": "offer_count", "label": "Offers", "align": "right", "formatter": format_int_or_float},
        {"key": "spend", "label": "Spend", "align": "right", "formatter": format_money},
        {"key": "ftd", "label": "FB FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_total_ftd", "label": "Total FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_qual_ftd", "label": "Qual FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_income", "label": "Payout", "align": "right", "formatter": format_money},
        {"key": "stat_profit", "label": "Profit", "align": "right", "formatter": format_money},
        {"key": "stat_roi", "label": "ROI", "align": "right", "formatter": format_percent},
    ]
    manager_columns = [
        {"key": "manager", "label": "Manager"},
        {"key": "campaign_count", "label": "Campaigns", "align": "right", "formatter": format_int_or_float},
        {"key": "geo_count", "label": "Geos", "align": "right", "formatter": format_int_or_float},
        {"key": "spend", "label": "Spend", "align": "right", "formatter": format_money},
        {"key": "stat_chatterfy", "label": "Chatterfy", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_total_ftd", "label": "Total FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_qual_ftd", "label": "Qual FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_income", "label": "Payout", "align": "right", "formatter": format_money},
        {"key": "stat_profit", "label": "Profit", "align": "right", "formatter": format_money},
        {"key": "stat_roi", "label": "ROI", "align": "right", "formatter": format_percent},
    ]
    geo_columns = [
        {"key": "geo", "label": "Geo"},
        {"key": "campaign_count", "label": "Campaigns", "align": "right", "formatter": format_int_or_float},
        {"key": "offer_count", "label": "Offers", "align": "right", "formatter": format_int_or_float},
        {"key": "spend", "label": "Spend", "align": "right", "formatter": format_money},
        {"key": "ftd", "label": "FB FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_chatterfy", "label": "Chatterfy", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_total_ftd", "label": "Total FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_qual_ftd", "label": "Qual FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_income", "label": "Payout", "align": "right", "formatter": format_money},
        {"key": "stat_profit", "label": "Profit", "align": "right", "formatter": format_money},
        {"key": "stat_roi", "label": "ROI", "align": "right", "formatter": format_percent},
    ]
    flow_table_columns = [
        {"key": "flow_label", "label": "Flow", "wrap": True, "formatter": lambda value: value, "html": True},
        {"key": "offer_count", "label": "Offers", "align": "right", "formatter": format_int_or_float},
        {"key": "campaign_count", "label": "Campaigns", "align": "right", "formatter": format_int_or_float},
        {"key": "spend", "label": "Spend", "align": "right", "formatter": format_money},
        {"key": "stat_chatterfy", "label": "Chatterfy", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_total_ftd", "label": "Total FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_qual_ftd", "label": "Qual FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_income", "label": "Payout", "align": "right", "formatter": format_money},
        {"key": "stat_profit", "label": "Profit", "align": "right", "formatter": format_money},
        {"key": "stat_roi", "label": "ROI", "align": "right", "formatter": format_percent},
    ]
    for item in flow_rows:
        item["flow_label"] = render_flow_badge(item.get("platform"), item.get("manager"), item.get("geo"))
    for item in manager_rows:
        item["geo_count"] = len({safe_text(row.get("geo")) for row in rows if safe_text(row.get("manager")) == safe_text(item.get("manager")) and safe_text(row.get("geo"))})

    campaign_columns = [
        {"key": "launch_date", "label": "Start"},
        {"key": "buyer", "label": "Buyer"},
        {"key": "flow_label", "label": "Flow", "wrap": True, "formatter": lambda value: value, "html": True},
        {"key": "offer", "label": "Offer"},
        {"key": "creative", "label": "Creative"},
        {"key": "ad_name", "label": "Campaign", "wrap": True},
        {"key": "spend", "label": "Spend", "align": "right", "formatter": format_money},
        {"key": "ftd", "label": "FB FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_chatterfy", "label": "Chatterfy", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_total_ftd", "label": "Total FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_qual_ftd", "label": "Qual FTD", "align": "right", "formatter": format_int_or_float},
        {"key": "stat_income", "label": "Payout", "align": "right", "formatter": format_money},
        {"key": "stat_profit", "label": "Profit", "align": "right", "formatter": format_money},
        {"key": "stat_roi", "label": "ROI", "align": "right", "formatter": format_percent},
    ]
    for item in campaign_rows:
        item["flow_label"] = render_flow_badge(item.get("platform"), item.get("manager"), item.get("geo"))

    return (
        render_stat_table(
            "Buyer Overview",
            "Fast read by buyer: spend, payout, profit and ROI.",
            buyer_rows,
            buyer_columns,
            empty_text="No buyer data yet",
        )
        + render_stat_table(
            "Manager Overview",
            "See who is carrying spend, payout and profit across geos.",
            manager_rows,
            manager_columns,
            empty_text="No manager data yet",
        )
        + render_stat_table(
            "Geo Overview",
            "Quick read on where spend, payout and ROI are concentrated right now.",
            geo_rows,
            geo_columns,
            empty_text="No geo data yet",
        )
        + render_stat_table(
            "Flow Overview",
            "Platform + manager + geo combined into one readable operating view.",
            flow_rows,
            flow_table_columns,
            empty_text="No flow data yet",
        )
        + render_stat_table(
            "Campaign Performance",
            "Main working table for campaign analytics: spend, payout, profit, ROI and qualification.",
            campaign_rows,
            campaign_columns,
            empty_text="No campaign rows yet",
        )
    )



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

    submit_label = "Save Changes" if current_edit_id else "Create User"
    password_hint = "Leave blank to keep current password." if current_edit_id else "Password"
    message_html = ""
    if error_text:
        message_html += f'<div class="notice notice-danger">{escape(error_text)}</div>'
    if success_text:
        message_html += f'<div class="notice">{escape(success_text)}</div>'

    create_panel = f"""
    <details class="upload-menu upload-menu-right" {'open' if current_edit_id else ''}>
        <summary class="btn toggle-indicator"></summary>
        <div class="upload-menu-list" style="width:420px; max-width:min(420px, calc(100vw - 48px));">
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
                <span><strong>Active</strong><br><span class="muted">User can sign in.</span></span>
            </label>
            <div style="display:flex; gap:10px; flex-wrap:wrap;">
                <button type="submit" class="btn">{submit_label}</button>
                <a href="/users" class="ghost-btn">Reset</a>
            </div>
        </form>
        </div>
    </details>
    """

    content = f"""
    {message_html}
    <div class="panel compact-panel">
        <div class="toolbar-actions">
            {create_panel}
        </div>
    </div>

    <div class="panel">
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
    """
    return page_shell("Users", content, active_page="users", current_user=current_user)


@app.get("/fb", response_class=HTMLResponse)
def fb_page(request: Request):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "fb")

    content = """
    <div class="panel compact-panel">
        <div class="empty-dev" style="min-height:34vh;">
            <div class="empty-dev-card">
                <div class="big">FB</div>
                <div class="muted">This section is empty for now.</div>
            </div>
        </div>
    </div>
    """
    return page_shell("FB", content, active_page="fb", current_user=user)


def caps_page_html(current_user, rows, filter_values=None, form_data=None, success_text="", error_text=""):
    filter_values = filter_values or {}
    form_data = form_data or {}
    selected_period = safe_text(filter_values.get("period_label") or form_data.get("period_label") or get_current_period_label())
    selected_period_view = safe_text(filter_values.get("period_view") or "period")
    sort_by = safe_text(filter_values.get("sort_by") or "cabinet")
    order = safe_text(filter_values.get("order") or "asc")
    buyers, codes, _owners = get_caps_filter_options(period_label=selected_period)
    period_options = make_options(build_period_options(), selected_period)
    buyer_options = make_options(buyers, filter_values.get("buyer", ""))
    code_options = make_options(codes, filter_values.get("code", ""))

    total_cap = sum(safe_number(row.cap_value) for row in rows)
    total_current = sum(safe_number(row.current_ftd) for row in rows)
    total_remaining = sum(max(0.0, safe_number(row.cap_value) - safe_number(row.current_ftd)) for row in rows)
    fill_avg = cap_fill_percent(total_current, total_cap)
    caps_columns = [
        ("advertiser", "Brands"),
        ("manager", "Manager"),
        ("cabinet", "Cabinet"),
        ("code", "Code"),
        ("geo", "Geo"),
        ("rate", "Rate"),
        ("baseline", "Baseline"),
        ("cap", "Cap"),
        ("current_ftd", "FTD"),
        ("remaining", "Remaining"),
        ("fill", "Fill"),
        ("agent", "TG"),
        ("chat_title", "Chat Name"),
        ("kpi", "KPI"),
        ("comments", "Comments"),
        ("promo_code", "Promocode"),
        ("link", "Link"),
        ("action", "Action"),
    ]

    def header_link(field, label):
        return sort_link(
            label,
            field,
            sort_by,
            order,
            search=filter_values.get("search", ""),
            period_view=selected_period_view,
            period_label=selected_period,
            buyer=filter_values.get("buyer", ""),
            code=filter_values.get("code", ""),
        )

    rows_html = ""
    for row in rows:
        fill_percent = cap_fill_percent(row.current_ftd, row.cap_value)
        bar_width = max(0, min(100, fill_percent))
        remaining_value = max(0.0, safe_number(row.cap_value) - safe_number(row.current_ftd))
        link_value = safe_text(row.link)
        edit_payload = escape(json.dumps({
            "id": row.id,
            "period_label": safe_text(row.period_label or selected_period),
            "advertiser": safe_text(row.advertiser),
            "owner_name": safe_text(row.owner_name),
            "cabinet_name": safe_text(row.cabinet_name),
            "code": safe_text(normalize_geo_value(row.code or "")),
            "geo": safe_text(geo_display_name(row.code or row.geo or "")),
            "rate": safe_text(format_plain_number_text(row.rate)),
            "baseline": safe_text(format_plain_number_text(row.baseline)),
            "cap_value": safe_text(format_int_or_float(row.cap_value)),
            "promo_code": safe_text(row.promo_code),
            "agent": safe_text(row.agent),
            "chat_title": safe_text(row.chat_title),
            "link": safe_text(row.link),
            "kpi": safe_text(row.kpi),
            "comments": safe_text(row.comments),
        }))
        link_button = f'<button type="button" class="ghost-btn small-btn cap-copy-link" data-link="{escape(link_value)}" aria-label="Copy link" title="Copy link">⧉</button>' if link_value else "—"
        state = "Free"
        progress_class = "progress-free"
        if fill_percent >= 100:
            state = "Overflow"
            progress_class = "progress-overflow"
        elif safe_number(row.current_ftd) > 0:
            state = "Filling"
            progress_class = "progress-filling"
        rows_html += f"""
        <tr>
            <td class="advertiser-col" data-col="advertiser" title="{escape(row.advertiser or '')}">{escape(row.advertiser or "")}</td>
            <td class="owner-col" data-col="manager" title="{escape(row.owner_name or '')}">{escape(row.owner_name or "")}</td>
            <td class="buyer-col" data-col="cabinet" title="{escape(row.cabinet_name or '')}">{escape(row.cabinet_name or "")}</td>
            <td class="code-col" data-col="code" title="{escape(normalize_geo_value(row.code or ''))}">{escape(normalize_geo_value(row.code or ""))}</td>
            <td class="geo-col" data-col="geo" title="{escape(geo_display_name(row.code or row.geo or ''))}">{escape(geo_display_name(row.code or row.geo or ""))}</td>
            <td class="rate-col" data-col="rate" title="{escape(row.rate or '')}">{escape(format_plain_number_text(row.rate))}</td>
            <td class="baseline-col" data-col="baseline" title="{escape(row.baseline or '')}">{escape(format_plain_number_text(row.baseline))}</td>
            <td class="cap-col" data-col="cap">{format_int_or_float(row.cap_value)}</td>
            <td class="current-col" data-col="current_ftd">{format_int_or_float(row.current_ftd)}</td>
            <td class="remaining-col" data-col="remaining">{format_int_or_float(remaining_value)}</td>
            <td class="fill-col" data-col="fill">
                <div class="progress-shell {progress_class}">
                    <div class="fill-meta"><strong>{fill_percent:.0f}%</strong><span class="fill-status">{state}</span></div>
                    <div class="progress-bar"><span style="width:{bar_width}%;"></span></div>
                </div>
            </td>
            <td class="agent-col" data-col="agent" title="{escape(row.agent or '')}">{escape(row.agent or "")}</td>
            <td class="chat-col" data-col="chat_title" title="{escape(row.chat_title or '')}">{escape(row.chat_title or "")}</td>
            <td class="kpi-col" data-col="kpi" title="{escape(row.kpi or '')}">{escape(row.kpi or "")}</td>
            <td class="comment-col" data-col="comments" title="{escape(row.comments or '')}">{escape(row.comments or "")}</td>
            <td class="promo-col" data-col="promo_code" title="{escape(row.promo_code or '')}">{escape(row.promo_code or "")}</td>
            <td class="link-col" data-col="link">{link_button}</td>
            <td class="action-col" data-col="action">
                <div class="table-icon-actions">
                    <button type="button" class="ghost-btn small-btn action-icon-btn cap-edit-trigger" data-cap="{edit_payload}" aria-label="Edit" title="Edit">✏</button>
                    <form method="post" action="/caps/delete" class="cap-delete-form">
                        <input type="hidden" name="cap_id" value="{row.id}">
                        <input type="hidden" name="period_view" value="{escape(selected_period_view)}">
                        <input type="hidden" name="period_label" value="{escape(selected_period)}">
                        <input type="hidden" name="buyer" value="{escape(filter_values.get('buyer', ''))}">
                        <input type="hidden" name="code" value="{escape(filter_values.get('code', ''))}">
                        <input type="hidden" name="search" value="{escape(filter_values.get('search', ''))}">
                        <input type="hidden" name="sort_by" value="{escape(sort_by)}">
                        <input type="hidden" name="order" value="{escape(order)}">
                        <button type="button" class="ghost-btn small-btn action-icon-btn cap-delete-trigger" data-cap-id="{row.id}" aria-label="Delete" title="Delete">🗑</button>
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

    ensure_cabinet_table()
    ensure_partner_table()
    db = SessionLocal()
    try:
        cabinet_rows = db.query(CabinetRow).order_by(CabinetRow.name.asc(), CabinetRow.id.asc()).all()
        cap_rows = db.query(CapRow).order_by(CapRow.id.desc()).all()
    finally:
        db.close()

    def build_datalist(options):
        unique = []
        seen = set()
        for item in options:
            value = safe_text(item).strip()
            if not value:
                continue
            key = value.lower()
            if key in seen:
                continue
            seen.add(key)
            unique.append(value)
        return "".join(f'<option value="{escape(value)}"></option>' for value in unique)

    def build_select_options(options, selected_value, placeholder="Choose"):
        html = '<option value=""></option>'
        seen = set()
        for item in options:
            value = safe_text(item).strip()
            if not value:
                continue
            key = value.lower()
            if key in seen:
                continue
            seen.add(key)
            selected = "selected" if value == safe_text(selected_value).strip() else ""
            html += f'<option value="{escape(value)}" {selected}>{escape(value)}</option>'
        return html

    cabinet_catalog = []
    seen_catalog_rows = set()
    for row in cabinet_rows:
        cabinet_name_value = safe_text(row.name).strip()
        manager_value = safe_text(row.manager_name).strip()
        brand_values = split_list_tokens(getattr(row, "brands", ""))
        if not cabinet_name_value or not brand_values or not manager_value:
            continue
        for brand_value in brand_values:
            row_key = (cabinet_name_value.lower(), brand_value.lower(), manager_value.lower())
            if row_key in seen_catalog_rows:
                continue
            seen_catalog_rows.add(row_key)
            cabinet_catalog.append({
                "cabinet": cabinet_name_value,
                "advertiser": brand_value,
                "manager": manager_value,
            })
    advertiser_select_options = build_select_options(
        sorted({item["advertiser"] for item in cabinet_catalog}, key=lambda value: value.lower()),
        form_data.get("advertiser", ""),
        "Brands",
    )
    manager_select_options = build_select_options(
        sorted({item["manager"] for item in cabinet_catalog}, key=lambda value: value.lower()),
        form_data.get("owner_name", ""),
        "Manager",
    )
    cabinet_select_options = build_select_options(
        [item["cabinet"] for item in cabinet_catalog],
        form_data.get("cabinet_name", ""),
        "Cabinet",
    )
    cabinet_catalog_json = escape(json.dumps(cabinet_catalog))

    agent_list = build_datalist([row.agent for row in cap_rows])

    current_edit_id = str(form_data.get("edit_id") or "")
    is_edit_mode = bool(current_edit_id)
    add_form_data = {} if is_edit_mode else form_data
    edit_form_data = form_data if is_edit_mode else {}
    add_advertiser_options = build_select_options(
        sorted({item["advertiser"] for item in cabinet_catalog}, key=lambda value: value.lower()),
        add_form_data.get("advertiser", ""),
        "Brands",
    )
    add_manager_options = build_select_options(
        sorted({item["manager"] for item in cabinet_catalog}, key=lambda value: value.lower()),
        add_form_data.get("owner_name", ""),
        "Manager",
    )
    add_cabinet_options = build_select_options(
        [item["cabinet"] for item in cabinet_catalog],
        add_form_data.get("cabinet_name", ""),
        "Cabinet",
    )
    edit_advertiser_options = build_select_options(
        sorted({item["advertiser"] for item in cabinet_catalog}, key=lambda value: value.lower()),
        edit_form_data.get("advertiser", ""),
        "Brands",
    )
    edit_manager_options = build_select_options(
        sorted({item["manager"] for item in cabinet_catalog}, key=lambda value: value.lower()),
        edit_form_data.get("owner_name", ""),
        "Manager",
    )
    edit_cabinet_options = build_select_options(
        [item["cabinet"] for item in cabinet_catalog],
        edit_form_data.get("cabinet_name", ""),
        "Cabinet",
    )
    create_panel = f"""
    <details class="upload-menu upload-menu-right" {'open' if add_form_data and not is_edit_mode else ''}>
        <summary class="btn toggle-indicator toolbar-square-trigger" aria-label="Add cap" title="Add cap"></summary>
        <div class="upload-menu-list cap-menu-list">
            <form method="post" action="/caps/save" class="caps-form">
            <input type="hidden" name="edit_id" value="">
            <input type="hidden" name="period_view" value="{escape(selected_period_view)}">
            <input type="hidden" name="buyer" value="{escape(filter_values.get('buyer', ''))}">
            <input type="hidden" name="code_filter" value="{escape(filter_values.get('code', ''))}">
            <input type="hidden" name="search" value="{escape(filter_values.get('search', ''))}">
            <input type="hidden" name="sort_by" value="{escape(sort_by)}">
            <input type="hidden" name="order" value="{escape(order)}">
            <input type="hidden" id="capCabinetCatalog" value="{cabinet_catalog_json}">
            <datalist id="capAgentOptions">{agent_list}</datalist>
            <div class="period-picker" data-period-nav="local">
                <button type="button" class="ghost-btn small-btn period-jump-btn" data-period-jump="-1" aria-label="Previous period">‹</button>
                <label>Period
                    <select name="period_label" required>{period_options}</select>
                </label>
                <button type="button" class="ghost-btn small-btn period-jump-btn" data-period-jump="1" aria-label="Next period">›</button>
            </div>
            <div class="caps-grid-2">
                <label>Brands
                    <select name="advertiser" id="addCapAdvertiserSelect" required>{add_advertiser_options}</select>
                </label>
                <label>Manager
                    <select name="owner_name" id="addCapManagerSelect" required>{add_manager_options}</select>
                </label>
            </div>
            <label>Cabinet
                <select name="cabinet_name" id="addCapCabinetSelect" required>{add_cabinet_options}</select>
            </label>
            <div class="caps-grid-2">
                <label>Code
                    <input type="text" name="code" id="addCapCode" value="{escape(add_form_data.get('code', ''))}">
                </label>
                <label>Geo
                    <input type="text" id="addCapGeo" value="{escape(geo_display_name(add_form_data.get('code', '')))}" readonly>
                </label>
            </div>
            <div class="caps-grid-2">
                <label>Rate
                    <input type="text" name="rate" value="{escape(add_form_data.get('rate', ''))}">
                </label>
                <label>Baseline
                    <input type="text" name="baseline" value="{escape(add_form_data.get('baseline', ''))}">
                </label>
            </div>
            <label>Cap
                <input type="number" step="0.01" name="cap_value" value="{escape(add_form_data.get('cap_value', ''))}" required>
            </label>
            <div class="caps-grid-2">
                <label>Promocode
                    <input type="text" name="promo_code" value="{escape(add_form_data.get('promo_code', ''))}">
                </label>
                <label>TG
                    <input type="text" name="agent" list="capAgentOptions" value="{escape(add_form_data.get('agent', ''))}">
                </label>
            </div>
            <label>Link
                <input type="text" name="link" value="{escape(add_form_data.get('link', ''))}">
            </label>
            <label>KPI
                <textarea name="kpi">{escape(add_form_data.get('kpi', ''))}</textarea>
            </label>
            <label>Comments
                <textarea name="comments">{escape(add_form_data.get('comments', ''))}</textarea>
            </label>
            <div style="display:flex; gap:10px; flex-wrap:wrap;">
                <button type="submit" class="btn">Add Cap</button>
                <a href="/caps" class="ghost-btn">Reset</a>
            </div>
            </form>
        </div>
    </details>
    """

    edit_panel = f"""
    <aside class="cap-edit-drawer {'open' if is_edit_mode else ''}" id="capEditDrawer" aria-hidden="{'false' if is_edit_mode else 'true'}">
        <div class="cap-edit-card">
            <div class="cap-edit-head">
                <div class="cap-edit-title">Edit Cap</div>
                <button type="button" class="ghost-btn cap-edit-close" id="capEditClose" aria-label="Close">×</button>
            </div>
            <form method="post" action="/caps/save" class="caps-form" id="capEditForm">
                <input type="hidden" name="edit_id" id="editCapEditId" value="{escape(edit_form_data.get('edit_id', ''))}">
                <input type="hidden" name="period_view" value="{escape(selected_period_view)}">
                <input type="hidden" name="buyer" value="{escape(filter_values.get('buyer', ''))}">
                <input type="hidden" name="code_filter" value="{escape(filter_values.get('code', ''))}">
                <input type="hidden" name="search" value="{escape(filter_values.get('search', ''))}">
                <input type="hidden" name="sort_by" value="{escape(sort_by)}">
                <input type="hidden" name="order" value="{escape(order)}">
                <datalist id="editCapAgentOptions">{agent_list}</datalist>
                <div class="period-picker" data-period-nav="local">
                    <button type="button" class="ghost-btn small-btn period-jump-btn" data-period-jump="-1" aria-label="Previous period">‹</button>
                    <label>Period
                        <select name="period_label" id="editCapPeriodSelect" required>{period_options}</select>
                    </label>
                    <button type="button" class="ghost-btn small-btn period-jump-btn" data-period-jump="1" aria-label="Next period">›</button>
                </div>
                <div class="caps-grid-2">
                    <label>Brands
                        <select name="advertiser" id="editCapAdvertiserSelect" required>{edit_advertiser_options}</select>
                    </label>
                    <label>Manager
                        <select name="owner_name" id="editCapManagerSelect" required>{edit_manager_options}</select>
                    </label>
                </div>
                <label>Cabinet
                    <select name="cabinet_name" id="editCapCabinetSelect" required>{edit_cabinet_options}</select>
                </label>
                <div class="caps-grid-2">
                    <label>Code
                        <input type="text" name="code" id="editCapCode" value="{escape(edit_form_data.get('code', ''))}">
                    </label>
                    <label>Geo
                        <input type="text" id="editCapGeo" value="{escape(geo_display_name(edit_form_data.get('code', '')))}" readonly>
                    </label>
                </div>
                <div class="caps-grid-2">
                    <label>Rate
                        <input type="text" name="rate" id="editCapRate" value="{escape(edit_form_data.get('rate', ''))}">
                    </label>
                    <label>Baseline
                        <input type="text" name="baseline" id="editCapBaseline" value="{escape(edit_form_data.get('baseline', ''))}">
                    </label>
                </div>
                <label>Cap
                    <input type="number" step="0.01" name="cap_value" id="editCapValue" value="{escape(edit_form_data.get('cap_value', ''))}" required>
                </label>
                <div class="caps-grid-2">
                    <label>Promocode
                        <input type="text" name="promo_code" id="editCapPromocode" value="{escape(edit_form_data.get('promo_code', ''))}">
                    </label>
                    <label>TG
                        <input type="text" name="agent" list="editCapAgentOptions" id="editCapAgent" value="{escape(edit_form_data.get('agent', ''))}">
                    </label>
                </div>
                <label>Link
                    <input type="text" name="link" id="editCapLink" value="{escape(edit_form_data.get('link', ''))}">
                </label>
                <label>KPI
                    <textarea name="kpi" id="editCapKpi">{escape(edit_form_data.get('kpi', ''))}</textarea>
                </label>
                <label>Comments
                    <textarea name="comments" id="editCapComments">{escape(edit_form_data.get('comments', ''))}</textarea>
                </label>
                <div style="display:flex; gap:10px; flex-wrap:wrap;">
                    <button type="submit" class="btn">Save</button>
                    <button type="button" class="ghost-btn" id="capEditCancel">Close</button>
                </div>
            </form>
        </div>
    </aside>
    """

    content = f"""
    {message_html}
    {render_active_period_banner(selected_period)}
    {edit_panel}
    <div class="caps-sticky-header" id="capsStickyHeader" aria-hidden="true"></div>
    <div>
        <div class="panel compact-panel caps-toolbar-panel">
            <div class="toolbar-actions">
                <div class="panel compact-panel filters">
                    <form method="get" action="/caps" id="capsFilterForm">
                        <input type="hidden" name="period_view" value="period">
                        <div class="period-picker">
                            <button type="button" class="ghost-btn small-btn period-jump-btn" data-period-jump="-1" aria-label="Previous period">‹</button>
                            <label>Period<select name="period_label" id="capsPeriodSelect">{period_options}</select></label>
                            <button type="button" class="ghost-btn small-btn period-jump-btn" data-period-jump="1" aria-label="Next period">›</button>
                        </div>
                        <label>Cabinet<select name="buyer">{buyer_options}</select></label>
                        <label>Code<select name="code">{code_options}</select></label>
                        <label>Search<input type="text" name="search" value="{escape(filter_values.get('search', ''))}" placeholder="Search caps"></label>
                        <input type="hidden" name="sort_by" value="{escape(sort_by)}">
                        <input type="hidden" name="order" value="{escape(order)}">
                        <div class="filter-actions-stack">
                            <button type="submit" class="btn small-btn">Filter</button>
                            <a href="/caps" class="ghost-btn small-btn filter-reset-btn" data-reset-filters="caps" aria-label="Reset filters" title="Reset filters">×</a>
                        </div>
                    </form>
                </div>
                <div class="caps-toolbar-stats">
                    <div class="mini-stat"><div class="name">cap total</div><div class="value">{format_int_or_float(total_cap)}</div></div>
                    <div class="mini-stat"><div class="name">ftd</div><div class="value">{format_int_or_float(total_current)}</div></div>
                    <div class="mini-stat"><div class="name">remaining</div><div class="value">{format_int_or_float(total_remaining)}</div></div>
                    <div class="mini-stat"><div class="name">fill avg</div><div class="value">{fill_avg:.0f}%</div></div>
                </div>
                {create_panel}
            </div>
        </div>

        <div class="panel compact-panel">
            <div class="table-wrap caps-table-wrap">
                <table class="caps-table" id="capsTable">
                    <thead>
                        <tr>
                            <th class="advertiser-col" data-col="advertiser" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("advertiser", "Brands")}<span class="resizer"></span></div></th>
                            <th class="owner-col" data-col="manager" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("manager", "Manager")}<span class="resizer"></span></div></th>
                            <th class="buyer-col" data-col="cabinet" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("cabinet", "Cabinet")}<span class="resizer"></span></div></th>
                            <th class="code-col" data-col="code" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("code", "Code")}<span class="resizer"></span></div></th>
                            <th class="geo-col" data-col="geo" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("geo", "Geo")}<span class="resizer"></span></div></th>
                            <th class="rate-col" data-col="rate" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("rate", "Rate")}<span class="resizer"></span></div></th>
                            <th class="baseline-col" data-col="baseline" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("baseline", "Baseline")}<span class="resizer"></span></div></th>
                            <th class="cap-col" data-col="cap" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("cap", "Cap")}<span class="resizer"></span></div></th>
                            <th class="current-col" data-col="current_ftd" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("current_ftd", "FTD")}<span class="resizer"></span></div></th>
                            <th class="remaining-col" data-col="remaining" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("remaining", "Remaining")}<span class="resizer"></span></div></th>
                            <th class="fill-col" data-col="fill" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("fill", "Fill")}<span class="resizer"></span></div></th>
                            <th class="agent-col" data-col="agent" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("agent", "TG")}<span class="resizer"></span></div></th>
                            <th class="chat-col" data-col="chat_title" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("chat_title", "Chat Name")}<span class="resizer"></span></div></th>
                            <th class="kpi-col" data-col="kpi" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("kpi", "KPI")}<span class="resizer"></span></div></th>
                            <th class="comment-col" data-col="comments" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("comments", "Comments")}<span class="resizer"></span></div></th>
                            <th class="promo-col" data-col="promo_code" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("promo_code", "Promocode")}<span class="resizer"></span></div></th>
                            <th class="link-col" data-col="link" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("link", "Link")}<span class="resizer"></span></div></th>
                            <th class="action-col" data-col="action" draggable="true"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("action", "Action")}<span class="resizer"></span></div></th>
                        </tr>
                    </thead>
                        <tbody>{rows_html if rows_html else '<tr><td colspan="18">No caps yet</td></tr>'}</tbody>
                </table>
            </div>
        </div>
    </div>
    <div class="confirm-overlay" id="capDeleteOverlay" aria-hidden="true">
        <div class="confirm-card">
            <div class="confirm-title">Delete cap?</div>
            <div class="confirm-text">This action will remove the selected cap from the list. You can cancel if you opened it by mistake.</div>
            <div class="confirm-actions">
                <button type="button" class="ghost-btn" id="capDeleteCancel">Cancel</button>
                <button type="button" class="btn danger-btn" id="capDeleteConfirm">Delete</button>
            </div>
        </div>
    </div>
    """
    extra_scripts = """
    <script>
        (function initCapsDeleteModal() {
            const overlay = document.getElementById('capDeleteOverlay');
            if (!overlay) return;
            const cancelBtn = document.getElementById('capDeleteCancel');
            const confirmBtn = document.getElementById('capDeleteConfirm');
            let activeForm = null;

            function closeModal() {
                overlay.classList.remove('open');
                overlay.setAttribute('aria-hidden', 'true');
                activeForm = null;
            }

            document.querySelectorAll('.cap-delete-trigger').forEach(function(button) {
                button.addEventListener('click', function() {
                    activeForm = button.closest('form');
                    overlay.classList.add('open');
                    overlay.setAttribute('aria-hidden', 'false');
                });
            });

            cancelBtn?.addEventListener('click', closeModal);
            confirmBtn?.addEventListener('click', function() {
                if (activeForm) activeForm.submit();
            });
            overlay.addEventListener('click', function(event) {
                if (event.target === overlay) closeModal();
            });
            document.addEventListener('keydown', function(event) {
                if (event.key === 'Escape' && overlay.classList.contains('open')) closeModal();
            });
        })();
        (function initCapsCopyButtons() {
            function showCopyFeedback(button, text) {
                if (!button) return;
                const original = button.dataset.originalLabel || button.textContent;
                button.dataset.originalLabel = original;
                button.classList.add('is-copied');
                button.textContent = text;
                setTimeout(function() {
                    button.classList.remove('is-copied');
                    button.textContent = original;
                }, 1200);
            }
            document.querySelectorAll('.cap-copy-link').forEach(function(button) {
                button.addEventListener('click', async function() {
                    const value = button.getAttribute('data-link') || '';
                    if (!value) return;
                    try {
                        await navigator.clipboard.writeText(value);
                        showCopyFeedback(button, '✓');
                    } catch (err) {
                        const temp = document.createElement('textarea');
                        temp.value = value;
                        temp.style.position = 'fixed';
                        temp.style.opacity = '0';
                        document.body.appendChild(temp);
                        temp.focus();
                        temp.select();
                        try {
                            document.execCommand('copy');
                            showCopyFeedback(button, '✓');
                        } catch (copyErr) {
                            showCopyFeedback(button, '!');
                        }
                        document.body.removeChild(temp);
                    }
                });
            });
        })();
        (function initCapsCabinetSelectors() {
            const source = document.getElementById('capCabinetCatalog');
            if (!source) return;

            let records = [];
            try {
                records = JSON.parse(source.value || '[]');
            } catch (error) {
                records = [];
            }
            if (!Array.isArray(records) || !records.length) return;

            function normalize(value) {
                return (value || '').trim().toLowerCase();
            }

            function uniqueValues(items, key) {
                const seen = new Set();
                const values = [];
                items.forEach(function(item) {
                    const value = (item[key] || '').trim();
                    const norm = normalize(value);
                    if (!norm || seen.has(norm)) return;
                    seen.add(norm);
                    values.push(value);
                });
                values.sort(function(a, b) { return a.localeCompare(b); });
                return values;
            }

            function fillOptions(select, values, placeholder, selected) {
                const cleanSelected = (selected || '').trim();
                select.innerHTML = '';
                const emptyOption = document.createElement('option');
                emptyOption.value = '';
                emptyOption.textContent = '';
                emptyOption.selected = !cleanSelected;
                select.appendChild(emptyOption);
                values.forEach(function(value) {
                    const option = document.createElement('option');
                    option.value = value;
                    option.textContent = value;
                    if (value === cleanSelected) option.selected = true;
                    select.appendChild(option);
                });
                if (cleanSelected && !values.includes(cleanSelected)) {
                    select.value = '';
                }
            }

            function setup(prefix) {
                const advertiserSelect = document.getElementById(prefix + 'AdvertiserSelect');
                const managerSelect = document.getElementById(prefix + 'ManagerSelect');
                const cabinetSelect = document.getElementById(prefix + 'CabinetSelect');
                if (!advertiserSelect || !managerSelect || !cabinetSelect) return null;

                function filterRecords(filters) {
                    return records.filter(function(item) {
                        return (!filters.advertiser || item.advertiser === filters.advertiser)
                            && (!filters.manager || item.manager === filters.manager)
                            && (!filters.cabinet || item.cabinet === filters.cabinet);
                    });
                }

                function refresh(changedField) {
                    let advertiser = advertiserSelect.value;
                    let manager = managerSelect.value;
                    let cabinet = cabinetSelect.value;
                    const allAdvertisers = uniqueValues(records, 'advertiser');
                    const allManagers = uniqueValues(records, 'manager');
                    const allCabinets = uniqueValues(records, 'cabinet');

                    if (changedField === 'cabinet') {
                        const cabinetMatch = records.find(function(item) {
                            return item.cabinet === cabinet;
                        });
                        if (cabinetMatch) {
                            advertiser = cabinetMatch.advertiser;
                            manager = cabinetMatch.manager;
                        }
                    }

                    if (advertiser && !allAdvertisers.includes(advertiser)) {
                        advertiser = '';
                    }
                    const advertiserValues = uniqueValues(filterRecords({
                        manager: changedField === 'advertiser' ? '' : manager,
                        cabinet: changedField === 'advertiser' ? '' : cabinet,
                    }), 'advertiser');
                    fillOptions(advertiserSelect, advertiserValues.length ? advertiserValues : allAdvertisers, 'Advertiser', advertiser);
                    advertiserSelect.value = advertiser;

                    const managerValues = uniqueValues(filterRecords({
                        advertiser: advertiser,
                        cabinet: changedField === 'manager' ? '' : cabinet,
                    }), 'manager');
                    if (manager && !managerValues.includes(manager)) {
                        manager = '';
                    }
                    fillOptions(managerSelect, managerValues.length ? managerValues : allManagers, 'Manager', manager);
                    managerSelect.value = manager;

                    const cabinetValues = uniqueValues(filterRecords({ advertiser: advertiser, manager: manager }), 'cabinet');
                    if (cabinet && !cabinetValues.includes(cabinet)) {
                        cabinet = '';
                    }
                    fillOptions(cabinetSelect, cabinetValues.length ? cabinetValues : allCabinets, 'Cabinet', cabinet);
                    cabinetSelect.value = cabinet;

                    if (changedField !== 'cabinet' && cabinetSelect.value) {
                        const exactMatches = filterRecords({
                            advertiser: advertiserSelect.value,
                            manager: managerSelect.value,
                            cabinet: cabinetSelect.value,
                        });
                        if (!exactMatches.length) {
                            cabinetSelect.value = '';
                        }
                    }

                    if (changedField === 'manager' && managerSelect.value) {
                        const advertiserForManager = uniqueValues(filterRecords({
                            manager: managerSelect.value,
                            cabinet: cabinetSelect.value,
                        }), 'advertiser');
                        if (advertiserForManager.length === 1) {
                            advertiserSelect.value = advertiserForManager[0];
                        }
                    }
                }

                advertiserSelect.addEventListener('change', function() { refresh('advertiser'); });
                managerSelect.addEventListener('change', function() { refresh('manager'); });
                cabinetSelect.addEventListener('change', function() { refresh('cabinet'); });
                refresh('');
                return { refresh: refresh };
            }

            const setups = {
                addCap: setup('addCap'),
                editCap: setup('editCap'),
            };
            window.capFormSetups = setups;
        })();
        (function initCapsEditDrawer() {
            const drawer = document.getElementById('capEditDrawer');
            if (!drawer) return;
            const closeButton = document.getElementById('capEditClose');
            const cancelButton = document.getElementById('capEditCancel');

            function setValue(id, value) {
                const element = document.getElementById(id);
                if (element) element.value = value || '';
            }

            function closeDrawer() {
                drawer.classList.remove('open');
                drawer.setAttribute('aria-hidden', 'true');
            }

            function openDrawer(payload) {
                setValue('editCapEditId', payload.id);
                setValue('editCapPeriodSelect', payload.period_label);
                setValue('editCapAdvertiserSelect', payload.advertiser);
                setValue('editCapManagerSelect', payload.owner_name);
                setValue('editCapCabinetSelect', payload.cabinet_name);
                const setup = (window.capFormSetups || {}).editCap;
                if (setup && typeof setup.refresh === 'function') {
                    setup.refresh('');
                }
                setValue('editCapAdvertiserSelect', payload.advertiser);
                setValue('editCapManagerSelect', payload.owner_name);
                setValue('editCapCabinetSelect', payload.cabinet_name);
                setValue('editCapCode', payload.code);
                setValue('editCapRate', payload.rate);
                setValue('editCapBaseline', payload.baseline);
                setValue('editCapValue', payload.cap_value);
                setValue('editCapPromocode', payload.promo_code);
                setValue('editCapAgent', payload.agent);
                setValue('editCapLink', payload.link);
                setValue('editCapKpi', payload.kpi);
                setValue('editCapComments', payload.comments);
                drawer.classList.add('open');
                drawer.setAttribute('aria-hidden', 'false');
            }

            document.querySelectorAll('.cap-edit-trigger').forEach(function(button) {
                button.addEventListener('click', function() {
                    let payload = {};
                    try {
                        payload = JSON.parse(button.getAttribute('data-cap') || '{}');
                    } catch (error) {
                        payload = {};
                    }
                    openDrawer(payload);
                });
            });

            closeButton?.addEventListener('click', closeDrawer);
            cancelButton?.addEventListener('click', closeDrawer);
            document.addEventListener('keydown', function(event) {
                if (event.key === 'Escape' && drawer.classList.contains('open')) closeDrawer();
            });
        })();
            (function initCapsPeriodJump() {
                const form = document.getElementById('capsFilterForm');
                const select = document.getElementById('capsPeriodSelect');
                if (!form || !select) return;

            function updateButtons() {
                const options = Array.from(select.options || []);
                const currentIndex = options.findIndex(function(option) {
                    return option.value === select.value;
                });
                document.querySelectorAll('[data-period-jump]').forEach(function(button) {
                    const step = Number(button.getAttribute('data-period-jump') || '0');
                    const nextIndex = currentIndex + step;
                    button.disabled = nextIndex < 0 || nextIndex >= options.length;
                    button.style.opacity = button.disabled ? '0.45' : '1';
                });
            }

            document.querySelectorAll('[data-period-jump]').forEach(function(button) {
                button.addEventListener('click', function() {
                    const options = Array.from(select.options || []);
                    const currentIndex = options.findIndex(function(option) {
                        return option.value === select.value;
                    });
                    const step = Number(button.getAttribute('data-period-jump') || '0');
                    const nextIndex = currentIndex + step;
                    if (nextIndex < 0 || nextIndex >= options.length) return;
                    select.value = options[nextIndex].value;
                    form.requestSubmit();
                });
            });

            select.addEventListener('change', updateButtons);
            updateButtons();
            })();
            (function initLocalPeriodPickers() {
                document.querySelectorAll('.period-picker[data-period-nav="local"]').forEach(function(picker) {
                    const select = picker.querySelector('select[name="period_label"]');
                    if (!select) return;
                    function updateButtons() {
                        const options = Array.from(select.options || []);
                        const currentIndex = options.findIndex(function(option) {
                            return option.value === select.value;
                        });
                        picker.querySelectorAll('[data-period-jump]').forEach(function(button) {
                            const step = Number(button.getAttribute('data-period-jump') || '0');
                            const nextIndex = currentIndex + step;
                            button.disabled = currentIndex < 0 || nextIndex < 0 || nextIndex >= options.length;
                            button.style.opacity = button.disabled ? '0.45' : '1';
                        });
                    }
                    picker.querySelectorAll('[data-period-jump]').forEach(function(button) {
                        button.addEventListener('click', function() {
                            const options = Array.from(select.options || []);
                            const currentIndex = options.findIndex(function(option) {
                                return option.value === select.value;
                            });
                            const step = Number(button.getAttribute('data-period-jump') || '0');
                            const nextIndex = currentIndex + step;
                            if (currentIndex < 0 || nextIndex < 0 || nextIndex >= options.length) return;
                            select.value = options[nextIndex].value;
                            select.dispatchEvent(new Event('change', { bubbles: true }));
                            updateButtons();
                        });
                    });
                    select.addEventListener('change', updateButtons);
                    updateButtons();
                });
            })();
            (function initCapsStickyHeader() {
                const wrap = document.querySelector('.caps-table-wrap');
                const table = document.getElementById('capsTable');
            const thead = table ? table.querySelector('thead') : null;
            const stickyHost = document.getElementById('capsStickyHeader');
            if (!wrap || !table || !thead || !stickyHost) return;

            let stickyTable = null;
            let stickyHeadCells = [];

            function buildStickyHeader() {
                stickyHost.innerHTML = '';
                const shell = document.createElement('div');
                shell.className = 'table-wrap';
                stickyTable = document.createElement('table');
                stickyTable.className = table.className;
                const clonedHead = thead.cloneNode(true);
                stickyTable.appendChild(clonedHead);
                shell.appendChild(stickyTable);
                stickyHost.appendChild(shell);
                stickyHeadCells = Array.from(clonedHead.querySelectorAll('th'));
                stickyHeadCells.forEach(function(cell) {
                    cell.removeAttribute('draggable');
                    const resizer = cell.querySelector('.resizer');
                    if (resizer) resizer.remove();
                });
            }

            function syncStickyHeader() {
                if (!stickyTable || stickyHeadCells.length !== thead.querySelectorAll('th').length) {
                    buildStickyHeader();
                }
                const sourceCells = Array.from(thead.querySelectorAll('th'));
                const wrapRect = wrap.getBoundingClientRect();
                stickyHost.style.left = wrapRect.left + 'px';
                stickyHost.style.width = wrapRect.width + 'px';
                stickyTable.style.width = table.getBoundingClientRect().width + 'px';
                stickyTable.style.transform = 'translateX(' + (-wrap.scrollLeft) + 'px)';
                sourceCells.forEach(function(cell, index) {
                    const width = Math.round(cell.getBoundingClientRect().width);
                    const clone = stickyHeadCells[index];
                    if (!clone) return;
                    clone.style.width = width + 'px';
                    clone.style.minWidth = width + 'px';
                    clone.style.maxWidth = width + 'px';
                });
            }

            function updateStickyVisibility() {
                const tableRect = table.getBoundingClientRect();
                const headRect = thead.getBoundingClientRect();
                const shouldShow = headRect.top < 10 && tableRect.bottom > 90;
                stickyHost.classList.toggle('open', shouldShow);
                if (shouldShow) syncStickyHeader();
            }

            buildStickyHeader();
            syncStickyHeader();
            updateStickyVisibility();

            wrap.addEventListener('scroll', function() {
                if (stickyHost.classList.contains('open')) syncStickyHeader();
            }, { passive: true });
            window.addEventListener('scroll', updateStickyVisibility, { passive: true });
            window.addEventListener('resize', function() {
                syncStickyHeader();
                updateStickyVisibility();
            });

            if (window.ResizeObserver) {
                const observer = new ResizeObserver(function() {
                    syncStickyHeader();
                    updateStickyVisibility();
                });
                observer.observe(table);
                observer.observe(wrap);
            }
        })();
        (function initCapsColumns() {
            const table = document.getElementById('capsTable');
            if (!table) return;
            const ORDER_KEY = window.teambeadStorageKey('capsColumnsOrder:v4');
            const WIDTH_KEY = window.teambeadStorageKey('capsColumnsWidth:v4');
            const HIDDEN_KEY = window.teambeadStorageKey('capsColumnsHidden:v4');

            function getCurrentOrder() {
                return Array.from(table.querySelectorAll('thead th[data-col]')).map(th => th.dataset.col);
            }
            function reorderCells(order) {
                table.querySelectorAll('tr').forEach(function(row) {
                    const cells = {};
                    Array.from(row.children).forEach(function(cell) {
                        if (cell.dataset.col) cells[cell.dataset.col] = cell;
                    });
                    order.forEach(function(key) {
                        if (cells[key]) row.appendChild(cells[key]);
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
                document.querySelectorAll('.column-toggle-caps').forEach(cb => {
                    cb.checked = !hidden.includes(cb.value);
                });
                document.querySelectorAll('#capsTable [data-col]').forEach(el => {
                    el.style.display = hidden.includes(el.dataset.col) ? 'none' : '';
                });
            }
            function saveVisibility() {
                const hidden = [];
                document.querySelectorAll('.column-toggle-caps').forEach(cb => {
                    if (!cb.checked) hidden.push(cb.value);
                });
                localStorage.setItem(HIDDEN_KEY, JSON.stringify(hidden));
                applyVisibility();
            }
            function applyWidths() {
                const widths = JSON.parse(localStorage.getItem(WIDTH_KEY) || '{}');
                Object.entries(widths).forEach(([key, width]) => {
                    document.querySelectorAll('#capsTable [data-col="' + key + '"]').forEach(el => {
                        el.style.width = width + 'px';
                        el.style.minWidth = width + 'px';
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
                    document.querySelectorAll('#capsTable [data-col="' + key + '"]').forEach(el => {
                        el.style.width = newWidth + 'px';
                        el.style.minWidth = newWidth + 'px';
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
                    if (e.target.classList.contains('resizer')) {
                        e.preventDefault();
                        return;
                    }
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
            document.querySelectorAll('.column-toggle-caps').forEach(cb => cb.addEventListener('change', saveVisibility));
            applyOrder();
            applyVisibility();
            applyWidths();
        })();
    </script>
    """
    return page_shell("Caps", content, active_page="caps", extra_scripts=extra_scripts, current_user=current_user)


def render_finance_table(title, subtitle, headers, rows_html, min_width="980px"):
    return f"""
    <div class="panel compact-panel">
        <div class="panel-title" style="margin-bottom:10px;">{escape(title)}</div>
        <div class="table-wrap">
            <table class="finance-table" style="min-width:{escape(min_width)};">
                <thead><tr>{headers}</tr></thead>
                <tbody>{rows_html if rows_html else '<tr><td colspan="6">No data</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    """


def finance_page_html(current_user, success_text="", error_text="", form_data=None, filter_values=None):
    snapshot = load_finance_snapshot()
    manual_all = load_manual_finance()
    form_data = form_data or {}
    filter_values = filter_values or {}
    date_from = safe_text(filter_values.get("date_from"))
    date_to = safe_text(filter_values.get("date_to"))
    year = safe_text(filter_values.get("year"))
    period_view = safe_text(filter_values.get("period_view") or "current")
    period_label = safe_text(filter_values.get("period_label"))
    effective_period_label = resolve_period_label(period_view, period_label) or get_current_period_label()
    manual = filter_finance_manual_rows(
        manual_all,
        date_from=date_from,
        date_to=date_to,
        year=year,
        period_label=effective_period_label if period_view != "all" else "",
    )
    period_view_options = "".join([
        f'<option value="{value}" {"selected" if period_view == value else ""}>{label}</option>'
        for value, label in [("all", "All Time"), ("current", "Current Period"), ("period", "Choose Period")]
    ])
    period_options = make_options(build_period_options(), effective_period_label)
    balances = compute_finance_balances(snapshot, manual_all)

    service_wallet_rows = ""
    for item in manual_all["wallets"]:
        service_wallet_rows += f"""
        <tr>
            <td>{escape(item.category or "")}</td>
            <td>{escape(item.description or "")}</td>
            <td>{escape(item.owner_name or "")}</td>
            <td class="wallet-code">{escape(item.wallet or "")}</td>
            <td>{format_money(item.amount)}</td>
            <td>
                <div class="caps-actions">
                    <form method="get" action="/finance">
                        <input type="hidden" name="edit_wallet" value="{item.id}">
                        <input type="hidden" name="date_from" value="{escape(date_from)}">
                        <input type="hidden" name="date_to" value="{escape(date_to)}">
                        <input type="hidden" name="year" value="{escape(year)}">
                        <button type="submit" class="ghost-btn small-btn">Edit</button>
                    </form>
                    <form method="post" action="/finance/wallets/delete" onsubmit="return confirm('Delete this wallet?');">
                        <input type="hidden" name="wallet_id" value="{item.id}">
                        <input type="hidden" name="date_from" value="{escape(date_from)}">
                        <input type="hidden" name="date_to" value="{escape(date_to)}">
                        <input type="hidden" name="year" value="{escape(year)}">
                        <button type="submit" class="ghost-btn small-btn">Delete</button>
                    </form>
                </div>
            </td>
        </tr>
        """

    balance_rows = ""
    for item in balances["rows"]:
        balance_rows += f"""
        <tr>
            <td>{escape(item['wallet_name'])}</td>
            <td>{format_money(item['balance'])}</td>
        </tr>
        """

    operation_rows = ""
    for item in manual["expenses"]:
        operation_rows += f"""
        <tr>
            <td>{item.id}</td>
            <td>Expense</td>
            <td>{escape(item.expense_date or "")}</td>
            <td>{escape(item.category or "")}</td>
            <td>{escape(item.wallet_name or "")}</td>
            <td>{escape(item.from_wallet or item.paid_by or "")}</td>
            <td>{format_money(item.amount)}</td>
            <td>{escape(item.comment or "")}</td>
            <td>
                <div class="caps-actions">
                    <form method="get" action="/finance">
                        <input type="hidden" name="edit_expense" value="{item.id}">
                        <input type="hidden" name="date_from" value="{escape(date_from)}">
                        <input type="hidden" name="date_to" value="{escape(date_to)}">
                        <input type="hidden" name="year" value="{escape(year)}">
                        <button type="submit" class="ghost-btn small-btn">Edit</button>
                    </form>
                    <form method="post" action="/finance/expenses/delete" onsubmit="return confirm('Delete expense?');">
                        <input type="hidden" name="expense_id" value="{item.id}">
                        <input type="hidden" name="date_from" value="{escape(date_from)}">
                        <input type="hidden" name="date_to" value="{escape(date_to)}">
                        <input type="hidden" name="year" value="{escape(year)}">
                        <button type="submit" class="ghost-btn small-btn">Delete</button>
                    </form>
                </div>
            </td>
        </tr>
        """
    for item in manual["income"]:
        operation_rows += f"""
        <tr>
            <td>{item.id}</td>
            <td>Income</td>
            <td>{escape(item.income_date or "")}</td>
            <td>{escape(item.category or "")}</td>
            <td>{escape(item.wallet_name or item.wallet or "")}</td>
            <td>{escape(item.from_wallet or item.reconciliation or "")}</td>
            <td>{format_money(item.amount)}</td>
            <td>{escape(item.comment or item.description or "")}</td>
            <td>
                <div class="caps-actions">
                    <form method="get" action="/finance">
                        <input type="hidden" name="edit_income" value="{item.id}">
                        <input type="hidden" name="date_from" value="{escape(date_from)}">
                        <input type="hidden" name="date_to" value="{escape(date_to)}">
                        <input type="hidden" name="year" value="{escape(year)}">
                        <button type="submit" class="ghost-btn small-btn">Edit</button>
                    </form>
                    <form method="post" action="/finance/income/delete" onsubmit="return confirm('Delete income?');">
                        <input type="hidden" name="income_id" value="{item.id}">
                        <input type="hidden" name="date_from" value="{escape(date_from)}">
                        <input type="hidden" name="date_to" value="{escape(date_to)}">
                        <input type="hidden" name="year" value="{escape(year)}">
                        <button type="submit" class="ghost-btn small-btn">Delete</button>
                    </form>
                </div>
            </td>
        </tr>
        """
    for item in manual["transfers"]:
        operation_rows += f"""
        <tr>
            <td>{item.id}</td>
            <td>Transfer</td>
            <td>{escape(item.transfer_date or "")}</td>
            <td>{escape(item.category or "")}</td>
            <td>{escape(item.to_wallet or "")}</td>
            <td>{escape(item.from_wallet or "")}</td>
            <td>{format_money(item.amount)}</td>
            <td>{escape(item.comment or "")}</td>
            <td>
                <div class="caps-actions">
                    <form method="get" action="/finance">
                        <input type="hidden" name="edit_transfer" value="{item.id}">
                        <input type="hidden" name="date_from" value="{escape(date_from)}">
                        <input type="hidden" name="date_to" value="{escape(date_to)}">
                        <input type="hidden" name="year" value="{escape(year)}">
                        <button type="submit" class="ghost-btn small-btn">Edit</button>
                    </form>
                    <form method="post" action="/finance/transfers/delete" onsubmit="return confirm('Delete transfer?');">
                        <input type="hidden" name="transfer_id" value="{item.id}">
                        <input type="hidden" name="date_from" value="{escape(date_from)}">
                        <input type="hidden" name="date_to" value="{escape(date_to)}">
                        <input type="hidden" name="year" value="{escape(year)}">
                        <button type="submit" class="ghost-btn small-btn">Delete</button>
                    </form>
                </div>
            </td>
        </tr>
        """

    wallet_submit_label = "Save Changes" if form_data.get("wallet_edit_id") else "Save Wallet"
    expense_submit_label = "Save Changes" if form_data.get("expense_edit_id") else "Save Expense"
    income_submit_label = "Save Changes" if form_data.get("income_edit_id") else "Save Income"
    transfer_submit_label = "Save Changes" if form_data.get("transfer_edit_id") else "Save Transfer"
    message_html = ""
    if success_text:
        message_html += f'<div class="notice">{escape(success_text)}</div>'
    if error_text:
        message_html += f'<div class="notice notice-danger">{escape(error_text)}</div>'

    create_panel = f"""
    <details class="upload-menu upload-menu-right" {'open' if form_data else ''}>
        <summary class="btn small-btn" style="min-width:124px;">
            <span>Manage</span>
            <span class="toggle-indicator" style="width:18px; height:18px; min-width:18px;"></span>
        </summary>
        <div class="upload-menu-list" style="width:min(1120px, calc(100vw - 48px));">
        <div class="finance-grid" style="margin-top:14px;">
            <div class="panel">
                <div class="panel-title">{'Edit Service Wallet' if form_data.get('wallet_edit_id') else 'Add Service Wallet'}</div>
                <form method="post" action="/finance/wallets/save" class="caps-form" style="margin-top:14px;">
                    <input type="hidden" name="edit_id" value="{escape(form_data.get('wallet_edit_id', ''))}">
                    <input type="hidden" name="date_from" value="{escape(date_from)}">
                    <input type="hidden" name="date_to" value="{escape(date_to)}">
                    <input type="hidden" name="year" value="{escape(year)}">
                    <label>Type
                        <select name="category">
                            {make_options(['Сервисы', 'Рекламодатели', 'Партнеры'], form_data.get('wallet_category', 'Сервисы'))}
                        </select>
                    </label>
                    <label>Wallet Name<input type="text" name="description" value="{escape(form_data.get('wallet_description', ''))}" placeholder="Example: Service Wallet 1"></label>
                    <label>Owner<input type="text" name="owner_name" value="{escape(form_data.get('wallet_owner_name', ''))}" placeholder="Ivan"></label>
                    <label>Wallet<input type="text" name="wallet" value="{escape(form_data.get('wallet_wallet', ''))}" placeholder="Wallet address"></label>
                    <label>Opening Balance<input type="number" step="0.01" name="amount" value="{escape(form_data.get('wallet_amount', ''))}" placeholder="0.00"></label>
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
                    <input type="hidden" name="date_from" value="{escape(date_from)}">
                    <input type="hidden" name="date_to" value="{escape(date_to)}">
                    <input type="hidden" name="year" value="{escape(year)}">
                    <label>Date<input type="date" name="expense_date" value="{escape(form_data.get('expense_date', ''))}"></label>
                    <label>Category
                        <select name="category">{make_options(['Сервисы', 'Рекламодатели', 'Партнеры'], form_data.get('expense_category', 'Сервисы'))}</select>
                    </label>
                    <label>Amount<input type="number" step="0.01" name="amount" value="{escape(form_data.get('expense_amount', ''))}" placeholder="0.00"></label>
                    <label>Wallet Name<input type="text" name="wallet_name" value="{escape(form_data.get('expense_wallet_name', ''))}" placeholder="Service wallet name"></label>
                    <label>From Wallet<input type="text" name="from_wallet" value="{escape(form_data.get('expense_from_wallet', ''))}" placeholder="From which wallet sent"></label>
                    <label>Comment<textarea name="comment">{escape(form_data.get('expense_comment', ''))}</textarea></label>
                    <div style="display:flex; gap:10px; flex-wrap:wrap;">
                        <button type="submit" class="btn">{expense_submit_label}</button>
                        <a href="/finance" class="ghost-btn">Reset</a>
                    </div>
                </form>
            </div>
        </div>
        <div class="finance-grid" style="margin-top:16px;">
            <div class="panel">
                <div class="panel-title">{'Edit Income' if form_data.get('income_edit_id') else 'Add Income'}</div>
                <form method="post" action="/finance/income/save" class="caps-form" style="margin-top:14px;">
                    <input type="hidden" name="edit_id" value="{escape(form_data.get('income_edit_id', ''))}">
                    <input type="hidden" name="date_from" value="{escape(date_from)}">
                    <input type="hidden" name="date_to" value="{escape(date_to)}">
                    <input type="hidden" name="year" value="{escape(year)}">
                    <label>Date<input type="date" name="income_date" value="{escape(form_data.get('income_date', ''))}"></label>
                    <label>Category
                        <select name="category">{make_options(['Сервисы', 'Рекламодатели', 'Партнеры'], form_data.get('income_category', 'Сервисы'))}</select>
                    </label>
                    <label>Amount<input type="number" step="0.01" name="amount" value="{escape(form_data.get('income_amount', ''))}" placeholder="0.00"></label>
                    <label>Wallet Name<input type="text" name="wallet_name" value="{escape(form_data.get('income_wallet_name', ''))}" placeholder="Service wallet name"></label>
                    <label>From Wallet<input type="text" name="from_wallet" value="{escape(form_data.get('income_from_wallet', ''))}" placeholder="From which wallet sent"></label>
                    <label>Comment<textarea name="comment">{escape(form_data.get('income_comment', ''))}</textarea></label>
                    <div style="display:flex; gap:10px; flex-wrap:wrap;">
                        <button type="submit" class="btn">{income_submit_label}</button>
                        <a href="/finance" class="ghost-btn">Reset</a>
                    </div>
                </form>
            </div>
            <div class="panel">
                <div class="panel-title">{'Edit Transfer' if form_data.get('transfer_edit_id') else 'Add Transfer'}</div>
                <form method="post" action="/finance/transfers/save" class="caps-form" style="margin-top:14px;">
                    <input type="hidden" name="edit_id" value="{escape(form_data.get('transfer_edit_id', ''))}">
                    <input type="hidden" name="date_from" value="{escape(date_from)}">
                    <input type="hidden" name="date_to" value="{escape(date_to)}">
                    <input type="hidden" name="year" value="{escape(year)}">
                    <label>Date<input type="date" name="transfer_date" value="{escape(form_data.get('transfer_date', ''))}"></label>
                    <label>Category
                        <select name="category">{make_options(['Сервисы', 'Рекламодатели', 'Партнеры'], form_data.get('transfer_category', 'Сервисы'))}</select>
                    </label>
                    <label>Amount<input type="number" step="0.01" name="amount" value="{escape(form_data.get('transfer_amount', ''))}" placeholder="0.00"></label>
                    <label>From Wallet<input type="text" name="from_wallet" value="{escape(form_data.get('transfer_from_wallet', ''))}" placeholder="From wallet"></label>
                    <label>To Wallet<input type="text" name="to_wallet" value="{escape(form_data.get('transfer_to_wallet', ''))}" placeholder="To wallet"></label>
                    <label>Comment<textarea name="comment">{escape(form_data.get('transfer_comment', ''))}</textarea></label>
                    <div style="display:flex; gap:10px; flex-wrap:wrap;">
                        <button type="submit" class="btn">{transfer_submit_label}</button>
                        <a href="/finance" class="ghost-btn">Reset</a>
                    </div>
                </form>
            </div>
        </div>
        </div>
    </details>
    """

    content = f"""
    {message_html}
    {render_active_period_banner(effective_period_label if period_view != "all" else "")}
    <div class="panel compact-panel">
        <div class="controls-line">
            <div>
                <div class="panel-title">Finance</div>
            </div>
            <div class="toolbar-actions">
                <div class="panel compact-panel filters">
                    <form method="get" action="/finance" style="justify-content:flex-end;" data-persist-filters="finance">
                        <label>View<select name="period_view">{period_view_options}</select></label>
                        <label>Period<select name="period_label">{period_options}</select></label>
                        <button type="submit" class="btn small-btn">Filter</button>
                        <a href="/finance" class="ghost-btn small-btn" data-reset-filters="finance">Reset</a>
                    </form>
                </div>
                {create_panel}
            </div>
        </div>
    </div>

    <div class="panel compact-panel">
        <div class="stats-grid">
            <div class="stat-card"><div class="name">Current Total Balance</div><div class="value">{format_money(balances['total'])}</div></div>
            <div class="stat-card"><div class="name">Wallets Count</div><div class="value">{len(manual_all['wallets'])}</div></div>
            <div class="stat-card"><div class="name">Expenses</div><div class="value">{format_money(sum(safe_number(x.amount) for x in manual['expenses']))}</div></div>
            <div class="stat-card"><div class="name">Income</div><div class="value">{format_money(sum(safe_number(x.amount) for x in manual['income']))}</div></div>
            <div class="stat-card"><div class="name">Transfers</div><div class="value">{format_money(sum(safe_number(x.amount) for x in manual['transfers']))}</div></div>
        </div>
    </div>
    {render_finance_table("Service Wallets", "", '<th>Type</th><th>Wallet Name</th><th>Owner</th><th>Wallet</th><th>Opening Balance</th><th>Action</th>', service_wallet_rows, "1240px")}

    {render_finance_table("Wallet Balances", "", '<th>Wallet</th><th>Current Balance</th>', balance_rows, "980px")}

    {render_finance_table("Operations", "", '<th>ID</th><th>Type</th><th>Date</th><th>Category</th><th>Wallet Name / To</th><th>From Wallet</th><th>Amount</th><th>Comment</th><th>Action</th>', operation_rows, "1560px")}
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
        clean_html = '<option value="">All</option>'
        for item in assignable_users:
            selected = "selected" if filter_values.get("assignee", "") == item.username else ""
            clean_html += f'<option value="{escape(item.username)}" {selected}>{escape((item.display_name or item.username) + " · " + (item.role or ""))}</option>'
        assignee_filter_rendered = f'<label>Assignee<select name="assignee">{clean_html}</select></label>'

    assign_options = ""
    for item in assignable_users:
        selected = "selected" if form_data.get("assigned_to_username", "") == item.username else ""
        assign_options += f'<option value="{escape(item.username)}" {selected}>{escape((item.display_name or item.username) + " · " + (item.role or ""))}</option>'

    create_block = ""
    if is_admin_role(current_user):
        due_selects = build_task_datetime_selects("due", form_data.get("due_at", ""))
        create_block = f"""
        <details class="upload-menu upload-menu-right" {'open' if form_data else ''}>
            <summary class="btn small-btn" style="min-width:112px;">
                <span>Add Task</span>
                <span class="toggle-indicator" style="width:18px; height:18px; min-width:18px;"></span>
            </summary>
            <div class="upload-menu-list" style="width:min(720px, calc(100vw - 48px));">
            <form method="post" action="/tasks/save" class="tasks-form" style="margin-top:14px;">
                <input type="hidden" name="status_filter" value="{escape(filter_values.get('status', ''))}">
                <input type="hidden" name="assignee_filter" value="{escape(filter_values.get('assignee', ''))}">
                <input type="hidden" name="search_filter" value="{escape(filter_values.get('search', ''))}">
                <label>Assignee
                    <select name="assigned_to_username" required>{assign_options}</select>
                </label>
                <label>Task
                    <input type="text" name="title" value="{escape(form_data.get('title', ''))}" required placeholder="Example: Check KPI for Peru">
                </label>
                <label>Description
                    <textarea name="description" placeholder="What needs to be done and what result is expected">{escape(form_data.get('description', ''))}</textarea>
                </label>
                <div>
                    <div class="muted" style="margin-bottom:8px; font-size:12px; font-weight:800;">Due Date</div>
                    {due_selects}
                </div>
                <label>Notes
                    <textarea name="notes" placeholder="Extra context, links or agreements">{escape(form_data.get('notes', ''))}</textarea>
                </label>
                <button type="submit" class="btn">Add Task</button>
            </form>
            </div>
        </details>
        """

    task_cards = ""
    for row in rows:
        response_html = f'<div class="task-note">{escape(row.response_text)}</div>' if (row.response_text or "").strip() else '<div class="task-answer-empty">No reply yet</div>'
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
                    <input type="hidden" name="status_filter" value="{escape(filter_values.get('status', ''))}">
                    <input type="hidden" name="assignee_filter" value="{escape(filter_values.get('assignee', ''))}">
                    <input type="hidden" name="search_filter" value="{escape(filter_values.get('search', ''))}">
                    <button type="submit" class="ghost-btn small-btn">Delete</button>
                </form>
                """
            respond_block = f"""
            <form method="post" action="/tasks/respond" class="tasks-form" style="margin-top:14px;">
                <input type="hidden" name="task_id" value="{row.id}">
                <input type="hidden" name="status_filter" value="{escape(filter_values.get('status', ''))}">
                <input type="hidden" name="assignee_filter" value="{escape(filter_values.get('assignee', ''))}">
                <input type="hidden" name="search_filter" value="{escape(filter_values.get('search', ''))}">
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
                    <div class="task-title">{escape(row.title or "Untitled")}</div>
                    <div class="muted">{escape(row.description or "No description")}</div>
                </div>
                <div class="task-meta">
                    <div class="task-chip">Status: {escape(row.status or "Not started")}</div>
                    <div class="task-chip">Due: {escape(due_text)}</div>
                    <div class="task-chip">Reply: {escape(answered_text)}</div>
                    {admin_controls}
                </div>
            </div>
            <div class="task-body">
                <div>
                    <div class="muted" style="margin-bottom:8px; font-size:12px; font-weight:800;">Task Brief</div>
                    <div class="task-note">{escape(row.notes or "No extra notes")}</div>
                </div>
                <div>
                    <div class="muted" style="margin-bottom:8px; font-size:12px; font-weight:800;">Assignee Reply</div>
                    {response_html}
                </div>
            </div>
            <div class="muted" style="margin-top:12px;">By {escape(row.created_by_name or row.created_by_username)} · created {escape(format_datetime_human(row.created_at))}</div>
            {respond_block}
        </div>
        """

    content = f"""
    {message_html}
    <div class="panel compact-panel">
        <div class="controls-line">
            <div class="toolbar-actions">
                <div class="panel compact-panel filters">
                    <form method="get" action="/tasks" data-persist-filters="tasks">
                        {assignee_filter_rendered}
                        <label>Статус<select name="status">{status_options_html}</select></label>
                        <label>Search<input type="text" name="search" value="{escape(filter_values.get('search', ''))}" placeholder="Search tasks"></label>
                        <button type="submit" class="btn small-btn">Filter</button>
                        <a href="/tasks" class="ghost-btn small-btn" data-reset-filters="tasks">Reset</a>
                    </form>
                </div>
                {create_block}
            </div>
        </div>
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
        <div>
            <div class="task-stack">{task_cards if task_cards else '<div class="panel">No tasks</div>'}</div>
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
    period_view="current",
    period_label="",
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
    period_view_options = "".join([
        f'<option value="{value}" {"selected" if period_view == value else ""}>{label}</option>'
        for value, label in [("all", "All Time"), ("current", "Current Period"), ("period", "Choose Period")]
    ])
    period_options = make_options(build_period_options(), period_label)
    total_pages = max(1, (int(total_count or 0) + per_page - 1) // per_page)

    rows_html = ""
    for item in rows:
        row = item["row"]
        chat_link = item.get("chat_link") or ""
        chat_link_html = f'<a href="https://{escape(chat_link)}" target="_blank" rel="noreferrer" class="ghost-btn small-btn">Open</a>' if chat_link else "—"
        rows_html += f"""
        <tr>
            <td data-col="report_date">{escape(item.get("report_date") or "")}</td>
            <td data-col="period_label">{escape(item.get("period_label") or "")}</td>
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
        period_view=period_view,
        period_label=period_label,
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
            period_view=period_view,
            period_label=period_label,
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
        ("report_date", "Report Date"),
        ("period_label", "Period"),
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
            const WIDTH_KEY = window.teambeadStorageKey('chatterfy_widths_v1');
            const HIDDEN_KEY = window.teambeadStorageKey('chatterfy_hidden_columns_v1');
            const ORDER_KEY = window.teambeadStorageKey('chatterfy_column_order_v1');
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
        <div class="toolbar-actions">
                <div class="panel compact-panel filters">
                    <form method="get" action="/chatterfy" style="justify-content:flex-end;" data-persist-filters="chatterfy">
                        <label>Date<input type="text" name="date_filter" value="{escape(date_filter)}" placeholder="27.03.2026"></label>
                        <label>Time<input type="text" name="time_filter" value="{escape(time_filter)}" placeholder="09:3"></label>
                        <label>View<select name="period_view">{period_view_options}</select></label>
                        <label>Period<select name="period_label">{period_options}</select></label>
                        <label>Telegram ID<input type="text" name="telegram_id" value="{escape(telegram_id)}" placeholder="5065148172"></label>
                        <label>ID in PP<input type="text" name="pp_player_id" value="{escape(pp_player_id)}" placeholder="1601157577"></label>
                        <label>Status<select name="status">{status_options}</select></label>
                        <label>Search<input type="text" name="search" value="{escape(search)}" placeholder="tags, manager, geo, offer"></label>
                        <input type="hidden" name="page" value="1">
                        <button type="submit" class="btn small-btn">Filter</button>
                        <a href="/chatterfy" class="ghost-btn small-btn" data-reset-filters="chatterfy">Reset</a>
                    </form>
                </div>
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
        </div>
        <div class="table-wrap">
            <table id="chatterfyTable" style="min-width:1900px;">
                <thead>
                    <tr>
                        <th data-col="report_date"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("report_date", "Report Date")}<span class="resizer"></span></div></th>
                        <th data-col="period_label"><div class="th-inner"><span class="drag-handle">⋮⋮</span>{header_link("period_label", "Period")}<span class="resizer"></span></div></th>
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
                <tbody>{rows_html if rows_html else '<tr><td colspan="16">No data</td></tr>'}</tbody>
            </table>
        </div>
        <div style="display:flex; justify-content:space-between; align-items:center; gap:12px; margin-top:14px; flex-wrap:wrap;">
            <div class="user-chip">{len(rows)} / {total_count}</div>
            <div style="display:flex; gap:8px; align-items:center;">
                {f'<a href="{prev_link}" class="ghost-btn small-btn">Prev</a>' if prev_link else '<span class="ghost-btn small-btn" style="opacity:0.45; pointer-events:none;">Prev</span>'}
                <span class="user-chip">Page {page} / {total_pages}</span>
                {f'<a href="{next_link}" class="ghost-btn small-btn">Next</a>' if next_link else '<span class="ghost-btn small-btn" style="opacity:0.45; pointer-events:none;">Next</span>'}
            </div>
        </div>
    </div>
    """
    return page_shell("Chatterfy", content, active_page="chatterfy", current_user=current_user, extra_scripts=extra_scripts)


def hold_wager_page_html(current_user, rows, cabinet_name="", period_view="current", period_label="", search="", success_text="", error_text=""):
    all_cabinets = get_partner_cabinet_options(period_label if period_view == "period" else "")
    cabinet_options = make_options(all_cabinets, cabinet_name)
    period_view_options = "".join([
        f'<option value="{value}" {"selected" if period_view == value else ""}>{label}</option>'
        for value, label in [("all", "All Time"), ("current", "Current Period"), ("period", "Choose Period")]
    ])
    period_options = make_options(build_period_options(), period_label)

    total_players = len(rows)
    baseline_fails = sum(1 for item in rows if "Baseline" in item["reason"])
    wager_fails = sum(1 for item in rows if "Wager" in item["reason"])

    rows_html = ""
    for item in rows:
        chat_link = item.get("chat_link") or ""
        chat_html = f'<a href="https://{escape(chat_link)}" target="_blank" rel="noreferrer" class="ghost-btn small-btn">Open</a>' if chat_link else "—"
        chatter_status_html = render_chatterfy_status_icon(item.get("chatter_status"))
        rows_html += f"""
        <tr>
            <td>{escape(item['report_date'])}</td>
            <td>{escape(item['period_label'])}</td>
            <td>{escape(item['registration_date'])}</td>
            <td>{escape(item['cabinet_name'])}</td>
            <td>{escape(item['sub_id'])}</td>
            <td>{escape(item['player_id'])}</td>
            <td>{escape(item.get('telegram_id') or '')}</td>
            <td>{escape(item.get('pp_player_id') or '')}</td>
            <td>{chat_html}</td>
            <td>{chatter_status_html}</td>
            <td>{escape(item['country'])}</td>
            <td>{escape(item['flow'])}</td>
            <td>{format_plain_number_text(item['baseline'])}</td>
            <td>{format_plain_number_text(item['rate'])}</td>
            <td>{format_money(item['deposit_amount'])}</td>
            <td>{format_money(item['bet_amount'])}</td>
            <td>{escape(item['reason'])}</td>
            <td>{format_money(item['missing_baseline'])}</td>
            <td>{format_money(item['missing_wager'])}</td>
        </tr>
        """

    message_html = ""
    if success_text:
        message_html += f'<div class="notice">{escape(success_text)}</div>'
    if error_text:
        message_html += f'<div class="notice notice-danger">{escape(error_text)}</div>'

    content = f"""
    {message_html}
    <div class="panel compact-panel">
        <div class="toolbar-actions">
            <div class="panel compact-panel filters">
                <form method="get" action="/hold-wager" data-persist-filters="hold-wager">
                    <label>View<select name="period_view">{period_view_options}</select></label>
                    <label>Period<select name="period_label">{period_options}</select></label>
                    <label>Cabinet<select name="cabinet_name">{cabinet_options}</select></label>
                    <label>Search<input type="text" name="search" value="{escape(search)}" placeholder="subid, player id, country"></label>
                    <button type="submit" class="btn small-btn">Filter</button>
                    <a href="/hold-wager" class="ghost-btn small-btn" data-reset-filters="hold-wager">Reset</a>
                </form>
            </div>
            <div class="caps-toolbar-stats">
                <div class="mini-stat"><div class="name">players</div><div class="value">{total_players}</div></div>
                <div class="mini-stat"><div class="name">baseline fail</div><div class="value">{baseline_fails}</div></div>
                <div class="mini-stat"><div class="name">wager fail</div><div class="value">{wager_fails}</div></div>
            </div>
        </div>
    </div>

    <div class="panel compact-panel">
        <div class="table-wrap">
            <table style="min-width:1600px;">
                <thead>
                    <tr>
                        <th>Report Date</th>
                        <th>Period</th>
                        <th>Reg Date</th>
                        <th>Cabinet</th>
                        <th>Promo/Link</th>
                        <th>Player ID</th>
                        <th>ID Chatterfy</th>
                        <th>ID in PP</th>
                        <th>Chat</th>
                        <th>Status</th>
                        <th>Country</th>
                        <th>Flow</th>
                        <th>Baseline</th>
                        <th>Rate</th>
                        <th>Deposit</th>
                        <th>Bet</th>
                        <th>Reason</th>
                        <th>Missing BL</th>
                        <th>Missing WG</th>
                    </tr>
                </thead>
                <tbody>{rows_html if rows_html else '<tr><td colspan="19">No hold/wager issues for the selected filters.</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    """
    return page_shell("Hold", content, active_page="holdwager", current_user=current_user)


def cabinets_page_html(current_user, rows, filter_values=None, form_data=None, success_text="", error_text=""):
    filter_values = filter_values or {}
    form_data = form_data or {}
    open_attr = "open" if form_data.get("edit_id") or form_data.get("name") else ""

    rows_html = ""
    for row in rows:
        rows_html += f"""
        <tr>
            <td class="advertiser-col">{escape(row.advertiser or "")}</td>
            <td class="platform-col">{escape(row.platform or "")}</td>
            <td class="cabinet-col">{escape(row.name or "")}</td>
            <td class="code-col">{escape(format_geo_list_codes(row.geo_list or ""))}</td>
            <td class="geo-col">{escape(format_geo_list_names(row.geo_list or ""))}</td>
            <td class="brands-col">{escape(row.brands or "")}</td>
            <td class="tg-col">{escape(row.team_name or "")}</td>
            <td class="manager-col">{escape(row.manager_name or "")}</td>
            <td class="manager-contact-col">{escape(row.manager_contact or "")}</td>
            <td class="chat-name-col">{escape(getattr(row, 'chat_name', '') or "")}</td>
            <td class="wallet-col">{escape(row.wallet or "")}</td>
            <td class="comments-col">{escape(row.comments or "")}</td>
            <td class="actions-col">
                <div class="table-icon-actions">
                    <form method="get" action="/cabinets">
                        <input type="hidden" name="edit" value="{row.id}">
                        <input type="hidden" name="search" value="{escape(filter_values.get('search', ''))}">
                        <button type="submit" class="ghost-btn small-btn action-icon-btn" aria-label="Edit" title="Edit">✏</button>
                    </form>
                    <form method="post" action="/cabinets/delete" onsubmit="return confirm('Delete this cabinet?');">
                        <input type="hidden" name="cabinet_id" value="{row.id}">
                        <input type="hidden" name="search" value="{escape(filter_values.get('search', ''))}">
                        <button type="submit" class="ghost-btn small-btn action-icon-btn" aria-label="Delete" title="Delete">🗑</button>
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

    content = f"""
    {message_html}

    <div class="panel compact-panel">
        <div class="partners-toolbar">
                <div class="panel compact-panel filters">
                    <form method="get" action="/cabinets" data-persist-filters="cabinets">
                        <label>Search<input type="text" name="search" value="{escape(filter_values.get('search', ''))}" placeholder=""></label>
                        <div class="filter-actions-stack">
                            <button type="submit" class="btn small-btn">Filter</button>
                            <a href="/cabinets" class="ghost-btn small-btn filter-reset-btn" data-reset-filters="cabinets" aria-label="Reset filters" title="Reset filters">×</a>
                        </div>
                    </form>
                </div>
                <details class="upload-menu" {open_attr}>
                    <summary class="btn toggle-indicator"></summary>
                    <div class="upload-menu-list" style="width:520px; max-width:min(520px, calc(100vw - 48px));">
                        <form method="post" action="/cabinets/save">
                            <input type="hidden" name="edit_id" value="{escape(form_data.get('edit_id', ''))}">
                            <input type="hidden" name="search" value="{escape(filter_values.get('search', ''))}">
                            <label>Advertiser
                                <input type="text" name="advertiser" value="{escape(form_data.get('advertiser', ''))}" placeholder="Example: 1xBet">
                            </label>
                            <label>Platform
                                <input type="text" name="platform" value="{escape(form_data.get('platform', ''))}" placeholder="Example: Facebook / Google / Native">
                            </label>
                            <label>Cabinet
                                <input type="text" name="name" value="{escape(form_data.get('name', ''))}" required placeholder="Example: 1xBet Main 01">
                            </label>
                            <label>Code
                                <input type="text" name="geo_list" value="{escape(form_data.get('geo_list', ''))}" placeholder="Example: PE, CO, CL">
                            </label>
                            <label>Geo
                                <input type="text" value="{escape(format_geo_list_names(form_data.get('geo_list', '')))}" readonly>
                            </label>
                            <label>Brands
                                <input type="text" name="brands" value="{escape(form_data.get('brands', ''))}" placeholder="Example: 1xBet, Mostbet">
                            </label>
                            <label>TG
                                <input type="text" name="team_name" value="{escape(form_data.get('team_name', ''))}" placeholder="Example: Sales Team / Telegram Team">
                            </label>
                            <label>Manager
                                <input type="text" name="manager_name" value="{escape(form_data.get('manager_name', ''))}" placeholder="Example: Maria">
                            </label>
                            <label>Manager Contact
                                <input type="text" name="manager_contact" value="{escape(form_data.get('manager_contact', ''))}" placeholder="@manager or phone">
                            </label>
                            <label>Chat Name
                                <input type="text" name="chat_name" value="{escape(form_data.get('chat_name', ''))}" placeholder="Example: 1x Main Chat">
                            </label>
                            <label>Wallet
                                <textarea name="wallet" placeholder="TRC20 wallet, notes or several wallets">{escape(form_data.get('wallet', ''))}</textarea>
                            </label>
                            <label>Comments
                                <textarea name="comments" placeholder="Anything important about this cabinet">{escape(form_data.get('comments', ''))}</textarea>
                            </label>
                            <div style="display:flex; gap:10px; justify-content:flex-end;">
                                <a href="/cabinets" class="ghost-btn small-btn">Reset</a>
                                <button type="submit" class="btn small-btn">Save</button>
                            </div>
                        </form>
                    </div>
                </details>
        </div>
        <div class="table-wrap">
            <table class="partners-table">
                <thead>
                    <tr>
                        <th class="advertiser-col" style="text-align:center;">Advertiser</th>
                        <th class="platform-col" style="text-align:center;">Platform</th>
                        <th class="cabinet-col" style="text-align:center;">Cabinet</th>
                        <th class="code-col" style="text-align:center;">Code</th>
                        <th class="geo-col" style="text-align:center;">Geo</th>
                        <th class="brands-col" style="text-align:center;">Brands</th>
                        <th class="tg-col" style="text-align:center;">TG</th>
                        <th class="manager-col" style="text-align:center;">Manager</th>
                        <th class="manager-contact-col" style="text-align:center;">Manager Contact</th>
                        <th class="chat-name-col" style="text-align:center;">Chat Name</th>
                        <th class="wallet-col" style="text-align:center;">Wallet</th>
                        <th class="comments-col" style="text-align:center;">Comments</th>
                        <th class="actions-col" style="text-align:center;">Actions</th>
                    </tr>
                </thead>
                <tbody>{rows_html if rows_html else '<tr><td colspan="13">No partners yet</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    """
    return page_shell("Partners", content, active_page="cabinets", current_user=current_user)


def partner_report_page_html(
    current_user,
    rows,
    upload_summaries=None,
    source_name="",
    cabinet_name="",
    upload_platform="1xbet",
    country="",
    search="",
    period_view="current",
    period_label="",
    sort_by="id",
    order="desc",
    success_text="",
    error_text="",
):
    upload_summaries = upload_summaries or []
    all_sources = get_partner_period_options()
    all_cabinets = get_partner_cabinet_options(period_label if period_view == "period" else "")
    upload_cabinet_catalog = get_partner_upload_cabinet_catalog()
    upload_cabinets = [item["cabinet_name"] for item in upload_cabinet_catalog] or get_partner_cabinet_options() or all_cabinets
    all_countries = get_partner_country_options()
    period_view_options = "".join([
        f'<option value="{value}" {"selected" if period_view == value else ""}>{label}</option>'
        for value, label in [("all", "All Time"), ("current", "Current Period"), ("period", "Choose Period")]
    ])
    period_options = make_options(build_period_options(), period_label)
    source_options = make_options(all_sources, source_name)
    cabinet_options = make_options(all_cabinets, cabinet_name)
    upload_platform_options = "".join([
        f'<option value="{escape(value)}" {"selected" if safe_text(upload_platform).strip().lower() == safe_text(value).strip().lower() else ""}>{escape(label)}</option>'
        for value, label in get_partner_platform_options()
    ])
    upload_cabinet_options = "".join([
        f'<option value="{escape(item["cabinet_name"])}" data-platform="{escape(item["platform_name"])}">{escape(item["cabinet_name"])}</option>'
        for item in upload_cabinet_catalog
    ]) or '<option value="">No cabinets yet</option>'
    country_options = make_options(all_countries, country)
    totals = aggregate_partner_totals(rows)

    rows_html = ""
    for row in rows:
        rows_html += f"""
        <tr>
            <td>{escape(getattr(row, "brand_name", "") or "")}</td>
            <td>{escape(row.cabinet_name or "")}</td>
            <td>{escape(getattr(row, "geo_name", "") or "")}</td>
            <td>{escape(row.registration_date or "")}</td>
            <td>{escape(row.player_id or "")}</td>
            <td>${safe_number(row.deposit_amount):,.2f}</td>
            <td>${safe_number(row.bet_amount):,.2f}</td>
            <td>${safe_number(row.company_income):,.2f}</td>
            <td>{escape(row.sub_id or "")}</td>
            <td>${safe_number(row.cpa_amount):,.2f}</td>
        </tr>
        """

    message_html = ""
    if success_text:
        message_html += f'<div class="notice">{escape(success_text)}</div>'
    if error_text:
        message_html += f'<div class="notice notice-danger">{escape(error_text)}</div>'

    delete_upload_rows = ""
    for item in upload_summaries:
        delete_upload_rows += f"""
        <tr>
            <td>{escape(item.get('cabinet_name') or '—')}</td>
            <td>{escape(item.get('platform_label') or '—')}</td>
            <td>{escape(item.get('period_label') or '—')}</td>
            <td>{escape(item.get('date_start') or '—')}</td>
            <td>{escape(item.get('date_end') or '—')}</td>
            <td>{format_int_or_float(item.get('rows_count', 0))}</td>
            <td>{format_int_or_float(item.get('ftd_count', 0))}</td>
            <td style="white-space:nowrap;">
                <form method="post" action="/partner-report/delete-upload">
                    <input type="hidden" name="source_name" value="{escape(item.get('source_name', ''))}">
                    <input type="hidden" name="period_view" value="{escape(period_view)}">
                    <input type="hidden" name="period_label" value="{escape(period_label)}">
                    <input type="hidden" name="cabinet_name" value="{escape(cabinet_name)}">
                    <input type="hidden" name="country" value="{escape(country)}">
                    <input type="hidden" name="search" value="{escape(search)}">
                    <input type="hidden" name="sort_by" value="{escape(sort_by)}">
                    <input type="hidden" name="order" value="{escape(order)}">
                    <button type="button" class="ghost-btn small-btn players-delete-upload-trigger">Delete</button>
                </form>
            </td>
        </tr>
        """

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
            source_name=source_name,
            period_view=period_view,
            period_label=period_label,
            cabinet_name=cabinet_name,
            country=country,
            search=search,
            sort_by=field,
            order=next_order,
        )
        return f'<a href="/partner-report?{qs}">{escape(label)}{arrow}</a>'

    content = f"""
    {message_html}
    {render_active_period_banner(period_label)}

    <div class="panel compact-panel">
        <div class="toolbar-actions players-toolbar">
                <div class="panel compact-panel filters">
                    <form method="get" action="/partner-report" class="players-filter-form" data-persist-filters="partner-report">
                        <input type="hidden" name="period_view" value="period">
                        <input type="hidden" name="source_name" value="{escape(source_name)}">
                        <label class="period-label">Period<select name="period_label">{period_options}</select></label>
                        <label class="cabinet-label">Cabinet<select name="cabinet_name">{cabinet_options}</select></label>
                        <label class="country-label">Country<select name="country">{country_options}</select></label>
                        <label class="search-label">Search<input type="text" name="search" value="{escape(search)}" placeholder=""></label>
                        <button type="submit" class="btn small-btn">Filter</button>
                        <a href="/partner-report" class="ghost-btn small-btn" data-reset-filters="partner-report">×</a>
                    </form>
                </div>
                <div class="caps-toolbar-stats">
                    <div class="mini-stat"><div class="name">ftd</div><div class="value">{totals['players']}</div></div>
                    <div class="mini-stat"><div class="name">qftd</div><div class="value">{totals['qualified_ftd_count']}</div></div>
                    <div class="mini-stat"><div class="name">deposit sum</div><div class="value">${totals['deposits']:,.2f}</div></div>
                    <div class="mini-stat"><div class="name">ngr</div><div class="value">${totals['income']:,.2f}</div></div>
                </div>
                <div class="players-side-tools">
                <div class="players-icon-stack">
                <details class="upload-menu upload-menu-right" style="z-index:90;">
                    <summary class="btn toggle-indicator toolbar-square-trigger" aria-label="Upload players" title="Upload players"></summary>
                    <div class="upload-menu-list" style="width:380px; max-width:min(380px, calc(100vw - 48px));">
                        <form method="post" action="/partner-report/upload" enctype="multipart/form-data">
                            <label>Platform
                                <select name="partner_platform" id="partner-upload-platform" required>{upload_platform_options}</select>
                            </label>
                            <label>Cabinet
                                <select name="cabinet_name" id="partner-upload-cabinet" required>{upload_cabinet_options}</select>
                            </label>
                            <label>Partner File
                                <input type="file" name="file" accept=".csv,.xlsx,.xls" required>
                            </label>
                            <button type="submit" class="btn small-btn">Upload</button>
                        </form>
                    </div>
                </details>
                <details class="upload-menu upload-menu-right" style="z-index:89;">
                    <summary class="ghost-btn small-btn toolbar-square-icon-btn" aria-label="Delete upload" title="Delete upload">🗑</summary>
                    <div class="upload-menu-list" style="width:min(860px, calc(100vw - 48px));">
                        <div class="panel-subtitle">Choose the exact cabinet and period upload you want to remove.</div>
                        <div class="table-wrap" style="margin-top:8px;">
                            <table style="min-width:780px;">
                                <thead>
                                    <tr>
                                        <th>Cabinet</th>
                                        <th>Platform</th>
                                        <th>Period</th>
                                        <th>From</th>
                                        <th>To</th>
                                        <th>Rows</th>
                                        <th>FTD</th>
                                        <th>Action</th>
                                    </tr>
                                </thead>
                                <tbody>{delete_upload_rows if delete_upload_rows else '<tr><td colspan="8">No uploads yet</td></tr>'}</tbody>
                            </table>
                        </div>
                    </div>
                </details>
                </div>
                <form method="get" action="/partner-report" class="players-upload-filter">
                    <input type="hidden" name="period_view" value="period">
                    <input type="hidden" name="period_label" value="{escape(period_label)}">
                    <input type="hidden" name="cabinet_name" value="{escape(cabinet_name)}">
                    <input type="hidden" name="country" value="{escape(country)}">
                    <input type="hidden" name="search" value="{escape(search)}">
                    <label>Upload<select name="source_name" onchange="this.form.submit()">{source_options}</select></label>
                </form>
                </div>
        </div>
    </div>

    <div class="panel compact-panel">
        <div class="table-wrap">
            <table class="players-report-table" style="min-width:1500px;">
                <thead>
                    <tr>
                        <th>{header_link('brand_name', 'Brands')}</th>
                        <th>{header_link('cabinet_name', 'Cabinet')}</th>
                        <th>{header_link('geo_name', 'Geo')}</th>
                        <th>{header_link('registration_date', 'Registration')}</th>
                        <th>{header_link('player_id', 'ID')}</th>
                        <th>{header_link('deposit_amount', 'Deposit Sum')}</th>
                        <th>{header_link('bet_amount', 'Bet Sum')}</th>
                        <th>{header_link('company_income', 'NGR')}</th>
                        <th>{header_link('sub_id', 'subid')}</th>
                        <th>{header_link('cpa_amount', 'CPA')}</th>
                    </tr>
                </thead>
                <tbody>{rows_html if rows_html else '<tr><td colspan="10">No partner rows yet</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    <div class="confirm-overlay" id="playersDeleteUploadOverlay" aria-hidden="true">
        <div class="confirm-card">
            <div class="confirm-title">Delete upload?</div>
            <div class="confirm-text">This action will remove the selected cabinet period upload from Players. You can cancel if you opened it by mistake.</div>
            <div class="confirm-actions">
                <button type="button" class="ghost-btn" id="playersDeleteUploadCancel">Cancel</button>
                <button type="button" class="btn danger-btn" id="playersDeleteUploadConfirm">Delete</button>
            </div>
        </div>
    </div>

    <script>
    (() => {{
        const deleteOverlay = document.getElementById("playersDeleteUploadOverlay");
        const deleteCancel = document.getElementById("playersDeleteUploadCancel");
        const deleteConfirm = document.getElementById("playersDeleteUploadConfirm");
        let activeDeleteForm = null;
        function closeDeleteModal() {{
            if (!deleteOverlay) return;
            deleteOverlay.classList.remove("open");
            deleteOverlay.setAttribute("aria-hidden", "true");
            activeDeleteForm = null;
        }}
        document.querySelectorAll(".players-delete-upload-trigger").forEach((button) => {{
            button.addEventListener("click", () => {{
                activeDeleteForm = button.closest("form");
                if (!deleteOverlay) return;
                deleteOverlay.classList.add("open");
                deleteOverlay.setAttribute("aria-hidden", "false");
            }});
        }});
        deleteCancel?.addEventListener("click", closeDeleteModal);
        deleteConfirm?.addEventListener("click", () => {{
            if (activeDeleteForm) activeDeleteForm.submit();
        }});
        deleteOverlay?.addEventListener("click", (event) => {{
            if (event.target === deleteOverlay) closeDeleteModal();
        }});
        document.addEventListener("keydown", (event) => {{
            if (event.key === "Escape" && deleteOverlay?.classList.contains("open")) closeDeleteModal();
        }});

        const platformSelect = document.getElementById("partner-upload-platform");
        const cabinetSelect = document.getElementById("partner-upload-cabinet");
        if (!platformSelect || !cabinetSelect) return;
        const allOptions = Array.from(cabinetSelect.querySelectorAll("option")).map((option) => ({{
            value: option.value,
            label: option.textContent,
            platform: option.dataset.platform || "",
        }}));
        const rebuildCabinetOptions = () => {{
            const selectedPlatform = (platformSelect.value || "").trim().toLowerCase();
            const currentValue = cabinetSelect.value;
            const filtered = allOptions.filter((item) => !selectedPlatform || (item.platform || "").trim().toLowerCase() === selectedPlatform);
            cabinetSelect.innerHTML = "";
            if (!filtered.length) {{
                const emptyOption = document.createElement("option");
                emptyOption.value = "";
                emptyOption.textContent = "No cabinets yet";
                cabinetSelect.appendChild(emptyOption);
                return;
            }}
            filtered.forEach((item, index) => {{
                const option = document.createElement("option");
                option.value = item.value;
                option.textContent = item.label;
                option.dataset.platform = item.platform;
                if (item.value === currentValue || (!currentValue && index === 0)) {{
                    option.selected = true;
                }}
                cabinetSelect.appendChild(option);
            }});
        }};
        platformSelect.addEventListener("change", rebuildCabinetOptions);
        rebuildCabinetOptions();
    }})();
    </script>
    """
    return page_shell("Report Players", content, active_page="partner", current_user=current_user)


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
    status_filter: str = Form(default=""),
    assignee_filter: str = Form(default=""),
    search_filter: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin", "admin")
    ensure_task_table()

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

    return RedirectResponse(
        url=(
            f"/tasks?status={quote_plus(safe_text(status_filter))}"
            f"&assignee={quote_plus(safe_text(assignee_filter))}"
            f"&search={quote_plus(safe_text(search_filter))}"
            f"&message=Task+created"
        ),
        status_code=303,
    )


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
def delete_task(
    request: Request,
    task_id: str = Form(...),
    status_filter: str = Form(default=""),
    assignee_filter: str = Form(default=""),
    search_filter: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin", "admin")
    ensure_task_table()
    db = SessionLocal()
    try:
        db.query(TaskRow).filter(TaskRow.id == safe_number(task_id)).delete()
        db.commit()
    finally:
        db.close()
    return RedirectResponse(
        url=(
            f"/tasks?status={quote_plus(safe_text(status_filter))}"
            f"&assignee={quote_plus(safe_text(assignee_filter))}"
            f"&search={quote_plus(safe_text(search_filter))}"
            f"&message=Task+deleted"
        ),
        status_code=303,
    )


@app.post("/tasks/respond")
def respond_task(
    request: Request,
    task_id: str = Form(...),
    status: str = Form(...),
    response_text: str = Form(default=""),
    status_filter: str = Form(default=""),
    assignee_filter: str = Form(default=""),
    search_filter: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "tasks")
    ensure_task_table()
    if status not in get_task_status_options():
        return RedirectResponse(
            url=(
                f"/tasks?status={quote_plus(safe_text(status_filter))}"
                f"&assignee={quote_plus(safe_text(assignee_filter))}"
                f"&search={quote_plus(safe_text(search_filter))}"
                f"&message=Неизвестный+статус"
            ),
            status_code=303,
        )

    db = SessionLocal()
    try:
        task = db.query(TaskRow).filter(TaskRow.id == safe_number(task_id)).first()
        if not task:
            return RedirectResponse(
                url=(
                    f"/tasks?status={quote_plus(safe_text(status_filter))}"
                    f"&assignee={quote_plus(safe_text(assignee_filter))}"
                    f"&search={quote_plus(safe_text(search_filter))}"
                    f"&message=Task+not+found"
                ),
                status_code=303,
            )
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

    return RedirectResponse(
        url=(
            f"/tasks?status={quote_plus(safe_text(status_filter))}"
            f"&assignee={quote_plus(safe_text(assignee_filter))}"
            f"&search={quote_plus(safe_text(search_filter))}"
            f"&message=%D0%9E%D1%82%D0%B2%D0%B5%D1%82+%D1%81%D0%BE%D1%85%D1%80%D0%B0%D0%BD%D0%B5%D0%BD"
        ),
        status_code=303,
    )


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
async def upload_partner_file(request: Request, file: UploadFile = File(...), cabinet_name: str = Form(default="")):
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
        df = read_partner_uploaded_dataframe(filename, ext)
        clean_cabinet_name = safe_text(cabinet_name)
        detected_period = detect_partner_upload_period(df, partner_platform="1xbet", fallback_text=original_name)
        if not detected_period:
            return HTMLResponse(partner_report_page_html(user, get_partner_rows_by_period(""), error_text="Could not determine upload period from the file header or filename."), status_code=400)
        source_name = original_name if not clean_cabinet_name else build_partner_source_name(
            detected_period["date_start"],
            detected_period["date_end"],
            prefix=clean_cabinet_name.replace("|", "/"),
        )
        replace_partner_rows(
            source_name,
            parse_partner_dataframe(
                df,
                source_name=source_name,
                cabinet_name=clean_cabinet_name,
                upload_period_data=detected_period,
            ),
        )
        return RedirectResponse(url="/partner-report?message=Upload+saved", status_code=303)
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
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin", "admin")
    buyer = resolve_effective_buyer(user, buyer)
    effective_period_label = resolve_period_label(period_view, period_label) or get_current_period_label()
    rows = enrich_statistic_rows(
        aggregate_grouped_rows(get_filtered_data(buyer, manager, geo, offer, search, period_label=effective_period_label)),
        period_label=effective_period_label,
    )
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
        <details class="upload-menu upload-menu-right">
            <summary class="btn toggle-indicator"></summary>
            <div class="upload-menu-list" style="width:360px; max-width:min(360px, calc(100vw - 48px));">
                <form method="post" action="/upload" enctype="multipart/form-data">
                    <label>Buyer
                        <input type="text" name="buyer" required placeholder="Example: TeamBead1">
                    </label>
                    <label>CSV / XLSX
                        <input type="file" name="file" accept=".csv,.xlsx,.xls" required>
                    </label>
                    <button type="submit" class="btn small-btn">Upload</button>
                </form>
            </div>
        </details>
        '''

    content = f'''
    <div class="panel compact-panel">
        <div class="toolbar-actions">
            <div class="panel compact-panel filters">
                <form method="get" action="/grouped" data-persist-filters="grouped">
                    {'<label>Buyer<select name="buyer">' + buyer_options + '</select></label>' if is_admin_role(user) or user.get("role") == "operator" else ''}
                    <label>Manager<select name="manager">{manager_options}</select></label>
                    <label>Geo<select name="geo">{geo_options}</select></label>
                    <label>Offer<select name="offer">{offer_options}</select></label>
                    <label>Search<input type="text" name="search" value="{escape(search)}" placeholder="Search rows"></label>
                    <input type="hidden" name="sort_by" value="{escape(sort_by)}">
                    <input type="hidden" name="order" value="{escape(order)}">
                    <button type="submit" class="btn small-btn">Filter</button>
                    <a href="/grouped" class="ghost-btn small-btn" data-reset-filters="grouped">Reset</a>
                </form>
            </div>
            {upload_block}
            <div class="column-menu-wrap">
                <button type="button" class="ghost-btn small-btn" onclick="toggleColumnMenu()">⚙️ Columns</button>
                <div class="column-menu" id="columnMenu">
                    <div class="column-actions">
                        <button type="button" class="ghost-btn small-btn" onclick="showAllColumns()">Show All</button>
                        <button type="button" class="ghost-btn small-btn" onclick="resetColumnsAll()">Reset All</button>
                    </div>
                    <div class="column-grid">{column_chips}</div>
                </div>
            </div>
        </div>
    </div>

    {render_stats_cards(totals)}

    <div class="panel compact-panel">
        <div class="table-wrap">
            <table id="groupedTable">
                <thead><tr>{head_html}</tr></thead>
                <tbody>{rows_html if rows_html else '<tr><td colspan="19">No data</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    '''

    extra_scripts = """
    <script>
        const HIDDEN_KEY = window.teambeadStorageKey('hidden-columns-v2');
        const ORDER_KEY = window.teambeadStorageKey('column-order-v2');
        const WIDTH_KEY = window.teambeadStorageKey('column-widths-v2');

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
def render_dashboard_page(
    request: Request,
    buyer: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "hierarchy")
    buyer = resolve_effective_buyer(user, safe_text(buyer))
    manager = safe_text(manager)
    geo = safe_text(geo)
    offer = safe_text(offer)
    search = safe_text(search)
    period_view = safe_text(period_view) or "current"
    period_label = safe_text(period_label)
    effective_period_label = resolve_period_label(period_view, period_label) or get_current_period_label()
    data = get_filtered_data(buyer, manager, geo, offer, search, period_label=effective_period_label)
    rows = enrich_statistic_rows(aggregate_grouped_rows(data), period_label=effective_period_label)
    all_buyers, all_managers, all_geos, all_offers = get_scoped_filter_options(user, period_label=effective_period_label)
    period_view_options = "".join([
        f'<option value="{value}" {"selected" if period_view == value else ""}>{label}</option>'
        for value, label in [("all", "All Time"), ("current", "Current Period"), ("period", "Choose Period")]
    ])
    period_options = make_options(build_period_options(), effective_period_label)

    buyer_options = make_options(all_buyers, buyer) if is_admin_role(user) or user.get("role") == "operator" else f'<option value="{escape(buyer)}">{escape(buyer or "Мой buyer")}</option>'
    manager_options = make_options(all_managers, manager)
    geo_options = make_options(all_geos, geo)
    offer_options = make_options(all_offers, offer)
    totals = aggregate_totals(rows)
    overview = build_dashboard_overview(
        user,
        rows,
        buyer=buyer,
        manager=manager,
        geo=geo,
        offer=offer,
        search=search,
        period_label=effective_period_label,
    )

    export_qs = build_query_string(buyer=buyer, manager=manager, geo=geo, offer=offer, search=search, period_view=period_view, period_label=effective_period_label)
    export_link = f"/export/hierarchy?{export_qs}" if export_qs else "/export/hierarchy"

    content = f'''
    {render_active_period_banner(effective_period_label)}

    <div class="panel compact-panel">
        <div class="toolbar-actions">
            <div class="panel compact-panel filters">
                <form method="get" action="/dashboard" data-persist-filters="dashboard">
                    {'<label>Buyer<select name="buyer">' + buyer_options + '</select></label>' if is_admin_role(user) or user.get("role") == "operator" else ''}
                    <label>Manager<select name="manager">{manager_options}</select></label>
                    <label>Geo<select name="geo">{geo_options}</select></label>
                    <label>Offer<select name="offer">{offer_options}</select></label>
                    <label>View<select name="period_view">{period_view_options}</select></label>
                    <label>Period<select name="period_label">{period_options}</select></label>
                    <label>Search<input type="text" name="search" value="{escape(search)}"></label>
                    <button type="submit" class="btn small-btn">Filter</button>
                    <a href="/dashboard" class="ghost-btn small-btn" data-reset-filters="dashboard">Reset</a>
                </form>
            </div>
        </div>
    </div>

    {render_statistic_cards(totals)}

    {render_statistic_dashboard(rows)}

    {render_dashboard_overview(overview)}
    '''

    top_actions = f'<a class="small-btn" href="{export_link}">⬇ CSV</a>' if is_admin_role(user) else ""
    return page_shell("Dashboard", content, "dashboard", top_actions=top_actions, current_user=user)


@app.get("/hierarchy", response_class=HTMLResponse)
def show_hierarchy(
    request: Request,
    buyer: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
    period_view: str = Query(default="all"),
    period_label: str = Query(default=""),
):
    return render_dashboard_page(request, buyer, manager, geo, offer, search, period_view, period_label)


@app.get("/dashboard", response_class=HTMLResponse)
def show_dashboard(
    request: Request,
    buyer: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
    period_view: str = Query(default="all"),
    period_label: str = Query(default=""),
):
    return render_dashboard_page(request, buyer, manager, geo, offer, search, period_view, period_label)


# =========================================
# BLOCK 12 — PLACEHOLDERS
# =========================================
@app.get("/finance", response_class=HTMLResponse)
def finance_page(
    request: Request,
    message: str = Query(default=""),
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
    year: str = Query(default=""),
    edit_wallet: str = Query(default=""),
    edit_expense: str = Query(default=""),
    edit_income: str = Query(default=""),
    edit_transfer: str = Query(default=""),
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
        transfer_item = db.query(FinanceTransferRow).filter(FinanceTransferRow.id == safe_number(edit_transfer)).first() if edit_transfer else None
        form_data = build_finance_form_data(wallet_item=wallet_item, expense_item=expense_item, income_item=income_item, transfer_item=transfer_item)
    finally:
        db.close()
    return finance_page_html(
        user,
        success_text=message,
        form_data=form_data,
        filter_values={
            "period_view": period_view,
            "period_label": period_label,
            "date_from": date_from,
            "date_to": date_to,
            "year": year,
        },
    )


@app.post("/finance/wallets/save")
def save_finance_wallet(
    request: Request,
    edit_id: str = Form(default=""),
    category: str = Form(default=""),
    description: str = Form(default=""),
    owner_name: str = Form(default=""),
    wallet: str = Form(default=""),
    amount: str = Form(default="0"),
    date_from: str = Form(default=""),
    date_to: str = Form(default=""),
    year: str = Form(default=""),
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
    clear_runtime_cache("finance_snapshot::")
    return RedirectResponse(url=f"/finance?date_from={quote_plus(safe_text(date_from))}&date_to={quote_plus(safe_text(date_to))}&year={quote_plus(safe_text(year))}&message=%D0%9A%D0%BE%D1%88%D0%B5%D0%BB%D0%B5%D0%BA+%D1%81%D0%BE%D1%85%D1%80%D0%B0%D0%BD%D0%B5%D0%BD", status_code=303)


@app.post("/finance/upload")
async def upload_finance_file(request: Request, file: UploadFile = File(...)):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_upload_dir()
    with open(FINANCE_UPLOAD_PATH, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    clear_runtime_cache("finance_snapshot::")
    return RedirectResponse(url="/finance?message=Финансы загружены", status_code=303)


@app.post("/finance/expenses/save")
def save_finance_expense(
    request: Request,
    edit_id: str = Form(default=""),
    expense_date: str = Form(default=""),
    category: str = Form(default=""),
    wallet_name: str = Form(default=""),
    amount: str = Form(default="0"),
    from_wallet: str = Form(default=""),
    paid_by: str = Form(default=""),
    comment: str = Form(default=""),
    date_from: str = Form(default=""),
    date_to: str = Form(default=""),
    year: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_finance_tables()
    form_data = {
        "expense_date": expense_date,
        "expense_category": category,
        "expense_wallet_name": wallet_name,
        "expense_amount": amount,
        "expense_from_wallet": from_wallet or paid_by,
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
        item.wallet_name = safe_text(wallet_name)
        item.amount = safe_cap_number(amount)
        item.from_wallet = safe_text(from_wallet or paid_by)
        item.paid_by = safe_text(from_wallet or paid_by)
        item.comment = safe_text(comment)
        db.commit()
    finally:
        db.close()
    clear_runtime_cache("finance_snapshot::")
    return RedirectResponse(url=f"/finance?date_from={quote_plus(safe_text(date_from))}&date_to={quote_plus(safe_text(date_to))}&year={quote_plus(safe_text(year))}&message=%D0%A0%D0%B0%D1%81%D1%85%D0%BE%D0%B4+%D1%81%D0%BE%D1%85%D1%80%D0%B0%D0%BD%D0%B5%D0%BD", status_code=303)


@app.post("/finance/income/save")
def save_finance_income(
    request: Request,
    edit_id: str = Form(default=""),
    income_date: str = Form(default=""),
    category: str = Form(default=""),
    wallet_name: str = Form(default=""),
    amount: str = Form(default="0"),
    from_wallet: str = Form(default=""),
    comment: str = Form(default=""),
    date_from: str = Form(default=""),
    date_to: str = Form(default=""),
    year: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_finance_tables()
    form_data = {
        "income_date": income_date,
        "income_category": category,
        "income_wallet_name": wallet_name,
        "income_amount": amount,
        "income_from_wallet": from_wallet,
        "income_comment": comment,
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
        item.wallet_name = safe_text(wallet_name)
        item.description = safe_text(comment)
        item.amount = safe_cap_number(amount)
        item.wallet = safe_text(wallet_name)
        item.from_wallet = safe_text(from_wallet)
        item.comment = safe_text(comment)
        item.reconciliation = safe_text(from_wallet)
        db.commit()
    finally:
        db.close()
    clear_runtime_cache("finance_snapshot::")
    return RedirectResponse(url=f"/finance?date_from={quote_plus(safe_text(date_from))}&date_to={quote_plus(safe_text(date_to))}&year={quote_plus(safe_text(year))}&message=%D0%9F%D1%80%D0%B8%D1%85%D0%BE%D0%B4+%D1%81%D0%BE%D1%85%D1%80%D0%B0%D0%BD%D0%B5%D0%BD", status_code=303)


@app.post("/finance/transfers/save")
def save_finance_transfer(
    request: Request,
    edit_id: str = Form(default=""),
    transfer_date: str = Form(default=""),
    category: str = Form(default=""),
    amount: str = Form(default="0"),
    from_wallet: str = Form(default=""),
    to_wallet: str = Form(default=""),
    comment: str = Form(default=""),
    date_from: str = Form(default=""),
    date_to: str = Form(default=""),
    year: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_finance_tables()
    form_data = {
        "transfer_date": transfer_date,
        "transfer_category": category,
        "transfer_amount": amount,
        "transfer_from_wallet": from_wallet,
        "transfer_to_wallet": to_wallet,
        "transfer_comment": comment,
    }
    if safe_cap_number(amount) <= 0:
        return HTMLResponse(finance_page_html(user, error_text="Сумма перемещения должна быть больше 0.", form_data=form_data), status_code=400)
    db = SessionLocal()
    try:
        item = db.query(FinanceTransferRow).filter(FinanceTransferRow.id == safe_number(edit_id)).first() if edit_id else None
        if not item:
            item = FinanceTransferRow()
            db.add(item)
        item.transfer_date = safe_text(transfer_date)
        item.category = safe_text(category)
        item.amount = safe_cap_number(amount)
        item.from_wallet = safe_text(from_wallet)
        item.to_wallet = safe_text(to_wallet)
        item.comment = safe_text(comment)
        db.commit()
    finally:
        db.close()
    clear_runtime_cache("finance_snapshot::")
    return RedirectResponse(url=f"/finance?date_from={quote_plus(safe_text(date_from))}&date_to={quote_plus(safe_text(date_to))}&year={quote_plus(safe_text(year))}&message=%D0%9F%D0%B5%D1%80%D0%B5%D0%BC%D0%B5%D1%89%D0%B5%D0%BD%D0%B8%D0%B5+%D1%81%D0%BE%D1%85%D1%80%D0%B0%D0%BD%D0%B5%D0%BD%D0%BE", status_code=303)


@app.post("/finance/wallets/delete")
def delete_finance_wallet(request: Request, wallet_id: str = Form(...), date_from: str = Form(default=""), date_to: str = Form(default=""), year: str = Form(default="")):
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
    clear_runtime_cache("finance_snapshot::")
    return RedirectResponse(url=f"/finance?date_from={quote_plus(safe_text(date_from))}&date_to={quote_plus(safe_text(date_to))}&year={quote_plus(safe_text(year))}&message=%D0%9A%D0%BE%D1%88%D0%B5%D0%BB%D0%B5%D0%BA+%D1%83%D0%B4%D0%B0%D0%BB%D0%B5%D0%BD", status_code=303)


@app.post("/finance/expenses/delete")
def delete_finance_expense(request: Request, expense_id: str = Form(...), date_from: str = Form(default=""), date_to: str = Form(default=""), year: str = Form(default="")):
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
    clear_runtime_cache("finance_snapshot::")
    return RedirectResponse(url=f"/finance?date_from={quote_plus(safe_text(date_from))}&date_to={quote_plus(safe_text(date_to))}&year={quote_plus(safe_text(year))}&message=%D0%A0%D0%B0%D1%81%D1%85%D0%BE%D0%B4+%D1%83%D0%B4%D0%B0%D0%BB%D0%B5%D0%BD", status_code=303)


@app.post("/finance/income/delete")
def delete_finance_income(request: Request, income_id: str = Form(...), date_from: str = Form(default=""), date_to: str = Form(default=""), year: str = Form(default="")):
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
    clear_runtime_cache("finance_snapshot::")
    return RedirectResponse(url=f"/finance?date_from={quote_plus(safe_text(date_from))}&date_to={quote_plus(safe_text(date_to))}&year={quote_plus(safe_text(year))}&message=%D0%9F%D1%80%D0%B8%D1%85%D0%BE%D0%B4+%D1%83%D0%B4%D0%B0%D0%BB%D0%B5%D0%BD", status_code=303)


@app.post("/finance/transfers/delete")
def delete_finance_transfer(request: Request, transfer_id: str = Form(...), date_from: str = Form(default=""), date_to: str = Form(default=""), year: str = Form(default="")):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin")
    ensure_finance_tables()
    db = SessionLocal()
    try:
        db.query(FinanceTransferRow).filter(FinanceTransferRow.id == safe_number(transfer_id)).delete()
        db.commit()
    finally:
        db.close()
    clear_runtime_cache("finance_snapshot::")
    return RedirectResponse(url=f"/finance?date_from={quote_plus(safe_text(date_from))}&date_to={quote_plus(safe_text(date_to))}&year={quote_plus(safe_text(year))}&message=%D0%9F%D0%B5%D1%80%D0%B5%D0%BC%D0%B5%D1%89%D0%B5%D0%BD%D0%B8%D0%B5+%D1%83%D0%B4%D0%B0%D0%BB%D0%B5%D0%BD%D0%BE", status_code=303)


@app.get("/caps", response_class=HTMLResponse)
def caps_page(
    request: Request,
    search: str = Query(default=""),
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
    sort_by: str = Query(default="cabinet"),
    order: str = Query(default="asc"),
    buyer: str = Query(default=""),
    code: str = Query(default=""),
    edit: str = Query(default=""),
    message: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "caps")
    search = safe_text(search)
    period_view = safe_text(period_view) or "current"
    period_label = safe_text(period_label)
    sort_by = safe_text(sort_by) or "cabinet"
    order = safe_text(order).lower() or "asc"
    buyer = safe_text(buyer)
    code = safe_text(code)
    edit = safe_text(edit)
    message = safe_text(message)
    refresh_cap_current_ftd_from_partner()
    effective_period_label = resolve_period_label(period_view, period_label) or get_current_period_label()

    rows = get_caps_rows(search=search, buyer=buyer, code=code, owner_name="", period_label=effective_period_label)
    allowed_sort_fields = {
        "advertiser", "manager", "cabinet", "code", "rate", "baseline",
        "cap", "current_ftd", "remaining", "fill", "promo_code", "agent", "chat_title", "kpi",
        "comments", "link", "action",
    }
    if sort_by not in allowed_sort_fields:
        sort_by = "cabinet"
    reverse = order == "desc"

    def caps_sort_value(item):
        values = {
            "advertiser": safe_text(item.advertiser).lower(),
            "manager": safe_text(item.owner_name).lower(),
            "cabinet": safe_text(item.cabinet_name).lower(),
            "code": normalize_geo_value(item.code or ""),
            "rate": safe_cap_number(item.rate),
            "baseline": safe_cap_number(item.baseline),
            "cap": safe_number(item.cap_value),
            "current_ftd": safe_number(item.current_ftd),
            "remaining": max(0.0, safe_number(item.cap_value) - safe_number(item.current_ftd)),
            "fill": cap_fill_percent(item.current_ftd, item.cap_value),
            "promo_code": safe_text(item.promo_code).lower(),
            "agent": safe_text(item.agent).lower(),
            "chat_title": safe_text(item.chat_title).lower(),
            "kpi": safe_text(item.kpi).lower(),
            "comments": safe_text(item.comments).lower(),
            "link": safe_text(item.link).lower(),
            "action": safe_number(item.id),
        }
        return values.get(sort_by, "")

    rows.sort(key=caps_sort_value, reverse=reverse)
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
                    "cabinet_name": item.cabinet_name or "",
                    "flow": item.flow or "",
                    "code": item.code or "",
                    "geo": item.geo or "",
                    "period_label": item.period_label or effective_period_label,
                    "rate": format_plain_number_text(item.rate),
                    "baseline": format_plain_number_text(item.baseline),
                    "cap_value": format_int_or_float(item.cap_value),
                    "current_ftd": format_int_or_float(item.current_ftd),
                    "promo_code": item.promo_code or "",
                    "chat_title": item.chat_title or "",
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
        filter_values={"search": search, "period_view": period_view, "period_label": effective_period_label, "sort_by": sort_by, "order": order, "buyer": buyer, "code": code},
        form_data=form_data,
        success_text=message,
    )


@app.post("/caps/save")
def save_cap(
    request: Request,
    edit_id: str = Form(default=""),
    advertiser: str = Form(default=""),
    owner_name: str = Form(default=""),
    period_view: str = Form(default="period"),
    period_label: str = Form(default=""),
    buyer: str = Form(default=""),
    code_filter: str = Form(default=""),
    search: str = Form(default=""),
    sort_by: str = Form(default="cabinet"),
    order: str = Form(default="asc"),
    cabinet_name: str = Form(default=""),
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
    clean_period_label = safe_text(period_label) or get_current_period_label()
    clean_cap_value = safe_cap_number(cap_value)
    clean_cabinet_name = safe_text(cabinet_name)
    clean_advertiser = safe_text(advertiser)
    clean_owner_name = safe_text(owner_name)
    form_data = {
        "edit_id": edit_id,
        "advertiser": advertiser,
        "owner_name": owner_name,
        "period_label": clean_period_label,
        "buyer": buyer,
        "cabinet_name": cabinet_name,
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
    current_filter_values = {
        "search": safe_text(search),
        "period_view": safe_text(period_view) or "period",
        "period_label": clean_period_label,
        "sort_by": safe_text(sort_by) or "cabinet",
        "order": safe_text(order).lower() or "asc",
        "buyer": clean_buyer,
        "code": safe_text(code_filter),
    }
    if clean_cap_value <= 0:
        return HTMLResponse(caps_page_html(user, get_caps_rows(period_label=clean_period_label), filter_values=current_filter_values, form_data=form_data, error_text="Cap must be greater than 0."), status_code=400)

    ensure_cabinet_table()
    db = SessionLocal()
    try:
        cabinet_item = db.query(CabinetRow).filter(CabinetRow.name == clean_cabinet_name).first()
        if not cabinet_item:
            rows = get_caps_rows(
                search=current_filter_values["search"],
                buyer=current_filter_values["buyer"],
                code=current_filter_values["code"],
                owner_name="",
                period_label=clean_period_label,
            )
            return HTMLResponse(caps_page_html(user, rows, filter_values=current_filter_values, form_data=form_data, error_text="Cabinet must exist in Cabinets list."), status_code=400)
        allowed_brands = split_list_tokens(getattr(cabinet_item, "brands", ""))
        expected_manager = safe_text(cabinet_item.manager_name)
        if not allowed_brands or not expected_manager:
            rows = get_caps_rows(
                search=current_filter_values["search"],
                buyer=current_filter_values["buyer"],
                code=current_filter_values["code"],
                owner_name="",
                period_label=clean_period_label,
            )
            return HTMLResponse(caps_page_html(user, rows, filter_values=current_filter_values, form_data=form_data, error_text="Fill brand and manager in Cabinets first."), status_code=400)
        if clean_advertiser not in allowed_brands or clean_owner_name != expected_manager:
            rows = get_caps_rows(
                search=current_filter_values["search"],
                buyer=current_filter_values["buyer"],
                code=current_filter_values["code"],
                owner_name="",
                period_label=clean_period_label,
            )
            return HTMLResponse(caps_page_html(user, rows, filter_values=current_filter_values, form_data=form_data, error_text="Brand, manager and cabinet must match Cabinets list."), status_code=400)

        item = db.query(CapRow).filter(CapRow.id == safe_number(edit_id)).first() if edit_id else None
        if not item:
            item = CapRow()
            db.add(item)

        item.advertiser = clean_advertiser
        item.owner_name = expected_manager
        item.period_label = clean_period_label
        item.buyer = clean_cabinet_name
        item.cabinet_name = clean_cabinet_name
        item.flow = safe_text(flow)
        item.code = normalize_geo_value(code)
        item.geo = geo_display_name(code)
        if safe_text(rate) or not edit_id:
            item.rate = safe_text(rate)
        if safe_text(baseline) or not edit_id:
            item.baseline = safe_text(baseline)
        item.cap_value = clean_cap_value
        item.current_ftd = safe_cap_number(current_ftd)
        item.promo_code = safe_text(promo_code)
        item.chat_title = safe_text(getattr(cabinet_item, "chat_name", ""))
        item.kpi = safe_text(kpi)
        item.link = safe_text(link)
        item.comments = safe_text(comments)
        item.agent = safe_text(agent)
        db.commit()
    finally:
        db.close()
    refresh_cap_current_ftd_from_partner()
    clear_runtime_cache("stat_support::")
    final_period_view = safe_text(period_view) or "period"
    return RedirectResponse(
        url=(
            f"/caps?period_view={quote_plus(final_period_view)}"
            f"&period_label={quote_plus(clean_period_label)}"
            f"&buyer={quote_plus(safe_text(buyer))}"
            f"&code={quote_plus(safe_text(code_filter))}"
            f"&search={quote_plus(safe_text(search))}"
            f"&sort_by={quote_plus(safe_text(sort_by) or 'cabinet')}"
            f"&order={quote_plus((safe_text(order).lower() or 'asc'))}"
            f"&message=Cap+saved"
        ),
        status_code=303,
    )

@app.post("/caps/delete")
def delete_cap(
    request: Request,
    cap_id: str = Form(...),
    period_view: str = Form(default="period"),
    period_label: str = Form(default=""),
    buyer: str = Form(default=""),
    code: str = Form(default=""),
    search: str = Form(default=""),
    sort_by: str = Form(default="cabinet"),
    order: str = Form(default="asc"),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "caps")
    clean_period_label = safe_text(period_label) or get_current_period_label()
    clean_period_view = safe_text(period_view) or "period"
    db = SessionLocal()
    try:
        db.query(CapRow).filter(CapRow.id == safe_number(cap_id)).delete()
        db.commit()
    finally:
        db.close()
    clear_runtime_cache("stat_support::")
    refresh_cap_current_ftd_from_partner()
    return RedirectResponse(
        url=(
            f"/caps?period_view={quote_plus(clean_period_view)}"
            f"&period_label={quote_plus(clean_period_label)}"
            f"&buyer={quote_plus(safe_text(buyer))}"
            f"&code={quote_plus(safe_text(code))}"
            f"&search={quote_plus(safe_text(search))}"
            f"&sort_by={quote_plus(safe_text(sort_by) or 'cabinet')}"
            f"&order={quote_plus((safe_text(order).lower() or 'asc'))}"
            f"&message=Cap+deleted"
        ),
        status_code=303,
    )

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
    source_name: str = Form(default="partner_players"),
    cabinet_name: str = Form(default=""),
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

        df = read_partner_uploaded_dataframe(temp_path, ext)

        detected_period = detect_partner_upload_period(df, partner_platform="1xbet", fallback_text=original_name)
        if period_mode != "half_month" and not detected_period:
            raise HTTPException(status_code=400, detail="Не удалось определить период выгрузки из шапки или имени файла.")
        if period_mode == "half_month":
            period = normalize_partner_period(date_start, date_end)
        else:
            period = detected_period or normalize_partner_period(date_start, date_end)

        final_source_name = build_partner_source_name(
            period["date_start"],
            period["date_end"],
            prefix=safe_text(source_name) or "partner_players",
        )

        rows = parse_partner_dataframe(df, source_name=final_source_name, cabinet_name=cabinet_name, upload_period_data=period)
        replace_partner_rows(final_source_name, rows)

        return {
            "status": "ok",
            "inserted": len(rows),
            "source_name": final_source_name,
            "date_start": period["date_start"],
            "date_end": period["date_end"],
            "period_label": period["period_label"],
            "cabinet_name": cabinet_name,
            "stored_file": temp_path,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Не удалось обработать файл партнера: {exc}")


@app.get("/cabinets", response_class=HTMLResponse)
def cabinets_page(
    request: Request,
    search: str = Query(default=""),
    status: str = Query(default=""),
    edit: str = Query(default=""),
    message: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "cabinets")
    rows = get_cabinet_rows(search=search, status=status)
    form_data = {}
    if edit:
        ensure_cabinet_table()
        db = SessionLocal()
        try:
            item = db.query(CabinetRow).filter(CabinetRow.id == safe_number(edit)).first()
            if item:
                form_data = {
                    "edit_id": str(item.id),
                    "advertiser": item.advertiser or "",
                    "platform": item.platform or "",
                    "name": item.name or "",
                    "geo_list": item.geo_list or "",
                    "brands": item.brands or "",
                    "team_name": item.team_name or "",
                    "manager_name": item.manager_name or "",
                    "manager_contact": item.manager_contact or "",
                    "chat_name": getattr(item, "chat_name", "") or "",
                    "wallet": item.wallet or "",
                    "comments": item.comments or "",
                    "status": item.status or "Active",
                }
        finally:
            db.close()
    return cabinets_page_html(
        user,
        rows,
        filter_values={"search": search},
        form_data=form_data,
        success_text=message,
    )


@app.post("/cabinets/save")
def save_cabinet(
    request: Request,
    edit_id: str = Form(default=""),
    advertiser: str = Form(default=""),
    platform: str = Form(default=""),
    name: str = Form(default=""),
    geo_list: str = Form(default=""),
    brands: str = Form(default=""),
    team_name: str = Form(default=""),
    manager_name: str = Form(default=""),
    manager_contact: str = Form(default=""),
    chat_name: str = Form(default=""),
    wallet: str = Form(default=""),
    comments: str = Form(default=""),
    status: str = Form(default="Active"),
    search: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "cabinets")
    ensure_cabinet_table()

    clean_name = safe_text(name)
    form_data = {
        "edit_id": edit_id,
        "advertiser": advertiser,
        "platform": platform,
        "name": name,
        "geo_list": geo_list,
        "brands": brands,
        "team_name": team_name,
        "manager_name": manager_name,
        "manager_contact": manager_contact,
        "chat_name": chat_name,
        "wallet": wallet,
        "comments": comments,
    }
    if not clean_name:
        return HTMLResponse(cabinets_page_html(user, get_cabinet_rows(), form_data=form_data, error_text="Cabinet name is required."), status_code=400)

    db = SessionLocal()
    try:
        duplicate = db.query(CabinetRow).filter(CabinetRow.name == clean_name)
        if edit_id:
            duplicate = duplicate.filter(CabinetRow.id != safe_number(edit_id))
        if duplicate.first():
            return HTMLResponse(cabinets_page_html(user, get_cabinet_rows(), form_data=form_data, error_text="Cabinet with this name already exists."), status_code=400)

        item = db.query(CabinetRow).filter(CabinetRow.id == safe_number(edit_id)).first() if edit_id else None
        if not item:
            item = CabinetRow()
            db.add(item)
        item.advertiser = safe_text(advertiser)
        item.platform = safe_text(platform)
        item.name = clean_name
        item.geo_list = safe_text(geo_list)
        item.brands = safe_text(brands)
        item.team_name = safe_text(team_name)
        item.manager_name = safe_text(manager_name)
        item.manager_contact = safe_text(manager_contact)
        item.chat_name = safe_text(chat_name)
        item.wallet = safe_text(wallet)
        item.comments = safe_text(comments)
        item.status = safe_text(item.status) or "Active"
        db.commit()
    finally:
        db.close()
    return RedirectResponse(
        url=f"/cabinets?search={quote_plus(safe_text(search))}&message=Partner+saved",
        status_code=303,
    )


@app.post("/cabinets/delete")
def delete_cabinet(
    request: Request,
    cabinet_id: str = Form(...),
    search: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "cabinets")
    ensure_cabinet_table()
    db = SessionLocal()
    try:
        db.query(CabinetRow).filter(CabinetRow.id == safe_number(cabinet_id)).delete()
        db.commit()
    finally:
        db.close()
    return RedirectResponse(
        url=f"/cabinets?search={quote_plus(safe_text(search))}&message=Partner+deleted",
        status_code=303,
    )


@app.get("/partner-report", response_class=HTMLResponse)
def partner_report_page(
    request: Request,
    source_name: str = Query(default=""),
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
    cabinet_name: str = Query(default=""),
    country: str = Query(default=""),
    search: str = Query(default=""),
    sort_by: str = Query(default="id"),
    order: str = Query(default="desc"),
    message: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "partner")
    effective_period_label = resolve_period_label(period_view, period_label) or get_current_period_label()
    upload_summaries = get_partner_upload_summaries()
    filtered = get_partner_rows_by_period(
        period_value=source_name,
        period_label=effective_period_label,
        cabinet_name=cabinet_name,
        country=country,
        search=search,
    )

    reverse = order != "asc"
    numeric_fields = {"deposit_amount", "bet_amount", "company_income", "cpa_amount"}

    def sort_value(row):
        value = getattr(row, sort_by, "")
        if sort_by in numeric_fields:
            return safe_number(value)
        if sort_by == "id":
            return safe_number(row.id)
        if sort_by in {"brand_name", "geo_name"}:
            return safe_text(value).lower()
        return safe_text(value).lower()

    filtered.sort(key=sort_value, reverse=reverse)
    return partner_report_page_html(
        user,
        filtered,
        upload_summaries=upload_summaries,
        source_name=source_name,
        period_view=period_view,
        period_label=effective_period_label,
        cabinet_name=cabinet_name,
        country=country,
        search=search,
        sort_by=sort_by,
        order=order,
        success_text=message,
    )


@app.post("/partner-report/upload")
async def upload_partner_report_file(
    request: Request,
    partner_platform: str = Form(default="1xbet"),
    cabinet_name: str = Form(default=""),
    file: UploadFile = File(...),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "partner")
    ensure_partner_table()
    ensure_cabinet_table()

    clean_cabinet_name = safe_text(cabinet_name)
    clean_platform = normalize_partner_platform(partner_platform)
    if not clean_cabinet_name:
        return HTMLResponse(partner_report_page_html(user, get_partner_rows_by_period(""), upload_platform=clean_platform, error_text="Choose a cabinet before upload."), status_code=400)
    cabinet_item = next((item for item in get_cabinet_rows() if safe_text(getattr(item, "name", "")) == clean_cabinet_name), None)
    if not cabinet_item:
        return HTMLResponse(partner_report_page_html(user, get_partner_rows_by_period(""), upload_platform=partner_platform, error_text="Choose a valid cabinet from Partners."), status_code=400)
    raw_platform_name = safe_text(getattr(cabinet_item, "platform", "")).strip()
    if safe_text(partner_platform).strip().lower() != raw_platform_name.lower():
        return HTMLResponse(partner_report_page_html(user, get_partner_rows_by_period(""), upload_platform=partner_platform, error_text="Selected cabinet does not belong to this platform."), status_code=400)

    original_name = file.filename or "partner_report.xlsx"
    ext = os.path.splitext(original_name)[1].lower() or ".xlsx"
    filename = f"temp_partner_manual_{uuid.uuid4()}{ext}"
    try:
        with open(filename, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        if clean_platform == "cellxpert":
            df = read_cellxpert_uploaded_dataframe(filename, ext)
        else:
            df = read_partner_uploaded_dataframe(filename, ext)
        if df is None or getattr(df, "empty", False):
            return HTMLResponse(partner_report_page_html(user, get_partner_rows_by_period(""), upload_platform=clean_platform, error_text="Partner file is empty."), status_code=400)
        detected_period = detect_partner_upload_period(df, partner_platform=clean_platform, fallback_text=original_name)
        if not detected_period:
            return HTMLResponse(partner_report_page_html(user, get_partner_rows_by_period(""), upload_platform=clean_platform, error_text="Could not determine upload period from the file header or filename."), status_code=400)
        source_prefix = clean_cabinet_name.replace("|", "/")
        if clean_platform == "cellxpert":
            source_prefix = f"cellxpert/{source_prefix}"
        final_source_name = build_partner_source_name(
            detected_period["date_start"],
            detected_period["date_end"],
            prefix=source_prefix,
        )
        rows = parse_partner_dataframe(
            df,
            source_name=final_source_name,
            cabinet_name=clean_cabinet_name,
            partner_platform=clean_platform,
            upload_period_data=detected_period,
        )
        if not rows:
            return HTMLResponse(partner_report_page_html(user, get_partner_rows_by_period(""), upload_platform=clean_platform, error_text="No valid player rows found in this file."), status_code=400)
        replace_partner_rows(final_source_name, rows)
        return RedirectResponse(url="/partner-report?message=Upload+saved", status_code=303)
    except Exception as exc:
        return HTMLResponse(partner_report_page_html(user, get_partner_rows_by_period(""), upload_platform=clean_platform, error_text=f"Could not process partner file: {exc}"), status_code=400)
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@app.post("/partner-report/delete-upload")
def delete_partner_upload(
    request: Request,
    source_name: str = Form(default=""),
    period_view: str = Form(default="current"),
    period_label: str = Form(default=""),
    cabinet_name: str = Form(default=""),
    country: str = Form(default=""),
    search: str = Form(default=""),
    sort_by: str = Form(default="id"),
    order: str = Form(default="desc"),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "partner")
    clean_source_name = safe_text(source_name)
    if not clean_source_name:
        return RedirectResponse(
            url=(
                f"/partner-report?period_view={quote_plus(safe_text(period_view) or 'current')}"
                f"&period_label={quote_plus(safe_text(period_label))}"
                f"&cabinet_name={quote_plus(safe_text(cabinet_name))}"
                f"&country={quote_plus(safe_text(country))}"
                f"&search={quote_plus(safe_text(search))}"
                f"&sort_by={quote_plus(safe_text(sort_by) or 'id')}"
                f"&order={quote_plus(safe_text(order).lower() or 'desc')}"
                f"&message=Upload+not+found"
            ),
            status_code=303,
        )

    ensure_partner_table()
    db = SessionLocal()
    try:
        db.query(PartnerRow).filter(PartnerRow.source_name == clean_source_name).delete()
        db.commit()
    finally:
        db.close()
    clear_runtime_cache("stat_support::")
    refresh_cap_current_ftd_from_partner()
    return RedirectResponse(
        url=(
            f"/partner-report?period_view={quote_plus(safe_text(period_view) or 'current')}"
            f"&period_label={quote_plus(safe_text(period_label))}"
            f"&cabinet_name={quote_plus(safe_text(cabinet_name))}"
            f"&country={quote_plus(safe_text(country))}"
            f"&search={quote_plus(safe_text(search))}"
            f"&sort_by={quote_plus(safe_text(sort_by) or 'id')}"
            f"&order={quote_plus(safe_text(order).lower() or 'desc')}"
            f"&message=Upload+deleted"
        ),
        status_code=303,
    )


@app.post("/partner-report/flags/save")
def save_partner_row_flags(
    request: Request,
    partner_row_id: str = Form(...),
    return_to: str = Form(default="/partner-report"),
    manual_hold: str = Form(default="0"),
    manual_blocked: str = Form(default="0"),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "partner")
    ensure_partner_table()
    db = SessionLocal()
    try:
        item = db.query(PartnerRow).filter(PartnerRow.id == safe_number(partner_row_id)).first()
        if not item:
            return RedirectResponse(url="/partner-report?message=Player+not+found", status_code=303)
        item.manual_hold = 1 if safe_text(manual_hold) in {"1", "true", "on", "yes"} else 0
        item.manual_blocked = 1 if safe_text(manual_blocked) in {"1", "true", "on", "yes"} else 0
        db.add(item)
        db.commit()
    finally:
        db.close()
    target = safe_text(return_to) or "/partner-report"
    separator = "&" if "?" in target else "?"
    return RedirectResponse(url=f"{target}{separator}message=Flags+saved", status_code=303)


@app.get("/chatterfy", response_class=HTMLResponse)
def chatterfy_page(
    request: Request,
    status: str = Query(default=""),
    search: str = Query(default=""),
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
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
    effective_period_label = resolve_period_label(period_view, period_label) or get_current_period_label()
    rows = get_chatterfy_rows(
        status=status,
        search=search,
        date_filter=date_filter,
        time_filter=time_filter,
        telegram_id=telegram_id,
        pp_player_id=pp_player_id,
        period_label=effective_period_label,
    )
    allowed_sort_fields = {
        "report_date",
        "period_label",
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
            "report_date": item.get("report_date") or "",
            "period_label": item.get("period_label") or "",
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
        period_view=period_view,
        period_label=effective_period_label,
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
def hold_wager_page(
    request: Request,
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
    cabinet_name: str = Query(default=""),
    search: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "holdwager")
    period_view = safe_text(period_view) or "current"
    period_label = safe_text(period_label)
    cabinet_name = safe_text(cabinet_name)
    search = safe_text(search)
    effective_period_label = resolve_period_label(period_view, period_label) or get_current_period_label()
    rows = get_hold_wager_rows(
        period_label=effective_period_label,
        cabinet_name=cabinet_name,
        search=search,
    )
    return hold_wager_page_html(
        user,
        rows,
        cabinet_name=cabinet_name,
        period_view=period_view,
        period_label=effective_period_label,
        search=search,
    )
