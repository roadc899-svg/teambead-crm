from fastapi import FastAPI, UploadFile, File, Query, Form, Request, HTTPException, Header
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse, Response, FileResponse, JSONResponse
from fastapi.exception_handlers import http_exception_handler
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text, or_, inspect, func
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
import shutil
import uuid
import os
import json
from urllib.parse import urlencode, quote_plus, urlparse
from html import escape
import io
import csv
import secrets
import hashlib
import calendar
import importlib.machinery
import importlib.util
import re
import sys
import time
import threading
import ssl
import urllib.request
import urllib.error
from functools import lru_cache
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo
from playwright.sync_api import sync_playwright
from teambead_domain_actions.analytics import bind_domain_actions as bind_analytics_actions
from teambead_domain_actions.management import bind_domain_actions as bind_management_actions
from teambead_domain_actions.parsers import bind_domain_actions as bind_parser_actions
from teambead_domain_actions.reports import bind_domain_actions as bind_report_actions
from teambead_ui.page_routes import bind_page_routes
from teambead_ui.page_views import bind_page_views

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
CELLXPERT_EUR_TO_USD_RATE = float(os.getenv("CELLXPERT_EUR_TO_USD_RATE", "1.08").strip() or "1.08")
CHATTERFY_API_BASE_URL = "https://api.chatterfy.ai/api"
CHATTERFY_SIGNIN_URL = "https://new.chatterfy.ai/signin"
CHATTERFY_PARSER_DEFAULT_START = "2026-03-16"
CHATTERFY_PARSER_DEFAULT_END = "2026-03-31"
CHATTERFY_PARSER_CONFIG_PATH = os.path.join(DATA_UPLOAD_DIR, "chatterfy_parser_config.json")
CHATTERFY_PARSER_ACCOUNT_EMAIL = "roadc899@gmail.com"
CHATTERFY_PARSER_ACCOUNT_PASSWORD = "nCjcTV0Om,W/zs/"
ONEXBET_RUNTIME_DIR = os.path.join(DATA_UPLOAD_DIR, "onexbet_runtime")
ONEXBET_ACCOUNTS_PATH = os.path.join(ONEXBET_RUNTIME_DIR, "accounts.json")
ONEXBET_SESSION_DIR = os.path.join(ONEXBET_RUNTIME_DIR, "sessions")
ONEXBET_STATUS_PATH = os.path.join(".", "parser_1xbet_status.json")
ONEXBET_LOGIN_URL = "https://1xpartners.com/sign-in"
ONEXBET_AGENT_STATE_PATH = os.path.join(ONEXBET_RUNTIME_DIR, "agent_state.json")
ONEXBET_AGENT_JOB_PATH = os.path.join(ONEXBET_RUNTIME_DIR, "agent_job.json")
ONEXBET_AGENT_API_KEY = os.getenv("TEAMBEAD_ONEX_AGENT_KEY", "teambead-onex-agent").strip() or "teambead-onex-agent"
ONEX_PARSER_IMPORT_LOCK = threading.Lock()
LOCAL_TIMEZONE = ZoneInfo(os.getenv("TEAMBEAD_TIMEZONE", "Europe/Kiev"))
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
LIVE_DATA_VERSION = int(time.time() * 1000)
CHATTERFY_SYNC_LOCK = threading.Lock()


class ChatterfySyncStopped(RuntimeError):
    pass
CHATTERFY_SYNC_THREAD_STARTED = False
CHATTERFY_CONFIG_LOCK = threading.Lock()
ONEXBET_STATUS_LOCK = threading.Lock()
ONEXBET_SESSION_LOCK = threading.Lock()
ONEXBET_SESSION_THREAD = None
ONEXBET_LEGACY_MODULE = None
ONEXBET_AGENT_LOCK = threading.Lock()
ONEXBET_KEEPALIVE_LOCK = threading.Lock()
ONEXBET_KEEPALIVE_THREADS = {}
ONEXBET_KEEPALIVE_INTERVAL_SECONDS = int(os.getenv("TEAMBEAD_ONEX_KEEPALIVE_INTERVAL", "240") or "240")


def get_crm_local_now():
    return datetime.now(LOCAL_TIMEZONE)


def get_crm_local_date():
    return get_crm_local_now().date()


# =========================================
# BLOCK 2 — MODEL
# =========================================
class FBRow(Base):
    __tablename__ = "fb_rows"

    id = Column(Integer, primary_key=True, index=True)
    uploader = Column(String)  # internally оставляем старое имя поля для совместимости с БД
    source_name = Column(String, default="")
    period_label = Column(String, default="")
    ad_name = Column(String)
    adset_name = Column(String, default="")
    campaign_name = Column(String, default="")
    budget = Column(Float, default=0)
    account_id = Column(String, default="")

    launch_date = Column(String)
    platform = Column(String)
    manager = Column(String)
    geo = Column(String)
    offer = Column(String)
    creative = Column(String)

    material_views = Column(Float, default=0)
    leads = Column(Float)
    reg = Column(Float)
    paid_subscriptions = Column(Float, default=0)
    contacts = Column(Float, default=0)
    ftd = Column(Float)
    clicks = Column(Float)
    spend = Column(Float)
    frequency = Column(Float, default=0)
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


class ChatterfyParserRow(Base):
    __tablename__ = "chatterfy_parser_rows"

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
    chat_link = Column(String, default="")
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


class OnexParserRow(Base):
    __tablename__ = "onex_parser_rows"

    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(String, index=True, default="")
    account_label = Column(String, default="")
    source_name = Column(String, default="")
    report_date = Column(String, default="")
    period_start = Column(String, default="")
    period_end = Column(String, default="")
    period_label = Column(String, default="")
    registration_date = Column(String, default="")
    country = Column(String, default="")
    sub_id = Column(String, default="")
    player_id = Column(String, default="")
    deposit_amount = Column(Float, default=0)
    bet_amount = Column(Float, default=0)
    company_income = Column(Float, default=0)
    cpa_amount = Column(Float, default=0)
    hold_time = Column(String, default="")
    blocked = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


try:
    Base.metadata.create_all(bind=engine)
except Exception as exc:
    # Some deployed databases already have chatterfy_parser_rows created,
    # but SQLAlchemy may still attempt a duplicate CREATE TABLE on import.
    error_text = str(exc).lower()
    if "chatterfy_parser_rows" not in error_text or "already exists" not in error_text:
        raise


def ensure_table_once(key: str, tables, sqlite_callback=None):
    if key in ENSURED_TABLES:
        return
    Base.metadata.create_all(bind=engine, tables=tables)
    if DATABASE_URL.startswith("sqlite") and sqlite_callback:
        sqlite_callback()
    ENSURED_TABLES.add(key)


def ensure_fb_table():
    def sqlite_migration():
        with engine.begin() as conn:
            columns = [row[1] for row in conn.execute(text("PRAGMA table_info(fb_rows)")).fetchall()]
            migration_map = {
                "source_name": "ALTER TABLE fb_rows ADD COLUMN source_name VARCHAR DEFAULT ''",
                "period_label": "ALTER TABLE fb_rows ADD COLUMN period_label VARCHAR DEFAULT ''",
                "adset_name": "ALTER TABLE fb_rows ADD COLUMN adset_name VARCHAR DEFAULT ''",
                "campaign_name": "ALTER TABLE fb_rows ADD COLUMN campaign_name VARCHAR DEFAULT ''",
                "budget": "ALTER TABLE fb_rows ADD COLUMN budget FLOAT DEFAULT 0",
                "account_id": "ALTER TABLE fb_rows ADD COLUMN account_id VARCHAR DEFAULT ''",
                "material_views": "ALTER TABLE fb_rows ADD COLUMN material_views FLOAT DEFAULT 0",
                "paid_subscriptions": "ALTER TABLE fb_rows ADD COLUMN paid_subscriptions FLOAT DEFAULT 0",
                "contacts": "ALTER TABLE fb_rows ADD COLUMN contacts FLOAT DEFAULT 0",
                "frequency": "ALTER TABLE fb_rows ADD COLUMN frequency FLOAT DEFAULT 0",
            }
            for column_name, statement in migration_map.items():
                if column_name not in columns:
                    conn.execute(text(statement))

    ensure_table_once("fb_rows", [FBRow.__table__], sqlite_migration)
    if not DATABASE_URL.startswith("sqlite"):
        inspector = inspect(engine)
        columns = {item.get("name") for item in inspector.get_columns("fb_rows")}
        migration_statements = {
            "source_name": text("ALTER TABLE fb_rows ADD COLUMN IF NOT EXISTS source_name VARCHAR DEFAULT ''"),
            "period_label": text("ALTER TABLE fb_rows ADD COLUMN IF NOT EXISTS period_label VARCHAR DEFAULT ''"),
            "adset_name": text("ALTER TABLE fb_rows ADD COLUMN IF NOT EXISTS adset_name VARCHAR DEFAULT ''"),
            "campaign_name": text("ALTER TABLE fb_rows ADD COLUMN IF NOT EXISTS campaign_name VARCHAR DEFAULT ''"),
            "budget": text("ALTER TABLE fb_rows ADD COLUMN IF NOT EXISTS budget FLOAT DEFAULT 0"),
            "account_id": text("ALTER TABLE fb_rows ADD COLUMN IF NOT EXISTS account_id VARCHAR DEFAULT ''"),
            "material_views": text("ALTER TABLE fb_rows ADD COLUMN IF NOT EXISTS material_views FLOAT DEFAULT 0"),
            "paid_subscriptions": text("ALTER TABLE fb_rows ADD COLUMN IF NOT EXISTS paid_subscriptions FLOAT DEFAULT 0"),
            "contacts": text("ALTER TABLE fb_rows ADD COLUMN IF NOT EXISTS contacts FLOAT DEFAULT 0"),
            "frequency": text("ALTER TABLE fb_rows ADD COLUMN IF NOT EXISTS frequency FLOAT DEFAULT 0"),
        }
        missing = [statement for key, statement in migration_statements.items() if key not in columns]
        if missing:
            with engine.begin() as conn:
                for statement in missing:
                    conn.execute(statement)


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
            "CREATE INDEX IF NOT EXISTS ix_chatterfy_parser_rows_scope ON chatterfy_parser_rows (period_label, status, manager, geo, offer)",
            "CREATE INDEX IF NOT EXISTS ix_chatterfy_parser_rows_lookup ON chatterfy_parser_rows (telegram_id, external_id)",
            "CREATE INDEX IF NOT EXISTS ix_chatterfy_id_rows_lookup ON chatterfy_id_rows (telegram_id, pp_player_id)",
            "CREATE INDEX IF NOT EXISTS ix_task_rows_scope ON task_rows (assigned_to_username, status, due_at)",
            "CREATE INDEX IF NOT EXISTS ix_cabinet_rows_scope ON cabinet_rows (status, name, manager_name)",
            "CREATE INDEX IF NOT EXISTS ix_finance_wallet_rows_wallet ON finance_wallet_rows (wallet)",
        ]
        for statement in index_statements:
            conn.execute(text(statement))
    RUNTIME_INDEXES_READY = True


ensure_runtime_indexes()
ensure_fb_table()


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


def bump_live_data_version():
    global LIVE_DATA_VERSION
    LIVE_DATA_VERSION = int(time.time() * 1000)


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
        "chatterfy_parser_rows",
        "chatterfy_id_rows",
    ]:
        sync_postgres_sequence(table_name)


sync_all_postgres_sequences()


# =========================================
# BLOCK 3 — APP
# =========================================
app = FastAPI(title="TEAMbead CRM")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.on_event("startup")
def chatterfy_parser_startup():
    start_chatterfy_parser_sync_thread()
    recover_chatterfy_parser_after_startup()


@app.middleware("http")
async def live_data_version_middleware(request: Request, call_next):
    response = await call_next(request)
    if request.method.upper() in {"POST", "PUT", "PATCH", "DELETE"} and response.status_code < 400:
        bump_live_data_version()
    return response


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


@app.get("/api/live-version")
def api_live_version(request: Request, response: Response):
    if not get_current_user(request):
        raise HTTPException(status_code=401)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    return {"version": LIVE_DATA_VERSION}


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
        "chatterfyparser": {"superadmin", "admin"},
        "onexparser": {"superadmin", "admin"},
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
    ensure_fb_table()
    buyer_scope = resolve_effective_buyer(user)
    rows = get_filtered_data(buyer=buyer_scope, period_label=period_label)
    buyer_values = [value for value, _label in get_fb_buyer_name_options()]
    return (
        buyer_values,
        sorted({r.manager for r in rows if r.manager}),
        sorted({r.geo for r in rows if r.geo}),
        sorted({r.offer for r in rows if r.offer}),
    )


def auth_redirect_response(url: str = "/login"):
    return RedirectResponse(url=url, status_code=302)


ensure_default_users()


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return _page_routes["login_page"](request)


@app.post("/login")
def login_submit(username: str = Form(...), password: str = Form(...)):
    return _page_routes["login_submit"](username, password)


@app.get("/logout")
def logout(request: Request):
    return _page_routes["logout"](request)


@app.api_route("/", methods=["GET", "HEAD"])
def home(request: Request):
    return _page_routes["home"](request)


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


def convert_cellxpert_eur_to_usd(value):
    amount = safe_number(value)
    return round(amount * CELLXPERT_EUR_TO_USD_RATE, 2)



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
        if value is None:
            return ""
        return f"${float(value):,.2f}"
    except Exception:
        return "$0.00"



def format_optional_money(value):
    if value is None:
        return ""
    text_value = safe_text(value)
    if not text_value:
        return ""
    return format_money(value)



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


def parse_fb_dimensions(ad_name="", adset_name="", campaign_name=""):
    parsed_ad = parse_ad_name(ad_name)
    parsed_adset = parse_ad_name(adset_name)
    parsed_campaign = parse_ad_name(campaign_name)

    def pick(field, *items):
        for item in items:
            value = safe_text((item or {}).get(field))
            if value:
                return value
        return ""

    return {
        "launch_date": pick("launch_date", parsed_ad, parsed_adset, parsed_campaign),
        "platform": pick("platform", parsed_adset, parsed_ad, parsed_campaign),
        "manager": pick("manager", parsed_adset, parsed_ad, parsed_campaign),
        "geo": normalize_geo_value(pick("geo", parsed_adset, parsed_ad, parsed_campaign)),
        "offer": pick("offer", parsed_adset, parsed_ad, parsed_campaign),
        "creative": pick("creative", parsed_ad, parsed_adset, parsed_campaign),
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


def make_labeled_options(options, selected_value):
    html = '<option value="">Все</option>'
    for value, label in options:
        option_value = escape(safe_text(value))
        option_label = escape(safe_text(label))
        selected = "selected" if safe_text(value) == safe_text(selected_value) else ""
        html += f'<option value="{option_value}" {selected}>{option_label}</option>'
    return html



def normalize_fb_date_value(value):
    dt = parse_datetime_flexible(value)
    if not dt:
        text_value = safe_text(value)
        if text_value:
            try:
                dt = pd.to_datetime(text_value, errors="coerce", dayfirst=False)
                if pd.isna(dt):
                    dt = pd.to_datetime(text_value, errors="coerce", dayfirst=True)
                if not pd.isna(dt) and hasattr(dt, "to_pydatetime"):
                    dt = dt.to_pydatetime()
            except Exception:
                dt = None
    return dt.strftime("%Y-%m-%d") if dt else ""


def detect_fb_upload_period(df):
    colmap = {str(c).strip().lower(): c for c in df.columns}

    def get_col(*names):
        for name in names:
            if name.lower() in colmap:
                return colmap[name.lower()]
        return None

    date_start_col = get_col("Дата начала отчетности", "Date Start")
    date_end_col = get_col("Дата окончания отчетности", "Date End")
    start_values = []
    end_values = []
    for _, row in df.iterrows():
        start_value = normalize_fb_date_value(row.get(date_start_col)) if date_start_col else ""
        end_value = normalize_fb_date_value(row.get(date_end_col)) if date_end_col else ""
        if start_value:
            start_values.append(start_value)
        if end_value:
            end_values.append(end_value)
    if not start_values and not end_values:
        return None
    date_start = min(start_values or end_values)
    date_end = max(end_values or start_values)
    try:
        start_dt = datetime.strptime(date_start, "%Y-%m-%d")
        end_dt = datetime.strptime(date_end, "%Y-%m-%d")
        period_label = f"{start_dt.strftime('%d.%m.%Y')} - {end_dt.strftime('%d.%m.%Y')}"
    except Exception:
        period_label = ""
    return {
        "date_start": date_start,
        "date_end": date_end,
        "period_label": period_label,
    }


def build_fb_source_name(buyer, period):
    buyer_name = safe_text(buyer) or "fb"
    date_start = safe_text((period or {}).get("date_start"))
    date_end = safe_text((period or {}).get("date_end"))
    if date_start and date_end:
        return f"{buyer_name} | {date_start} | {date_end}"
    return buyer_name


def parse_uploaded_dataframe(df, buyer, source_name="", period_label="", period_date_start="", period_date_end=""):
    colmap = {str(c).strip().lower(): c for c in df.columns}

    def get_col(*names):
        for name in names:
            if name.lower() in colmap:
                return colmap[name.lower()]
        return None

    ad_col = get_col("Название объявления", "Ad name", "Ad Name")
    adset_col = get_col("Название группы объявлений", "Ad set name", "Ad Set Name")
    campaign_col = get_col("Название кампании", "Название компании", "Campaign name", "Campaign Name")
    budget_col = get_col(
        "Budget",
        "Бюджет",
        "Бюджет группы объявлений",
        "Ad set budget",
        "Ad Set Budget",
        "Ad group budget",
        "Ad Group Budget",
    )
    budget_type_col = get_col(
        "Тип бюджета группы объявлений",
        "Ad set budget type",
        "Ad Set Budget Type",
        "Ad group budget type",
        "Ad Group Budget Type",
    )
    account_id_col = get_col("Идентификатор аккаунта", "Account ID", "Account id")
    material_views_col = get_col("Просмотры материалов", "Content views")
    leads_col = get_col("Лиды", "Leads")
    reg_col = get_col("Завершенные регистрации", "Регистрации", "REG")
    paid_subscriptions_col = get_col("Подписки", "Subscriptions")
    contacts_col = get_col("Контакты", "Contacts")
    ftd_col = get_col("Покупки", "FTD", "Purchases")
    clicks_col = get_col("Клики по ссылке", "Clicks", "Link Clicks")
    spend_col = get_col("Сумма затрат", "Потраченная сумма (USD)", "Spend", "Amount spent (USD)")
    cpc_col = get_col("CPC (цена за клик по ссылке)", "CPC")
    ctr_col = get_col("CTR (все)", "CTR")
    frequency_col = get_col("Частота", "Frequency")
    ds_col = get_col("Дата начала отчетности", "Date Start")
    de_col = get_col("Дата окончания отчетности", "Date End")

    items = []
    for _, row in df.iterrows():
        ad_name = str(row.get(ad_col) or "") if ad_col else ""
        adset_name = safe_text(row.get(adset_col)) if adset_col else ""
        campaign_name = safe_text(row.get(campaign_col)) if campaign_col else ""
        parsed = parse_fb_dimensions(ad_name=ad_name, adset_name=adset_name, campaign_name=campaign_name)
        row_date_start = normalize_fb_date_value(row.get(ds_col)) if ds_col else ""
        row_date_end = normalize_fb_date_value(row.get(de_col)) if de_col else ""

        raw_budget = row.get(budget_col) if budget_col else ""
        budget_value = safe_number(raw_budget) if budget_col else None
        budget_type = safe_text(row.get(budget_type_col)) if budget_type_col else ""
        if isinstance(raw_budget, str):
            raw_budget_text = safe_text(raw_budget).strip().lower()
            if raw_budget_text in {"using campaign budget", "campaign budget", "бюджет кампании"}:
                budget_value = None
            elif not raw_budget_text:
                budget_value = None

        items.append(
            FBRow(
                uploader=buyer,
                source_name=safe_text(source_name),
                period_label=safe_text(period_label),
                ad_name=ad_name,
                adset_name=adset_name,
                campaign_name=campaign_name,
                budget=budget_value,
                account_id=safe_text(row.get(account_id_col)) if account_id_col else "",
                launch_date=parsed["launch_date"],
                platform=parsed["platform"],
                manager=parsed["manager"],
                geo=parsed["geo"],
                offer=parsed["offer"],
                creative=parsed["creative"],
                material_views=safe_number(row.get(material_views_col)) if material_views_col else 0,
                leads=safe_number(row.get(leads_col)) if leads_col else 0,
                reg=safe_number(row.get(reg_col)) if reg_col else 0,
                paid_subscriptions=safe_number(row.get(paid_subscriptions_col)) if paid_subscriptions_col else 0,
                contacts=safe_number(row.get(contacts_col)) if contacts_col else 0,
                ftd=safe_number(row.get(ftd_col)) if ftd_col else 0,
                clicks=safe_number(row.get(clicks_col)) if clicks_col else 0,
                spend=safe_number(row.get(spend_col)) if spend_col else 0,
                frequency=safe_number(row.get(frequency_col)) if frequency_col else 0,
                cpc=safe_number(row.get(cpc_col)) if cpc_col else 0,
                ctr=safe_number(row.get(ctr_col)) if ctr_col else 0,
                date_start=row_date_start or safe_text(period_date_start),
                date_end=row_date_end or safe_text(period_date_end),
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


def read_chatterfy_uploaded_dataframe(filename, ext):
    ext = (ext or "").lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(filename)
    return read_csv_with_auto_separator(filename)


def normalize_dataframe_columns(df):
    if df is None:
        return {}
    normalized = {}
    columns = getattr(df, "columns", None)
    if columns is None:
        return normalized
    for column in list(columns):
        key = safe_text(column).strip().lower()
        if key and key not in normalized:
            normalized[key] = column
    return normalized


def resolve_normalized_dataframe_column(normalized_map, aliases):
    for alias in aliases:
        key = safe_text(alias).strip().lower()
        if key and key in normalized_map:
            return normalized_map[key]
    return ""


def detect_chatterfy_upload_kind(df):
    normalized_columns = normalize_dataframe_columns(df)
    has_main = all([
        resolve_normalized_dataframe_column(normalized_columns, ["Name"]),
        resolve_normalized_dataframe_column(normalized_columns, ["Telegram ID", "TelegramID", "Telegram Id"]),
        resolve_normalized_dataframe_column(normalized_columns, ["Tags", "Tag"]),
        resolve_normalized_dataframe_column(normalized_columns, ["Started", "Start", "Started At"]),
        resolve_normalized_dataframe_column(normalized_columns, ["Status"]),
    ])
    has_ids = bool(resolve_normalized_dataframe_column(normalized_columns, ["TELEGRAM ID", "Telegram ID", "telegram_id"]))
    has_linkage = bool(resolve_normalized_dataframe_column(normalized_columns, ["1xbet_id", "pp_id", "ID игрока", "chatlink", "chat_link", "link"]))
    if has_main:
        return "main"
    if has_ids and has_linkage:
        return "ids"
    return ""


def parse_datetime_flexible(value):
    text = safe_text(value)
    if not text:
        return None
    try:
        iso_text = text.replace("Z", "+00:00") if text.endswith("Z") else text
        return datetime.fromisoformat(iso_text)
    except Exception:
        pass
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
    base_date = today or get_crm_local_date()
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


def period_label_to_dates(period_label=""):
    clean_label = safe_text(period_label)
    if not clean_label:
        return get_half_month_period()
    parts = [part.strip() for part in clean_label.split("-")]
    if len(parts) >= 2:
        left = parts[0]
        right = parts[1]
        try:
            start_date = datetime.strptime(left, "%d.%m.%Y").strftime("%Y-%m-%d")
            end_date = datetime.strptime(right, "%d.%m.%Y").strftime("%Y-%m-%d")
            return {
                "period_label": f"{left} - {right}",
                "date_start": start_date,
                "date_end": end_date,
            }
        except Exception:
            pass
    return get_half_month_period()


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
    stored_label = safe_text(getattr(row, "period_label", ""))
    if stored_label:
        return stored_label
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
    os.makedirs(ONEXBET_RUNTIME_DIR, exist_ok=True)
    os.makedirs(ONEXBET_SESSION_DIR, exist_ok=True)


def read_json_file(path, default=None):
    fallback = {} if default is None else default
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception:
        return fallback
    return payload if isinstance(payload, type(fallback)) else fallback


def write_json_file(path, payload):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def get_onex_agent_state():
    ensure_upload_dir()
    with ONEXBET_AGENT_LOCK:
        payload = read_json_file(ONEXBET_AGENT_STATE_PATH, default={})
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("connected", False)
    payload.setdefault("status", "offline")
    payload.setdefault("message", "Локальный агент ещё не подключён.")
    payload.setdefault("agent_version", "")
    payload.setdefault("last_seen_at", "")
    payload.setdefault("current_account", "")
    payload.setdefault("current_account_label", "")
    payload.setdefault("job_status", "idle")
    payload.setdefault("job_message", "")
    payload.setdefault("session_saved", False)
    payload.setdefault("profile_exists", False)
    payload.setdefault("last_error", "")
    payload.setdefault("paused", False)
    payload.setdefault("logs", [])
    return payload


def save_onex_agent_state(**updates):
    ensure_upload_dir()
    with ONEXBET_AGENT_LOCK:
        payload = read_json_file(ONEXBET_AGENT_STATE_PATH, default={})
        if not isinstance(payload, dict):
            payload = {}
        payload.update(updates)
        payload["updated_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        write_json_file(ONEXBET_AGENT_STATE_PATH, payload)
    return payload


def append_onex_agent_log(message, kind="info"):
    payload = get_onex_agent_state()
    logs = list(payload.get("logs") or [])
    logs.append({
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "kind": safe_text(kind) or "info",
        "message": safe_text(message),
    })
    return save_onex_agent_state(logs=logs[-100:])


def build_onex_log_badge(kind: str):
    clean_kind = safe_text(kind).lower() or "info"
    palette = {
        "error": ("#fee2e2", "#991b1b"),
        "success": ("#dcfce7", "#166534"),
        "start": ("#dbeafe", "#1d4ed8"),
        "done": ("#dcfce7", "#166534"),
        "info": ("#f4f7fb", "#60708a"),
    }
    return palette.get(clean_kind, palette["info"])


def format_onex_status_label(value: str):
    clean_value = safe_text(value).lower()
    mapping = {
        "idle": "Idle",
        "running": "Running",
        "waiting_for_user": "Waiting for user",
        "ready": "Ready",
        "error": "Error",
        "offline": "Offline",
        "online": "Online",
    }
    if clean_value in mapping:
        return mapping[clean_value]
    if not clean_value:
        return "Idle"
    return safe_text(value).replace("_", " ").strip().title()


def build_onex_account_statuses(state=None):
    current_state = dict(state or get_onex_agent_state() or {})
    current_job = get_onex_agent_job()
    current_status = safe_text(current_state.get("status")).lower()
    current_account = safe_text(current_state.get("current_account"))
    current_job_type = safe_text(current_job.get("type")).lower()
    queued_ids = [safe_text(item) for item in (current_job.get("account_ids") or []) if safe_text(item)]
    if safe_text(current_job.get("account_id")) and safe_text(current_job.get("account_id")) != "__all__":
        queued_ids.append(safe_text(current_job.get("account_id")))
    queued_ids = list(dict.fromkeys(queued_ids))

    def status_meta(account_id, has_session, has_profile):
        if current_status == "waiting_for_user" and (current_account == account_id or current_account == "__all__"):
            return {
                "key": "captcha",
                "label": "Captcha",
                "note": "Нужно пройти captcha",
                "bg": "#fff7ed",
                "fg": "#9a3412",
            }
        if current_status == "running" and (current_account == account_id or current_account == "__all__"):
            return {
                "key": "running",
                "label": "Активно грузит",
                "note": "Идёт выгрузка",
                "bg": "#dbeafe",
                "fg": "#1d4ed8",
            }
        if account_id in queued_ids and current_job_type in {"start_multi_flow", "start_full_flow", "run_export", "open_login", "check_auth"}:
            return {
                "key": "queued",
                "label": "Ожидает запуск",
                "note": "Стоит в очереди",
                "bg": "#f4f7fb",
                "fg": "#60708a",
            }
        if has_session or has_profile:
            return {
                "key": "session",
                "label": "Хранит сессию",
                "note": "Готов к запуску",
                "bg": "#dcfce7",
                "fg": "#166534",
            }
        if current_status == "error" and (current_account == account_id or current_account == "__all__"):
            return {
                "key": "error",
                "label": "Ошибка",
                "note": "Нужна проверка",
                "bg": "#fee2e2",
                "fg": "#991b1b",
            }
        return {
            "key": "idle",
            "label": "Ожидает запуск",
            "note": "Сессии нет",
            "bg": "#f4f7fb",
            "fg": "#60708a",
        }

    result = []
    for account in get_onexbet_accounts():
        account_id = safe_text(account.get("id"))
        has_session = onexbet_session_exists(account_id)
        has_profile = onexbet_profile_exists(account_id)
        meta = status_meta(account_id, has_session, has_profile)
        result.append({
            "id": account_id,
            "label": safe_text(account.get("label")) or account_id,
            "status_key": meta["key"],
            "status_label": meta["label"],
            "status_note": meta["note"],
            "session_saved": has_session,
            "profile_exists": has_profile,
            "bg": meta["bg"],
            "fg": meta["fg"],
        })
    return result


def get_onex_agent_job():
    ensure_upload_dir()
    with ONEXBET_AGENT_LOCK:
        payload = read_json_file(ONEXBET_AGENT_JOB_PATH, default={})
    return payload if isinstance(payload, dict) else {}


def save_onex_agent_job(payload):
    ensure_upload_dir()
    with ONEXBET_AGENT_LOCK:
        write_json_file(ONEXBET_AGENT_JOB_PATH, payload or {})
    return payload or {}


def clear_onex_agent_job():
    return save_onex_agent_job({})


def queue_onex_agent_job(job_type, account_id="", payload=None, account_ids=None):
    clean_account_ids = [safe_text(item) for item in (account_ids or []) if safe_text(item)]
    account = get_onexbet_account(account_id) if account_id else None
    if account_id and safe_text(account_id) != "__all__" and not account:
        raise ValueError("Аккаунт 1xPartners не найден.")
    if clean_account_ids:
        available = {item["id"] for item in get_onexbet_accounts()}
        invalid = [item for item in clean_account_ids if item not in available]
        if invalid:
            raise ValueError(f"Аккаунты 1xPartners не найдены: {', '.join(invalid)}.")
    current_job = get_onex_agent_job()
    if current_job.get("status") == "pending":
        raise RuntimeError("У локального агента уже есть незавершённая задача.")
    if clean_account_ids:
        job_label = f"{len(clean_account_ids)} cabinets"
    else:
        job_label = safe_text((account or {}).get("label")) or safe_text(account_id)
    job = {
        "id": uuid.uuid4().hex,
        "type": safe_text(job_type),
        "status": "pending",
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": payload or {},
        "account_id": safe_text(account_id),
        "account_label": job_label,
        "account_ids": clean_account_ids,
    }
    save_onex_agent_job(job)
    append_onex_agent_log(f"Старт задачи {safe_text(job_type)} для {job_label}.", kind="start")
    save_onex_agent_state(
        job_status="pending",
        job_message=f"Ожидаю локальный агент для задачи {safe_text(job_type)}.",
        current_account=safe_text(account_id),
        current_account_label=job_label,
    )
    return job


def require_onex_agent_api_key(*values: str):
    normalized = [safe_text(value) for value in values if safe_text(value)]
    if safe_text(ONEXBET_AGENT_API_KEY) not in normalized:
        raise HTTPException(status_code=401, detail="Invalid agent api key")


def get_onexbet_accounts():
    ensure_upload_dir()
    payload = read_json_file(ONEXBET_ACCOUNTS_PATH, default={})
    result = []
    for item in payload.get("accounts") or []:
        account_id = safe_text(item.get("id"))
        if not account_id:
            continue
        result.append({
            "id": account_id,
            "label": safe_text(item.get("label")) or account_id,
            "login": safe_text(item.get("login")),
            "password": safe_text(item.get("password")),
        })
    return result


def get_onexbet_account(account_id):
    clean_id = safe_text(account_id)
    for item in get_onexbet_accounts():
        if item["id"] == clean_id:
            return item
    return None


def get_onexbet_storage_state_path(account_id):
    return os.path.join(ONEXBET_SESSION_DIR, f"{safe_text(account_id)}_storage_state.json")


def get_onexbet_cookies_path(account_id):
    return os.path.join(ONEXBET_SESSION_DIR, f"{safe_text(account_id)}_cookies.json")


def get_onexbet_profile_dir(account_id):
    return os.path.join(ONEXBET_SESSION_DIR, f"{safe_text(account_id)}_profile")


def onexbet_profile_exists(account_id):
    profile_dir = get_onexbet_profile_dir(account_id)
    return os.path.isdir(profile_dir) and bool(os.listdir(profile_dir))


def onexbet_session_exists(account_id):
    path = get_onexbet_storage_state_path(account_id)
    return (os.path.exists(path) and os.path.getsize(path) > 32) or onexbet_profile_exists(account_id)


def save_onexbet_server_session(account_id, storage_state_payload=None, cookies_payload=None):
    clean_id = safe_text(account_id)
    if not clean_id:
        raise ValueError("Account id is required.")
    storage_path = get_onexbet_storage_state_path(clean_id)
    cookies_path = get_onexbet_cookies_path(clean_id)
    if isinstance(storage_state_payload, dict):
        write_json_file(storage_path, storage_state_payload)
    if isinstance(cookies_payload, list):
        write_json_file(cookies_path, cookies_payload)
    return {
        "storage_state_exists": os.path.exists(storage_path) and os.path.getsize(storage_path) > 32,
        "cookies_exists": os.path.exists(cookies_path) and os.path.getsize(cookies_path) > 2,
    }


def refresh_onexbet_server_session(account):
    account_id = safe_text(account.get("id"))
    storage_state_path = get_onexbet_storage_state_path(account_id)
    cookies_path = get_onexbet_cookies_path(account_id)
    if not (os.path.exists(storage_state_path) and os.path.getsize(storage_state_path) > 32):
        return False
    browser = None
    context = None
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(
                storage_state=storage_state_path,
                accept_downloads=True,
                locale="ru-RU",
                ignore_https_errors=True,
            )
            context.set_default_timeout(45000)
            context.set_default_navigation_timeout(60000)
            page = context.new_page()
            page.goto("https://1xpartners.com/ru/partner/reports/players", wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2500)
            dismiss_onex_blockers(page)
            if not detect_onexbet_report_session(page):
                return False
            context.storage_state(path=storage_state_path)
            write_json_file(cookies_path, context.cookies())
            return True
    finally:
        try:
            if context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass


def run_onexbet_server_keepalive(account_id):
    clean_id = safe_text(account_id)
    thread_key = clean_id
    try:
        while True:
            account = get_onexbet_account(clean_id)
            if not account or not onexbet_session_exists(clean_id):
                break
            try:
                ok = refresh_onexbet_server_session(account)
                if not ok:
                    append_onex_agent_log(f"Server session для {safe_text(account.get('label')) or clean_id} требует новую captcha.", kind="info")
                    break
            except Exception as exc:
                error_text = safe_text(exc)
                if "Executable doesn't exist" in error_text or "playwright install" in error_text:
                    append_onex_agent_log(
                        f"Server keep-alive для {safe_text(account.get('label')) or clean_id} пропущен: на Render не установлен браузер Playwright.",
                        kind="info",
                    )
                    break
                append_onex_agent_log(
                    f"Keep-alive server session для {safe_text(account.get('label')) or clean_id} остановлен: {error_text}",
                    kind="info",
                )
                break
            time.sleep(ONEXBET_KEEPALIVE_INTERVAL_SECONDS)
    finally:
        with ONEXBET_KEEPALIVE_LOCK:
            existing = ONEXBET_KEEPALIVE_THREADS.get(thread_key)
            if existing is threading.current_thread():
                ONEXBET_KEEPALIVE_THREADS.pop(thread_key, None)


def ensure_onexbet_server_keepalive(account_id):
    clean_id = safe_text(account_id)
    if not clean_id:
        return
    with ONEXBET_KEEPALIVE_LOCK:
        worker = ONEXBET_KEEPALIVE_THREADS.get(clean_id)
        if worker and worker.is_alive():
            return
        worker = threading.Thread(
            target=run_onexbet_server_keepalive,
            args=(clean_id,),
            name=f"onexbet-server-keepalive-{clean_id}",
            daemon=True,
        )
        ONEXBET_KEEPALIVE_THREADS[clean_id] = worker
        worker.start()


def get_onexbet_status():
    ensure_upload_dir()
    with ONEXBET_STATUS_LOCK:
        payload = read_json_file(ONEXBET_STATUS_PATH, default={})
    if not isinstance(payload, dict):
        payload = {}
    current_account = safe_text(payload.get("current_account"))
    if current_account:
        payload["storage_state_exists"] = onexbet_session_exists(current_account)
        payload["storage_state_path"] = get_onexbet_storage_state_path(current_account)
        payload["profile_dir"] = get_onexbet_profile_dir(current_account)
        payload["profile_exists"] = onexbet_profile_exists(current_account)
        if payload.get("session_saved"):
            payload["session_saved"] = payload["storage_state_exists"]
    else:
        payload["storage_state_exists"] = False
        payload["profile_exists"] = False
    payload.setdefault("mode", "session_auth")
    payload.setdefault("status", "idle")
    payload.setdefault("message", "Сессия ещё не сохранена.")
    payload.setdefault("needs_user_action", False)
    payload.setdefault("session_saved", False)
    return payload


def save_onexbet_status(**updates):
    ensure_upload_dir()
    with ONEXBET_STATUS_LOCK:
        payload = read_json_file(ONEXBET_STATUS_PATH, default={})
        if not isinstance(payload, dict):
            payload = {}
        payload.update(updates)
        payload["updated_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        write_json_file(ONEXBET_STATUS_PATH, payload)
    return payload


def detect_onexbet_logged_in(page):
    try:
        current_url = safe_text(page.url).lower()
    except Exception:
        current_url = ""
    if "/sign-in" in current_url or "/login" in current_url:
        return False
    try:
        cookies = page.context.cookies("https://1xpartners.com")
    except Exception:
        cookies = []
    return any(safe_text(item.get("name")) for item in cookies)


def detect_onexbet_report_session(page):
    try:
        current_url = safe_text(page.url).lower()
    except Exception:
        current_url = ""
    if "/sign-in" in current_url or "/login" in current_url:
        return False
    try:
        cookies = page.context.cookies("https://1xpartners.com")
    except Exception:
        cookies = []
    return bool(cookies) and "1xpartners.com" in current_url


def run_onexbet_manual_auth(account):
    account_id = account["id"]
    account_label = account["label"]
    save_onexbet_status(
        mode="session_auth",
        status="running",
        message="Открываю окно 1xPartners для ручного входа.",
        error="",
        needs_user_action=True,
        session_saved=False,
        current_account=account_id,
        current_account_label=account_label,
        started_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        finished_at="",
        selected_accounts=[{"id": account_id, "label": account_label}],
    )
    context = None
    try:
        with sync_playwright() as playwright:
            profile_dir = get_onexbet_profile_dir(account_id)
            context = playwright.chromium.launch_persistent_context(
                profile_dir,
                headless=False,
                accept_downloads=True,
                locale="ru-RU",
                ignore_https_errors=True,
            )
            context.set_default_timeout(90000)
            context.set_default_navigation_timeout(120000)
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(ONEXBET_LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
            for selector in ["input[name='login']", "input[type='text']", "input[autocomplete='username']"]:
                try:
                    locator = page.locator(selector).first
                    if locator.count():
                        locator.fill(account["login"], timeout=3000)
                        break
                except Exception:
                    continue
            for selector in ["input[name='password']", "input[type='password']", "input[autocomplete='current-password']"]:
                try:
                    locator = page.locator(selector).first
                    if locator.count():
                        locator.fill(account["password"], timeout=3000)
                        break
                except Exception:
                    continue
            save_onexbet_status(
                status="waiting_for_user",
                message="Браузер открыт. Нажми LOG IN, пройди captcha и дождись входа в кабинет.",
                needs_user_action=True,
            )
            deadline = time.time() + 900
            while time.time() < deadline:
                if detect_onexbet_logged_in(page):
                    storage_state_path = get_onexbet_storage_state_path(account_id)
                    cookies_path = get_onexbet_cookies_path(account_id)
                    context.storage_state(path=storage_state_path)
                    write_json_file(cookies_path, context.cookies())
                    save_onexbet_status(
                        status="ready",
                        message="Сессия 1xPartners сохранена в постоянный профиль браузера и готова к выгрузке.",
                        needs_user_action=False,
                        session_saved=True,
                        finished_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                    )
                    return
                time.sleep(2)
            save_onexbet_status(
                status="timeout",
                message="Не дождался завершения входа за 15 минут. Запусти окно снова.",
                error="Login timeout.",
                needs_user_action=False,
                session_saved=onexbet_session_exists(account_id),
                finished_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            )
    except Exception as exc:
        save_onexbet_status(
            status="error",
            message="Не удалось открыть браузер или сохранить session state.",
            error=safe_text(exc),
            needs_user_action=False,
            session_saved=onexbet_session_exists(account_id),
            finished_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        )
    finally:
        try:
            if context:
                context.close()
        except Exception:
            pass
        global ONEXBET_SESSION_THREAD
        with ONEXBET_SESSION_LOCK:
            ONEXBET_SESSION_THREAD = None


def start_onexbet_manual_auth(account_id):
    account = get_onexbet_account(account_id)
    if not account:
        raise ValueError("Аккаунт 1xPartners не найден.")
    if not account.get("login") or not account.get("password"):
        raise ValueError("Для выбранного аккаунта не заполнен логин или пароль.")
    global ONEXBET_SESSION_THREAD
    with ONEXBET_SESSION_LOCK:
        if ONEXBET_SESSION_THREAD and ONEXBET_SESSION_THREAD.is_alive():
            raise RuntimeError("Окно авторизации уже запущено.")
        worker = threading.Thread(
            target=run_onexbet_manual_auth,
            args=(account,),
            name=f"onexbet-auth-{account['id']}",
            daemon=True,
        )
        ONEXBET_SESSION_THREAD = worker
        worker.start()
    return account


def validate_onexbet_session(account):
    account_id = safe_text(account.get("id"))
    legacy = load_legacy_onexbet_module()
    profile_dir = get_onexbet_profile_dir(account_id)
    storage_state_path = get_onexbet_storage_state_path(account_id)
    context = None
    try:
        with sync_playwright() as playwright:
            context = playwright.chromium.launch_persistent_context(
                profile_dir,
                headless=False,
                accept_downloads=True,
                locale="ru-RU",
                ignore_https_errors=True,
            )
            context.set_default_timeout(45000)
            context.set_default_navigation_timeout(60000)
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(legacy.PLAYERS_REPORT_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)
            if not legacy.is_authenticated(page):
                raise RuntimeError("Сессия ещё не подтверждена. Заверши captcha и дождись входа в кабинет.")
            context.storage_state(path=storage_state_path)
            write_json_file(get_onexbet_cookies_path(account_id), context.cookies())
            ensure_onexbet_server_keepalive(account_id)
            save_onexbet_status(
                status="ready",
                message="Проверка успешна. Вход подтверждён, можно запускать парсер.",
                error="",
                needs_user_action=False,
                session_saved=True,
                current_account=account_id,
                current_account_label=safe_text(account.get("label")) or account_id,
                finished_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            )
            append_onexbet_log(f"Проверка captcha подтверждена для {safe_text(account.get('label')) or account_id}.")
            return get_onexbet_status()
    finally:
        try:
            if context:
                context.close()
        except Exception:
            pass


def clear_onexbet_session(account_id):
    clean_id = safe_text(account_id)
    if not clean_id:
        raise ValueError("Выбери аккаунт.")
    for path in [get_onexbet_storage_state_path(clean_id), get_onexbet_cookies_path(clean_id)]:
        if os.path.exists(path):
            os.remove(path)
    profile_dir = get_onexbet_profile_dir(clean_id)
    if os.path.isdir(profile_dir):
        shutil.rmtree(profile_dir)
    account = get_onexbet_account(clean_id) or {}
    return save_onexbet_status(
        mode="session_auth",
        status="idle",
        message="Сохранённая сессия и профиль 1x очищены.",
        error="",
        needs_user_action=False,
        session_saved=False,
        current_account=clean_id,
        current_account_label=safe_text(account.get("label")) or clean_id,
        finished_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
    )


def append_onexbet_log(message, kind="info"):
    payload = get_onexbet_status()
    logs = list(payload.get("logs") or [])
    logs.append({
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "kind": safe_text(kind) or "info",
        "message": safe_text(message),
    })
    return save_onexbet_status(logs=logs[-100:])


def load_legacy_onexbet_module():
    global ONEXBET_LEGACY_MODULE
    if ONEXBET_LEGACY_MODULE is not None:
        return ONEXBET_LEGACY_MODULE
    import dotenv.main

    def fake_find_dotenv(*args, **kwargs):
        return os.path.join(os.getcwd(), ".env")

    dotenv.main.find_dotenv = fake_find_dotenv
    module_path = os.path.join(os.getcwd(), "__pycache__", "parser_1xbet_halfmonth.cpython-313.pyc")
    loader = importlib.machinery.SourcelessFileLoader("parser_1xbet_halfmonth_legacy", module_path)
    spec = importlib.util.spec_from_loader("parser_1xbet_halfmonth_legacy", loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    module.HEADLESS = False
    ONEXBET_LEGACY_MODULE = module
    return module


def build_onex_parser_source_name(account, period):
    return build_partner_source_name(
        safe_text(period.get("date_start")),
        safe_text(period.get("date_end")),
        prefix=f"1x_parser/{safe_text(account.get('label')) or safe_text(account.get('id'))}",
    )


def dismiss_onex_blockers(page):
    actions = [
        ("cookie_ok", [
            "button:has-text('Ok')",
            "button:has-text('OK')",
            "button:has-text('Ок')",
            "button:has-text('Согласен')",
            "button:has-text('Принять')",
            "button:has-text('Хорошо')",
        ]),
        ("email_modal_close", [
            "button[aria-label*='close' i]",
            "button[aria-label*='закрыть' i]",
            "[role='dialog'] button",
            ".modal button.close",
            ".modal__close",
            ".popup__close",
            ".ReactModal__Content button",
            "button:has-text('×')",
            "button:has-text('✕')",
            "text=×",
            "text=✕",
        ]),
    ]
    closed_any = False
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(300)
    except Exception:
        pass
    for label, selectors in actions:
        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if locator.count() and locator.is_visible():
                    locator.click(timeout=3000)
                    page.wait_for_timeout(500)
                    append_onexbet_log(f"Закрыл блокирующий элемент 1x: {label}.")
                    closed_any = True
                    break
            except Exception:
                continue
    try:
        forced = page.evaluate(
            """
            () => {
                const textContains = (el, text) => ((el.innerText || el.textContent || '').toLowerCase().includes(text));
                const clickables = Array.from(document.querySelectorAll('button, [role="button"], .close, .modal__close, .popup__close, [aria-label]'));
                let clicked = false;
                for (const el of clickables) {
                    const label = ((el.innerText || el.textContent || '') + ' ' + (el.getAttribute('aria-label') || '')).toLowerCase();
                    if (label.includes('×') || label.includes('✕') || label.includes('close') || label.includes('закры')) {
                        try { el.click(); clicked = true; } catch (e) {}
                    }
                }
                const nodes = Array.from(document.querySelectorAll('div, section, aside'));
                for (const node of nodes) {
                    const text = (node.innerText || node.textContent || '').toLowerCase();
                    if (text.includes('подтвердите свой email') || text.includes('полный доступ') || text.includes('на указанный адрес мы отправили письмо')) {
                        try {
                            node.remove();
                            clicked = true;
                        } catch (e) {}
                    }
                }
                for (const el of Array.from(document.querySelectorAll('[class*="modal"], [class*="popup"], [class*="overlay"], [class*="backdrop"]'))) {
                    const text = (el.innerText || el.textContent || '').toLowerCase();
                    if (text.includes('подтвердите свой email') || text.includes('полный доступ') || text.includes('мы отправили письмо')) {
                        try {
                            el.remove();
                            clicked = true;
                        } catch (e) {}
                    }
                }
                return clicked;
            }
            """
        )
        if forced:
            page.wait_for_timeout(400)
            append_onexbet_log("Принудительно закрыл блокирующую модалку 1x через JS.")
            closed_any = True
    except Exception:
        pass
    return closed_any


def build_onex_parser_row_identity(row):
    return (
        safe_text(getattr(row, "account_id", "")).strip().lower(),
        safe_text(getattr(row, "player_id", "")).strip().lower(),
        safe_text(getattr(row, "registration_date", "")).strip(),
    )


def replace_onex_parser_rows(source_name, account, rows_to_insert):
    ensure_onex_parser_table()
    db = SessionLocal()
    try:
        existing_rows = db.query(OnexParserRow).filter(OnexParserRow.source_name == safe_text(source_name)).all()
        merged_map = {
            build_onex_parser_row_identity(row): row
            for row in existing_rows
            if any(build_onex_parser_row_identity(row))
        }
        for row in (rows_to_insert or []):
            merged_map[build_onex_parser_row_identity(row)] = row
        db.query(OnexParserRow).filter(OnexParserRow.source_name == safe_text(source_name)).delete()
        payload = [
            {
                "account_id": safe_text(getattr(row, "account_id", "")),
                "account_label": safe_text(getattr(row, "account_label", "")),
                "source_name": safe_text(getattr(row, "source_name", "")),
                "report_date": safe_text(getattr(row, "report_date", "")),
                "period_start": safe_text(getattr(row, "period_start", "")),
                "period_end": safe_text(getattr(row, "period_end", "")),
                "period_label": safe_text(getattr(row, "period_label", "")),
                "registration_date": safe_text(getattr(row, "registration_date", "")),
                "country": safe_text(getattr(row, "country", "")),
                "sub_id": safe_text(getattr(row, "sub_id", "")),
                "player_id": safe_text(getattr(row, "player_id", "")),
                "deposit_amount": safe_number(getattr(row, "deposit_amount", 0)),
                "bet_amount": safe_number(getattr(row, "bet_amount", 0)),
                "company_income": safe_number(getattr(row, "company_income", 0)),
                "cpa_amount": safe_number(getattr(row, "cpa_amount", 0)),
                "hold_time": safe_text(getattr(row, "hold_time", "")),
                "blocked": safe_text(getattr(row, "blocked", "")),
                "created_at": datetime.utcnow(),
            }
            for row in merged_map.values()
        ]
        if payload:
            db.bulk_insert_mappings(OnexParserRow, payload)
        db.commit()
    finally:
        db.close()
    save_onexbet_status(
        last_source_name=safe_text(source_name),
        current_account=safe_text(account.get("id")),
        current_account_label=safe_text(account.get("label")) or safe_text(account.get("id")),
    )


def import_onex_parser_rows_async(account, source_name, rows):
    def worker():
        with ONEX_PARSER_IMPORT_LOCK:
            try:
                replace_onex_parser_rows(source_name, account, rows)
                bump_live_data_version()
                append_onex_agent_log(f"Фоновый импорт завершён: {len(rows)} строк для {safe_text(account.get('label'))}.", kind="success")
                save_onex_agent_state(
                    connected=True,
                    status="ready",
                    message=f"Локальный агент загрузил {len(rows)} строк в 1x Parser.",
                    current_account=safe_text(account.get("id")),
                    current_account_label=safe_text(account.get("label")),
                    job_status="idle",
                    job_message="",
                    session_saved=True,
                    profile_exists=True,
                    last_count=len(rows),
                    last_seen_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                    last_source_name=safe_text(source_name),
                    last_error="",
                )
            except Exception as exc:
                append_onex_agent_log(f"Ошибка фонового импорта 1x Parser: {safe_text(exc)}.", kind="error")
                save_onex_agent_state(
                    connected=True,
                    status="error",
                    message="Фоновый импорт 1x Parser завершился ошибкой.",
                    current_account=safe_text(account.get("id")),
                    current_account_label=safe_text(account.get("label")),
                    last_error=safe_text(exc),
                    last_seen_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                )

    threading.Thread(target=worker, daemon=True).start()


def get_onex_parser_rows(account_id="", period_label="", search=""):
    ensure_onex_parser_table()
    db = SessionLocal()
    try:
        query = db.query(OnexParserRow)
        if account_id:
            query = query.filter(OnexParserRow.account_id == safe_text(account_id))
        if period_label:
            query = query.filter(OnexParserRow.period_label == safe_text(period_label))
        if search:
            search_pattern = f"%{safe_text(search)}%"
            query = query.filter(or_(
                OnexParserRow.account_label.ilike(search_pattern),
                OnexParserRow.player_id.ilike(search_pattern),
                OnexParserRow.sub_id.ilike(search_pattern),
                OnexParserRow.country.ilike(search_pattern),
                OnexParserRow.period_label.ilike(search_pattern),
                OnexParserRow.source_name.ilike(search_pattern),
            ))
        return query.order_by(OnexParserRow.registration_date.desc(), OnexParserRow.id.desc()).all()
    finally:
        db.close()


def get_onex_parser_period_options():
    ensure_onex_parser_table()
    db = SessionLocal()
    try:
        values = db.query(OnexParserRow.period_label).distinct().all()
    finally:
        db.close()
    result = []
    for item in values:
        value = safe_text(item[0])
        if value:
            result.append(value)
    return sorted(set(result), reverse=True)


def partner_rows_to_onex_parser_rows(rows, account):
    result = []
    for row in rows:
        result.append(OnexParserRow(
            account_id=safe_text(account.get("id")),
            account_label=safe_text(account.get("label")) or safe_text(account.get("id")),
            source_name=safe_text(getattr(row, "source_name", "")),
            report_date=safe_text(getattr(row, "report_date", "")),
            period_start=safe_text(getattr(row, "period_start", "")),
            period_end=safe_text(getattr(row, "period_end", "")),
            period_label=safe_text(getattr(row, "period_label", "")),
            registration_date=safe_text(getattr(row, "registration_date", "")),
            country=safe_text(getattr(row, "country", "")),
            sub_id=safe_text(getattr(row, "sub_id", "")),
            player_id=safe_text(getattr(row, "player_id", "")),
            deposit_amount=safe_number(getattr(row, "deposit_amount", 0)),
            bet_amount=safe_number(getattr(row, "bet_amount", 0)),
            company_income=safe_number(getattr(row, "company_income", 0)),
            cpa_amount=safe_number(getattr(row, "cpa_amount", 0)),
            hold_time=safe_text(getattr(row, "hold_time", "")),
            blocked=safe_text(getattr(row, "blocked", "")),
        ))
    return result


def apply_onex_period_metadata(rows, period_data):
    if not isinstance(period_data, dict):
        return rows
    report_date = safe_text(period_data.get("report_date") or period_data.get("date_end"))
    period_start = safe_text(period_data.get("date_start"))
    period_end = safe_text(period_data.get("date_end"))
    period_label = safe_text(period_data.get("period_label"))
    for row in rows or []:
        row.report_date = report_date
        row.period_start = period_start
        row.period_end = period_end
        row.period_label = period_label
    return rows


def onex_parser_rows_from_payload(rows_payload):
    result = []
    for item in rows_payload or []:
        if not isinstance(item, dict):
            continue
        result.append(OnexParserRow(
            account_id=safe_text(item.get("account_id")),
            account_label=safe_text(item.get("account_label")),
            source_name=safe_text(item.get("source_name")),
            report_date=safe_text(item.get("report_date")),
            period_start=safe_text(item.get("period_start")),
            period_end=safe_text(item.get("period_end")),
            period_label=safe_text(item.get("period_label")),
            registration_date=safe_text(item.get("registration_date")),
            country=safe_text(item.get("country")),
            sub_id=safe_text(item.get("sub_id")),
            player_id=safe_text(item.get("player_id")),
            deposit_amount=safe_number(item.get("deposit_amount")),
            bet_amount=safe_number(item.get("bet_amount")),
            company_income=safe_number(item.get("company_income")),
            cpa_amount=safe_number(item.get("cpa_amount")),
            hold_time=safe_text(item.get("hold_time")),
            blocked=safe_text(item.get("blocked")),
        ))
    return result


def get_current_onex_export_plan(today=None):
    reference_date = today or get_crm_local_date()
    current_period = get_half_month_period(reference_date)
    period_start = datetime.strptime(current_period["date_start"], "%Y-%m-%d").date()
    period_end = datetime.strptime(current_period["date_end"], "%Y-%m-%d").date()
    today_str = reference_date.strftime("%Y-%m-%d")
    yesterday = reference_date - timedelta(days=1)
    ranges = []
    if yesterday >= period_start:
        ranges.append({
            "date_start": period_start.strftime("%Y-%m-%d"),
            "date_end": yesterday.strftime("%Y-%m-%d"),
            "period_label": f"{period_start.strftime('%d')}-{yesterday.strftime('%d.%m.%Y')}",
            "kind": "history",
        })
    ranges.append({
        "date_start": today_str,
        "date_end": today_str,
        "period_label": f"{reference_date.strftime('%d')}-{reference_date.strftime('%d.%m.%Y')}",
        "kind": "today",
    })
    return {
        "current_period": {
            "date_start": current_period["date_start"],
            "date_end": current_period["date_end"],
            "period_label": current_period["period_label"],
        },
        "ranges": ranges,
    }


def merge_partner_rows(rows):
    merged = {}
    for row in rows:
        key = build_partner_row_identity(row)
        existing = merged.get(key)
        if not existing:
            merged[key] = row
            continue
        if safe_number(getattr(row, "deposit_amount", 0)) >= safe_number(getattr(existing, "deposit_amount", 0)):
            existing.deposit_amount = getattr(row, "deposit_amount", existing.deposit_amount)
        if safe_number(getattr(row, "bet_amount", 0)) >= safe_number(getattr(existing, "bet_amount", 0)):
            existing.bet_amount = getattr(row, "bet_amount", existing.bet_amount)
        if safe_number(getattr(row, "company_income", 0)) >= safe_number(getattr(existing, "company_income", 0)):
            existing.company_income = getattr(row, "company_income", existing.company_income)
        if safe_number(getattr(row, "cpa_amount", 0)) >= safe_number(getattr(existing, "cpa_amount", 0)):
            existing.cpa_amount = getattr(row, "cpa_amount", existing.cpa_amount)
        if safe_text(getattr(row, "hold_time", "")):
            existing.hold_time = getattr(row, "hold_time", existing.hold_time)
        if safe_text(getattr(row, "blocked", "")):
            existing.blocked = getattr(row, "blocked", existing.blocked)
    return list(merged.values())


def set_onex_date_range(page, export_period):
    legacy = load_legacy_onexbet_module()
    legacy.open_date_picker(page)
    start_value = safe_text((export_period or {}).get("date_start"))
    end_value = safe_text((export_period or {}).get("date_end"))
    if not start_value or not end_value:
        raise RuntimeError("1x export period is incomplete.")

    def find_date_inputs_via_dom():
        handles = page.evaluate_handle(
            """
            () => {
                const isVisible = (el) => {
                    if (!el) return false;
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return style.visibility !== 'hidden'
                        && style.display !== 'none'
                        && rect.width > 40
                        && rect.height > 18;
                };
                const looksLikeDate = (el) => {
                    const value = String(el.value || '').trim();
                    const placeholder = String(el.placeholder || '').trim().toLowerCase();
                    const aria = String(el.getAttribute('aria-label') || '').trim().toLowerCase();
                    const name = String(el.getAttribute('name') || '').trim().toLowerCase();
                    const id = String(el.getAttribute('id') || '').trim().toLowerCase();
                    const type = String(el.getAttribute('type') || '').trim().toLowerCase();
                    const text = [placeholder, aria, name, id].join(' ');
                    return /^\\d{4}-\\d{2}-\\d{2}$/.test(value)
                        || type === 'date'
                        || text.includes('date')
                        || text.includes('дата')
                        || text.includes('нач')
                        || text.includes('кон');
                };
                const inputs = Array.from(document.querySelectorAll('input'))
                    .filter((el) => isVisible(el) && looksLikeDate(el))
                    .map((el) => {
                        const rect = el.getBoundingClientRect();
                        return {
                            x: rect.x,
                            y: rect.y,
                            value: String(el.value || ''),
                            id: String(el.getAttribute('id') || ''),
                            name: String(el.getAttribute('name') || ''),
                            placeholder: String(el.getAttribute('placeholder') || ''),
                            aria: String(el.getAttribute('aria-label') || ''),
                            handle: el,
                        };
                    })
                    .sort((a, b) => a.y - b.y || a.x - b.x);
                return inputs.map((item) => item.handle);
            }
            """
        )
        properties = handles.get_properties()
        result = []
        for prop in properties.values():
            element = prop.as_element()
            if element:
                result.append(element)
        try:
            handles.dispose()
        except Exception:
            pass
        return result

    def month_labels(target_date):
        month_names = {
            1: ["январь", "january"],
            2: ["февраль", "february"],
            3: ["март", "march"],
            4: ["апрель", "april"],
            5: ["май", "may"],
            6: ["июнь", "june"],
            7: ["июль", "july"],
            8: ["август", "august"],
            9: ["сентябрь", "september"],
            10: ["октябрь", "october"],
            11: ["ноябрь", "november"],
            12: ["декабрь", "december"],
        }
        names = month_names.get(target_date.month, [])
        return [f"{name} {target_date.year}" for name in names]

    def click_calendar_range():
        try:
            start_date = datetime.strptime(start_value, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_value, "%Y-%m-%d").date()
        except Exception:
            return

        def find_popup_handle():
            handle = page.evaluate_handle(
                """
                () => {
                    const visible = (el) => {
                        if (!el) return false;
                        const style = window.getComputedStyle(el);
                        const rect = el.getBoundingClientRect();
                        return style.visibility !== 'hidden'
                            && style.display !== 'none'
                            && rect.width > 180
                            && rect.height > 120;
                    };
                    const candidates = Array.from(document.querySelectorAll('body *'))
                        .filter((el) => visible(el) && /\\b20\\d{2}\\b/.test(el.textContent || ''))
                        .sort((a, b) => {
                            const ra = a.getBoundingClientRect();
                            const rb = b.getBoundingClientRect();
                            return (rb.width * rb.height) - (ra.width * ra.height);
                        });
                    return candidates[0] || null;
                }
                """
            )
            element = handle.as_element()
            if not element:
                try:
                    handle.dispose()
                except Exception:
                    pass
                return None
            return element

        popup = find_popup_handle()
        if not popup:
            return

        def popup_text():
            try:
                return safe_text(popup.inner_text(timeout=1000)).lower()
            except Exception:
                return ""

        def click_nav(next_button=True):
            try:
                buttons = popup.locator("button").all()
            except Exception:
                buttons = []
            candidates = []
            for item in buttons:
                try:
                    if not item.is_visible():
                        continue
                    box = item.bounding_box() or {}
                    text_value = safe_text(item.inner_text(timeout=500)).strip()
                    aria = safe_text(item.get_attribute("aria-label")).lower()
                    width = float(box.get("width", 0))
                    height = float(box.get("height", 0))
                    if width < 20 or height < 20:
                        continue
                    score = 0
                    if any(marker in aria for marker in ["next", "след", "дал", "right", "впер"]):
                        score += 10 if next_button else -10
                    if any(marker in aria for marker in ["prev", "пред", "left", "назад"]):
                        score += 10 if not next_button else -10
                    candidates.append((float(box.get("x", 0)), float(box.get("y", 0)), text_value, item, score))
                except Exception:
                    continue
            if not candidates:
                return False
            candidates.sort(key=lambda row: (row[1], row[0]))
            target = candidates[-1][3] if next_button else candidates[0][3]
            try:
                target.click(timeout=3000)
                page.wait_for_timeout(350)
                return True
            except Exception:
                return False

        target_labels = month_labels(start_date)
        for _ in range(12):
            text_value = popup_text()
            if any(label in text_value for label in target_labels):
                break
            if start_date > end_date:
                break
            if not click_nav(next_button=True):
                break

        def click_day(day_value):
            js = """
            (root, dayValue) => {
                const visible = (el) => {
                    if (!el) return false;
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return style.visibility !== 'hidden'
                        && style.display !== 'none'
                        && rect.width > 18
                        && rect.height > 18;
                };
                const text = String(dayValue);
                const rootRect = root.getBoundingClientRect();
                const nodes = Array.from(root.querySelectorAll('*'))
                    .filter((el) => visible(el))
                    .filter((el) => (el.textContent || '').trim() === text)
                    .filter((el) => !el.closest('[disabled], .disabled, .react-datepicker__day--disabled'))
                    .map((el) => {
                        const rect = el.getBoundingClientRect();
                        const cls = String(el.className || '').toLowerCase();
                        const id = String(el.id || '').toLowerCase();
                        const role = String(el.getAttribute('role') || '').toLowerCase();
                        const aria = String(el.getAttribute('aria-label') || '').toLowerCase();
                        const clickable = el.closest('button,[role="button"],td,th,div,span');
                        const clickableRect = clickable ? clickable.getBoundingClientRect() : rect;
                        const score =
                            ((cls.includes('day') || cls.includes('date') || cls.includes('calendar')) ? 20 : 0) +
                            ((id.includes('day') || id.includes('date') || id.includes('calendar')) ? 15 : 0) +
                            ((role.includes('button') || role.includes('gridcell')) ? 12 : 0) +
                            ((aria.includes(text) || aria.includes('day') || aria.includes('date')) ? 8 : 0) +
                            ((clickableRect.width >= 24 && clickableRect.width <= 48 && clickableRect.height >= 24 && clickableRect.height <= 48) ? 12 : 0) +
                            ((clickableRect.y > rootRect.y + 50) ? 8 : 0);
                        return { el, clickable: clickable || el, rect: clickableRect, score };
                    })
                    .filter((item) =>
                        item.rect.width >= 20 &&
                        item.rect.width <= 60 &&
                        item.rect.height >= 20 &&
                        item.rect.height <= 60 &&
                        item.rect.y > rootRect.y + 40
                    );
                nodes.sort((a, b) => {
                    if (b.score !== a.score) return b.score - a.score;
                    return a.rect.y - b.rect.y || a.rect.x - b.rect.x;
                });
                const target = nodes[0] ? nodes[0].clickable : null;
                if (!target) return false;
                target.click();
                return true;
            }
            """
            try:
                return bool(popup.evaluate(js, day_value))
            except Exception:
                return False

        clicked_start = click_day(start_date.day)
        page.wait_for_timeout(250)
        clicked_end = click_day(end_date.day) if end_date != start_date else clicked_start
        page.wait_for_timeout(400)
        append_onexbet_log(
            f"1x calendar commit {'ok' if (clicked_start and clicked_end) else 'skip'} "
            f"for {start_value} -> {end_value}"
        )

    visible_inputs = []
    selectors = [
        "input[placeholder*='Начало']",
        "input[placeholder*='Конец']",
        "input[placeholder*='2026']",
        "input[value*='2026-']",
        "input[value*='2025-']",
        "input[aria-label*='дата' i]",
        "input[aria-label*='date' i]",
        "input[name*='date' i]",
        "input[id*='date' i]",
        "input[type='date']",
        "input[readonly][value*='-']",
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector)
            for index in range(locator.count()):
                item = locator.nth(index)
                if not item.is_visible():
                    continue
                try:
                    input_value = safe_text(item.input_value(timeout=500))
                except Exception:
                    input_value = ""
                try:
                    placeholder = safe_text(item.get_attribute("placeholder"))
                except Exception:
                    placeholder = ""
                try:
                    input_type = safe_text(item.get_attribute("type")).lower()
                except Exception:
                    input_type = ""
                looks_like_date = (
                    bool(re.match(r"^\d{4}-\d{2}-\d{2}$", input_value))
                    or bool(re.match(r"^\d{4}-\d{2}-\d{2}$", placeholder))
                    or "нач" in placeholder.lower()
                    or "кон" in placeholder.lower()
                    or "date" in placeholder.lower()
                    or "дата" in placeholder.lower()
                )
                if not looks_like_date and input_type != "date":
                    continue
                box = item.bounding_box() or {}
                visible_inputs.append({
                    "locator": item,
                    "x": float(box.get("x", 0)),
                    "y": float(box.get("y", 0)),
                    "value": input_value,
                    "placeholder": placeholder,
                })
        except Exception:
            continue
    unique_inputs = []
    seen = set()
    for item in visible_inputs:
        key = (round(item["x"]), round(item["y"]))
        if key in seen:
            continue
        seen.add(key)
        unique_inputs.append(item)
    date_like_inputs = [
        item for item in unique_inputs
        if (
            bool(re.match(r"^\d{4}-\d{2}-\d{2}$", safe_text(item.get("value"))))
            or bool(re.match(r"^\d{4}-\d{2}-\d{2}$", safe_text(item.get("placeholder"))))
            or any(marker in safe_text(item.get("placeholder")).lower() for marker in ["нач", "кон", "date", "дата"])
        )
    ]
    if len(date_like_inputs) >= 2:
        unique_inputs = date_like_inputs
    if len(unique_inputs) < 2:
        dom_inputs = find_date_inputs_via_dom()
        unique_inputs = []
        for item in dom_inputs:
            try:
                box = item.bounding_box() or {}
                unique_inputs.append({
                    "locator": item,
                    "x": float(box.get("x", 0)),
                    "y": float(box.get("y", 0)),
                    "value": safe_text(item.input_value(timeout=500)),
                    "placeholder": safe_text(item.get_attribute("placeholder")),
                })
            except Exception:
                continue
    if len(unique_inputs) < 2:
        raise RuntimeError("Не найдены два видимых поля периода 1x.")
    unique_inputs.sort(key=lambda item: (item["y"], item["x"]))
    start_input = unique_inputs[0]["locator"]
    end_input = unique_inputs[1]["locator"] if len(unique_inputs) == 2 else unique_inputs[-1]["locator"]

    append_onexbet_log(f"Setting 1x period to {start_value} -> {end_value}")

    def set_input(locator, value):
        locator.click(timeout=5000)
        try:
            locator.press("Meta+A", timeout=1000)
        except Exception:
            pass
        try:
            locator.press("Control+A", timeout=1000)
        except Exception:
            pass
        try:
            locator.fill("", timeout=3000)
        except Exception:
            pass
        try:
            locator.fill(value, timeout=3000)
        except Exception:
            locator.type(value, delay=35, timeout=5000)
        try:
            locator.evaluate(
                """
                (el, val) => {
                    el.removeAttribute('readonly');
                    el.value = '';
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.value = val;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    el.dispatchEvent(new Event('blur', { bubbles: true }));
                }
                """,
                value,
            )
        except Exception:
            pass
        try:
            locator.blur(timeout=1000)
        except Exception:
            pass
        page.wait_for_timeout(700)

    set_input(start_input, start_value)
    set_input(end_input, end_value)
    click_calendar_range()
    try:
        page.mouse.click(10, 10)
        page.wait_for_timeout(500)
    except Exception:
        pass
    final_start = safe_text(start_input.input_value(timeout=3000))
    final_end = safe_text(end_input.input_value(timeout=3000))
    if final_start != start_value or final_end != end_value:
        set_input(end_input, end_value)
        try:
            page.mouse.click(10, 10)
            page.wait_for_timeout(500)
        except Exception:
            pass
    final_start = safe_text(start_input.input_value(timeout=3000))
    final_end = safe_text(end_input.input_value(timeout=3000))
    append_onexbet_log(f"1x date picker set to {final_start} -> {final_end}")
    if final_start != start_value or final_end != end_value:
        raise RuntimeError(f"1x date range mismatch after fill: {final_start} -> {final_end}")


def export_onex_csv_for_period(account, export_period):
    legacy = load_legacy_onexbet_module()
    storage_state_path = get_onexbet_storage_state_path(account["id"])
    cookies_path = get_onexbet_cookies_path(account["id"])
    browser = None
    context = None
    try:
        if not (os.path.exists(storage_state_path) and os.path.getsize(storage_state_path) > 32):
            raise RuntimeError("Server 1x session not found. Local captcha refresh is required.")
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(
                storage_state=storage_state_path,
                accept_downloads=True,
                locale="ru-RU",
                ignore_https_errors=True,
            )
            context.set_default_timeout(90000)
            context.set_default_navigation_timeout(120000)
            page = context.new_page()
            page.goto(legacy.PLAYERS_REPORT_URL, wait_until="domcontentloaded", timeout=120000)
            page.wait_for_timeout(5000)
            dismiss_onex_blockers(page)
            if not legacy.is_authenticated(page):
                raise RuntimeError("Server 1x session expired or captcha is required.")
            context.storage_state(path=storage_state_path)
            write_json_file(cookies_path, context.cookies())
            legacy.open_players_report(page)
            dismiss_onex_blockers(page)
            set_onex_date_range(page, export_period)
            legacy.check_new_players(page)
            legacy.click_generate_report(page)
            file_path = legacy.download_csv(page)
            return str(file_path)
    finally:
        try:
            if context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass


def run_onex_export(account):
    account_id = safe_text(account.get("id"))
    if not onexbet_session_exists(account_id):
        save_onexbet_status(
            status="error",
            message="Сначала сохрани сессию через Open Login.",
            error="Session state not found.",
            needs_user_action=False,
            session_saved=False,
        )
        raise RuntimeError("Server 1x session not found.")
    save_onexbet_status(
        status="running",
        message="Запускаю экспорт отчёта 1xPartners.",
        error="",
        needs_user_action=False,
        session_saved=True,
        current_account=account_id,
        current_account_label=safe_text(account.get("label")) or account_id,
    )
    append_onexbet_log(f"Старт экспорта для кабинета {safe_text(account.get('label')) or account_id}.")
    try:
        export_plan = get_current_onex_export_plan()
        current_period = export_plan["current_period"]
        source_name = build_onex_parser_source_name(account, current_period)
        combined_rows = []
        for export_period in export_plan["ranges"]:
            append_onexbet_log(
                f"Выгружаю диапазон {export_period['date_start']} - {export_period['date_end']} ({export_period['kind']})."
            )
            file_path = export_onex_csv_for_period(account, export_period)
            append_onexbet_log(f"CSV скачан: {os.path.basename(str(file_path))}")
            ext = os.path.splitext(str(file_path))[1].lower() or ".csv"
            df = read_partner_uploaded_dataframe(str(file_path), ext)
            parsed_rows = parse_partner_dataframe(
                df,
                source_name=source_name,
                cabinet_name=safe_text(account.get("label")) or account_id,
                partner_platform="1xbet",
                upload_period_data=export_period,
            )
            apply_onex_period_metadata(parsed_rows, current_period)
            append_onexbet_log(
                f"{safe_text(account.get('label')) or account_id}: диапазон {export_period['date_start']} - {export_period['date_end']}, "
                f"csv_rows={len(df.index)}, parsed_rows={len(parsed_rows)}"
            )
            combined_rows.extend(parsed_rows)
        merged_rows = merge_partner_rows(combined_rows)
        parser_rows = partner_rows_to_onex_parser_rows(merged_rows, account)
        replace_onex_parser_rows(source_name, account, parser_rows)
        ensure_onexbet_server_keepalive(account_id)
        append_onexbet_log(
            f"{safe_text(account.get('label')) or account_id}: combined_rows={len(combined_rows)}, imported_rows={len(parser_rows)}",
            kind="success" if parser_rows else "info",
        )
        save_onexbet_status(
            status="ready",
            message=f"Экспорт завершён. Загружено строк: {len(parser_rows)}.",
            error="",
            needs_user_action=False,
            session_saved=True,
            finished_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            last_count=len(parser_rows),
            period_label=safe_text(current_period.get("period_label")),
            date_start=safe_text(current_period.get("date_start")),
            date_end=safe_text(current_period.get("date_end")),
            last_source_name=source_name,
        )
        return len(parser_rows)
    except Exception as exc:
        append_onexbet_log(f"Ошибка экспорта: {exc}", kind="error")
        save_onexbet_status(
            status="error",
            message="Не удалось скачать или разобрать отчёт.",
            error=safe_text(exc),
            needs_user_action=False,
            session_saved=onexbet_session_exists(account_id),
            finished_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        )
        raise
    finally:
        global ONEXBET_SESSION_THREAD
        with ONEXBET_SESSION_LOCK:
            ONEXBET_SESSION_THREAD = None


def start_onex_export(account_id):
    account = get_onexbet_account(account_id)
    if not account:
        raise ValueError("Аккаунт 1xPartners не найден.")
    global ONEXBET_SESSION_THREAD
    with ONEXBET_SESSION_LOCK:
        if ONEXBET_SESSION_THREAD and ONEXBET_SESSION_THREAD.is_alive():
            raise RuntimeError("Сейчас уже выполняется операция 1x Parser.")
        worker = threading.Thread(
            target=run_onex_export,
            args=(account,),
            name=f"onex-export-{account['id']}",
            daemon=True,
        )
        ONEXBET_SESSION_THREAD = worker
        worker.start()
    return account


def needs_local_onex_captcha(exc):
    message = safe_text(exc).lower()
    return any(marker in message for marker in [
        "session not found",
        "session expired",
        "captcha",
        "sign-in",
        "login",
        "ещё не активна",
    ])


def run_onex_server_first_flow(account_ids):
    try:
        all_accounts = {item["id"]: item for item in get_onexbet_accounts()}
        clean_account_ids = [safe_text(item) for item in (account_ids or []) if safe_text(item)]
        target_accounts = [all_accounts[item] for item in clean_account_ids if item in all_accounts]
        if not target_accounts:
            raise RuntimeError("Не найдены аккаунты 1xPartners для запуска.")
        exported = []
        local_required = []
        failed = []
        save_onex_agent_state(
            status="running",
            connected=bool(get_onex_agent_state().get("connected")),
            paused=False,
            current_account="__all__" if len(target_accounts) > 1 else safe_text(target_accounts[0].get("id")),
            current_account_label="All cabinets" if len(target_accounts) > 1 else safe_text(target_accounts[0].get("label")),
            message="Сервер пытается сделать тихую выгрузку 1x по сохранённым сессиям.",
            job_status="running",
            job_message="Server-first export in progress.",
            last_error="",
        )
        append_onex_agent_log("Старт server-first выгрузки 1x.", kind="start")
        for account in target_accounts:
            account_id = safe_text(account.get("id"))
            account_label = safe_text(account.get("label")) or account_id
            try:
                append_onex_agent_log(f"Серверная попытка выгрузки для {account_label}.", kind="info")
                imported_count = run_onex_export(account)
                exported.append((account, imported_count))
                append_onex_agent_log(f"{account_label}: импортировано {imported_count} строк.", kind="success")
            except Exception as exc:
                if needs_local_onex_captcha(exc):
                    local_required.append(account)
                    append_onex_agent_log(f"Для {account_label} нужна локальная captcha.", kind="info")
                else:
                    failed.append((account, safe_text(exc)))
                    append_onex_agent_log(f"Ошибка server-first для {account_label}: {safe_text(exc)}", kind="error")
        if local_required:
            if get_onex_agent_state().get("connected"):
                if len(local_required) == 1:
                    queue_onex_agent_job("start_full_flow", account_id=safe_text(local_required[0].get("id")), payload=get_current_onex_export_plan())
                else:
                    queue_onex_agent_job(
                        "start_multi_flow",
                        payload={"export_plan": get_current_onex_export_plan()},
                        account_ids=[safe_text(item.get("id")) for item in local_required],
                    )
                exported_labels = ", ".join([f"{safe_text(item.get('label'))} ({count})" for item, count in exported]) or "ничего"
                local_labels = ", ".join([safe_text(item.get("label")) for item in local_required])
                save_onex_agent_state(
                    status="waiting_for_user",
                    connected=True,
                    paused=False,
                    current_account="__all__" if len(local_required) > 1 else safe_text(local_required[0].get("id")),
                    current_account_label="All cabinets" if len(local_required) > 1 else safe_text(local_required[0].get("label")),
                    message=f"Сервер выгрузил: {exported_labels}. Для кабинетов {local_labels} нужна локальная captcha.",
                    job_status="pending",
                    job_message="Waiting for local captcha fallback.",
                    last_error=safe_text(failed[0][1]) if failed else "",
                )
            else:
                local_labels = ", ".join([safe_text(item.get("label")) for item in local_required])
                save_onex_agent_state(
                    status="waiting_for_user",
                    connected=False,
                    paused=False,
                    current_account="__all__" if len(local_required) > 1 else safe_text(local_required[0].get("id")),
                    current_account_label="All cabinets" if len(local_required) > 1 else safe_text(local_required[0].get("label")),
                    message=f"Для кабинетов {local_labels} нужна локальная captcha, но локальный агент сейчас не подключён.",
                    job_status="idle",
                    job_message="",
                    last_error=safe_text(failed[0][1]) if failed else "",
                )
            return
        total_rows = sum(count for _, count in exported)
        summary = f"Серверная выгрузка завершена. Кабинетов: {len(exported)}, строк: {total_rows}."
        if failed:
            summary += f" Ошибок: {len(failed)}."
        save_onex_agent_state(
            status="ready",
            connected=bool(get_onex_agent_state().get("connected")),
            paused=False,
            current_account="__all__" if len(target_accounts) > 1 else safe_text(target_accounts[0].get("id")),
            current_account_label="All cabinets" if len(target_accounts) > 1 else safe_text(target_accounts[0].get("label")),
            message=summary,
            job_status="idle",
            job_message="",
            last_error=safe_text(failed[0][1]) if failed else "",
        )
        append_onex_agent_log(summary, kind="done" if not failed else "info")
    finally:
        global ONEXBET_SESSION_THREAD
        with ONEXBET_SESSION_LOCK:
            ONEXBET_SESSION_THREAD = None


def start_onex_server_first_flow(account_ids):
    global ONEXBET_SESSION_THREAD
    with ONEXBET_SESSION_LOCK:
        if ONEXBET_SESSION_THREAD and ONEXBET_SESSION_THREAD.is_alive():
            raise RuntimeError("Сейчас уже выполняется операция 1x Parser.")
        worker = threading.Thread(
            target=run_onex_server_first_flow,
            args=(list(account_ids or []),),
            name="onex-server-first",
            daemon=True,
        )
        ONEXBET_SESSION_THREAD = worker
        worker.start()


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
    ensure_fb_table()
    db = SessionLocal()
    try:
        return db.query(FBRow).all()
    finally:
        db.close()



def get_filtered_data(
    buyer="",
    manager="",
    geo="",
    offer="",
    search="",
    period_label="",
    source_name="",
    ad_name="",
    adset_name="",
    creative="",
    platform="",
):
    ensure_fb_table()
    db = SessionLocal()
    try:
        query = db.query(FBRow)
        if buyer:
            query = query.filter(FBRow.uploader == buyer)
        if platform:
            query = query.filter(FBRow.platform == safe_text(platform))
        if source_name:
            query = query.filter(FBRow.source_name == safe_text(source_name))
        if manager:
            query = query.filter(FBRow.manager == manager)
        if geo:
            query = query.filter(FBRow.geo == geo)
        if offer:
            query = query.filter(FBRow.offer == offer)
        if ad_name:
            query = query.filter(FBRow.ad_name == ad_name)
        if adset_name:
            query = query.filter(FBRow.adset_name == adset_name)
        if creative:
            query = query.filter(FBRow.creative == creative)
        if search:
            search_pattern = f"%{safe_text(search).strip()}%"
            query = query.filter(or_(
                FBRow.ad_name.ilike(search_pattern),
                FBRow.adset_name.ilike(search_pattern),
                FBRow.campaign_name.ilike(search_pattern),
                FBRow.account_id.ilike(search_pattern),
                FBRow.platform.ilike(search_pattern),
                FBRow.manager.ilike(search_pattern),
                FBRow.geo.ilike(search_pattern),
                FBRow.offer.ilike(search_pattern),
                FBRow.creative.ilike(search_pattern),
                FBRow.uploader.ilike(search_pattern),
                FBRow.source_name.ilike(search_pattern),
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


def get_fb_buyer_name_options():
    options = []
    seen = set()
    for item in load_users():
        buyer_key = safe_text(getattr(item, "buyer_name", "")).strip()
        if not buyer_key or not getattr(item, "is_active", 1):
            continue
        key = buyer_key.lower()
        if key in seen:
            continue
        seen.add(key)
        options.append((buyer_key, buyer_key))
    options.sort(key=lambda item: safe_text(item[1]).lower())
    return options


def get_latest_fb_upload_summary(buyer="", period_label="", source_name=""):
    ensure_fb_table()
    clean_buyer = safe_text(buyer)
    clean_period_label = safe_text(period_label)
    clean_source_name = safe_text(source_name)

    db = SessionLocal()
    try:
        query = db.query(FBRow).order_by(FBRow.id.desc())
        if clean_buyer:
            query = query.filter(FBRow.uploader == clean_buyer)
        if clean_source_name:
            query = query.filter(FBRow.source_name == clean_source_name)
        for row in query.all():
            row_period_label = fb_row_period_label(row)
            if clean_period_label and row_period_label != clean_period_label:
                continue
            summary_source_name = safe_text(getattr(row, "source_name", "")) or build_fb_source_name(getattr(row, "uploader", ""), {
                "date_start": safe_text(getattr(row, "date_start", "")),
                "date_end": safe_text(getattr(row, "date_end", "")),
            })
            rows_count = db.query(func.count(FBRow.id)).filter(FBRow.source_name == summary_source_name).scalar() or 0
            spend_total = db.query(func.coalesce(func.sum(FBRow.spend), 0)).filter(FBRow.source_name == summary_source_name).scalar() or 0
            return {
                "source_name": summary_source_name,
                "period_label": row_period_label,
                "buyer": safe_text(getattr(row, "uploader", "")),
                "date_start": safe_text(getattr(row, "date_start", "")),
                "date_end": safe_text(getattr(row, "date_end", "")),
                "rows_count": safe_number(rows_count),
                "spend": safe_number(spend_total),
            }
    finally:
        db.close()
    return {}



def get_filter_options():
    ensure_fb_table()
    db = SessionLocal()
    try:
        buyers = sorted(value[0] for value in db.query(FBRow.uploader).distinct().all() if value[0])
        managers = sorted(value[0] for value in db.query(FBRow.manager).distinct().all() if value[0])
        geos = sorted(value[0] for value in db.query(FBRow.geo).distinct().all() if value[0])
        offers = sorted(value[0] for value in db.query(FBRow.offer).distinct().all() if value[0])
        return buyers, managers, geos, offers
    finally:
        db.close()


def get_fb_upload_summaries():
    ensure_fb_table()
    db = SessionLocal()
    try:
        rows = db.query(FBRow).order_by(FBRow.id.desc()).all()
    finally:
        db.close()

    grouped = {}
    for row in rows:
        source_key = safe_text(getattr(row, "source_name", ""))
        if not source_key:
            source_key = build_fb_source_name(getattr(row, "uploader", ""), {
                "date_start": safe_text(getattr(row, "date_start", "")),
                "date_end": safe_text(getattr(row, "date_end", "")),
            })
        item = grouped.setdefault(source_key, {
            "source_name": source_key,
            "buyer": safe_text(getattr(row, "uploader", "")),
            "period_label": fb_row_period_label(row),
            "date_start": safe_text(getattr(row, "date_start", "")),
            "date_end": safe_text(getattr(row, "date_end", "")),
            "rows_count": 0,
            "spend": 0.0,
        })
        item["rows_count"] += 1
        item["spend"] += safe_number(getattr(row, "spend", 0))

    result = list(grouped.values())
    result.sort(key=lambda item: (safe_text(item.get("period_label")), safe_text(item.get("buyer")), safe_text(item.get("source_name"))), reverse=True)
    return result


def get_fb_upload_options(period_label="", buyer=""):
    result = []
    for item in get_fb_upload_summaries():
        if period_label and safe_text(item.get("period_label")) != safe_text(period_label):
            continue
        if buyer and safe_text(item.get("buyer")) != safe_text(buyer):
            continue
        source_name = safe_text(item.get("source_name"))
        if source_name:
            result.append(source_name)
    return result


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


def ensure_chatterfy_parser_table():
    def sqlite_migration():
        with engine.begin() as conn:
            columns = [row[1] for row in conn.execute(text("PRAGMA table_info(chatterfy_parser_rows)")).fetchall()]
            if "chat_link" not in columns:
                conn.execute(text("ALTER TABLE chatterfy_parser_rows ADD COLUMN chat_link VARCHAR DEFAULT ''"))
            if "report_date" not in columns:
                conn.execute(text("ALTER TABLE chatterfy_parser_rows ADD COLUMN report_date VARCHAR DEFAULT ''"))
            if "period_start" not in columns:
                conn.execute(text("ALTER TABLE chatterfy_parser_rows ADD COLUMN period_start VARCHAR DEFAULT ''"))
            if "period_end" not in columns:
                conn.execute(text("ALTER TABLE chatterfy_parser_rows ADD COLUMN period_end VARCHAR DEFAULT ''"))
            if "period_label" not in columns:
                conn.execute(text("ALTER TABLE chatterfy_parser_rows ADD COLUMN period_label VARCHAR DEFAULT ''"))
    ensure_table_once("chatterfy_parser_rows", [ChatterfyParserRow.__table__], sqlite_migration)
    if not DATABASE_URL.startswith("sqlite"):
        inspector = inspect(engine)
        columns = {item.get("name") for item in inspector.get_columns("chatterfy_parser_rows")}
        migration_statements = []
        if "chat_link" not in columns:
            migration_statements.append(text("ALTER TABLE chatterfy_parser_rows ADD COLUMN IF NOT EXISTS chat_link VARCHAR DEFAULT ''"))
        if "report_date" not in columns:
            migration_statements.append(text("ALTER TABLE chatterfy_parser_rows ADD COLUMN IF NOT EXISTS report_date VARCHAR DEFAULT ''"))
        if "period_start" not in columns:
            migration_statements.append(text("ALTER TABLE chatterfy_parser_rows ADD COLUMN IF NOT EXISTS period_start VARCHAR DEFAULT ''"))
        if "period_end" not in columns:
            migration_statements.append(text("ALTER TABLE chatterfy_parser_rows ADD COLUMN IF NOT EXISTS period_end VARCHAR DEFAULT ''"))
        if "period_label" not in columns:
            migration_statements.append(text("ALTER TABLE chatterfy_parser_rows ADD COLUMN IF NOT EXISTS period_label VARCHAR DEFAULT ''"))
        if migration_statements:
            with engine.begin() as conn:
                for statement in migration_statements:
                    conn.execute(statement)


def ensure_chatterfy_id_table():
    ensure_table_once("chatterfy_id_rows", [ChatterfyIdRow.__table__])


def ensure_onex_parser_table():
    ensure_table_once("onex_parser_rows", [OnexParserRow.__table__])


def get_half_month_period(today: date | None = None):
    if today is None:
        today = datetime.now(LOCAL_TIMEZONE).date()

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


def normalize_partner_period_bounds(period_data=None):
    if not isinstance(period_data, dict):
        return (None, None)
    start_raw = safe_text(period_data.get("date_start"))
    end_raw = safe_text(period_data.get("date_end"))
    try:
        start_date = datetime.strptime(start_raw, "%Y-%m-%d").date() if start_raw else None
    except Exception:
        start_date = None
    try:
        end_date = datetime.strptime(end_raw, "%Y-%m-%d").date() if end_raw else None
    except Exception:
        end_date = None
    return (start_date, end_date)


def registration_date_in_period(value, period_data=None):
    start_date, end_date = normalize_partner_period_bounds(period_data)
    if not start_date or not end_date:
        return True
    parsed = parse_datetime_flexible(value)
    if not parsed:
        return False
    registration_date = parsed.date()
    return start_date <= registration_date <= end_date


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
    raw_period = upload_period_data if isinstance(upload_period_data, dict) else get_dataframe_detected_period(df)

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
        if not registration_date_in_period(registration_value, raw_period):
            continue
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
    deposits_col = resolve_dataframe_column(alias_map, ["Deposits", "Deposit Amount", "Net Deposits"])
    net_deposits_col = resolve_dataframe_column(alias_map, ["Net Deposits", "Net Deposit", "NGR"])
    activity_count_col = resolve_dataframe_column(alias_map, ["Activity Count", "Hold Time", "Hold time"])
    status_col = resolve_dataframe_column(alias_map, ["Status", "Blocked"])
    upload_period = build_partner_storage_period(upload_period_data)

    records = []
    for _, row in df.iterrows():
        player_id = safe_text(row.get(player_id_col)) if player_id_col else ""
        country = normalize_geo_value(row.get(country_col)) if country_col else ""
        if not player_id or not country:
            continue
        deposit_amount = convert_cellxpert_eur_to_usd(row.get(deposits_col)) if deposits_col else 0.0
        registration_value = row.get(registration_col) if registration_col else ""
        period_info = upload_period if safe_text(upload_period.get("period_label")) else {"report_date": "", "period_start": "", "period_end": "", "period_label": ""}
        records.append(PartnerRow(
            source_name=source_name,
            cabinet_name=safe_text(cabinet_name),
            sub_id="",
            player_id=player_id,
            report_date=period_info["report_date"],
            period_start=period_info["period_start"],
            period_end=period_info["period_end"],
            period_label=period_info["period_label"],
            registration_date=safe_text(registration_value),
            country=country,
            deposit_amount=deposit_amount,
            bet_amount=None,
            company_income=convert_cellxpert_eur_to_usd(row.get(net_deposits_col)) if net_deposits_col else None,
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


def get_partner_brand_options():
    result = []
    seen = set()
    for row in get_cabinet_rows():
        if safe_text(getattr(row, "status", "")).lower() == "archived":
            continue
        for brand in split_list_tokens(getattr(row, "brands", "")):
            key = brand.lower()
            if not brand or key in seen:
                continue
            seen.add(key)
            result.append(brand)
    return sorted(result, key=lambda item: safe_text(item).lower())


def get_partner_geo_options():
    result = []
    seen = set()
    for row in get_cabinet_rows():
        if safe_text(getattr(row, "status", "")).lower() == "archived":
            continue
        for geo_code in split_geo_tokens(getattr(row, "geo_list", "")):
            key = geo_code.upper()
            if not key or key in seen:
                continue
            seen.add(key)
            result.append((geo_code, geo_display_name(geo_code)))
    result.sort(key=lambda item: safe_text(item[1]).lower())
    return result


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
    normalized_columns = normalize_dataframe_columns(df)
    name_col = resolve_normalized_dataframe_column(normalized_columns, ["Name"])
    telegram_col = resolve_normalized_dataframe_column(normalized_columns, ["Telegram ID", "TelegramID", "Telegram Id"])
    username_col = resolve_normalized_dataframe_column(normalized_columns, ["Username", "User Name"])
    tags_col = resolve_normalized_dataframe_column(normalized_columns, ["Tags", "Tag"])
    started_col = resolve_normalized_dataframe_column(normalized_columns, ["Started", "Start", "Started At"])
    last_user_col = resolve_normalized_dataframe_column(normalized_columns, ["Last User Message", "Last user message"])
    last_bot_col = resolve_normalized_dataframe_column(normalized_columns, ["Last Bot Message", "Last bot message"])
    status_col = resolve_normalized_dataframe_column(normalized_columns, ["Status"])
    step_col = resolve_normalized_dataframe_column(normalized_columns, ["Step"])
    external_id_col = resolve_normalized_dataframe_column(normalized_columns, ["ID", "Id", "External ID", "External Id"])
    chat_link_col = resolve_normalized_dataframe_column(normalized_columns, ["Chat Link", "chat_link", "Link", "URL", "Url"])

    required_columns = [name_col, telegram_col, tags_col, started_col, status_col]
    if any(not item for item in required_columns):
        return 0

    records = []
    for _, row in df.iterrows():
        tags = safe_text(row.get(tags_col))
        parsed = parse_chatterfy_tags(tags)
        period_info = get_half_month_period_from_date(row.get(started_col))
        records.append(ChatterfyRow(
            source_name=source_name,
            name=safe_text(row.get(name_col)),
            telegram_id=safe_text(row.get(telegram_col)),
            username=safe_text(row.get(username_col)) if username_col else "",
            tags=tags,
            started=safe_text(row.get(started_col)),
            last_user_message=safe_text(row.get(last_user_col)) if last_user_col else "",
            last_bot_message=safe_text(row.get(last_bot_col)) if last_bot_col else "",
            status=safe_text(row.get(status_col)),
            step=safe_text(row.get(step_col)) if step_col else "",
            external_id=safe_text(row.get(external_id_col)) if external_id_col else "",
            chat_link=safe_text(row.get(chat_link_col)) if chat_link_col else "",
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


def import_chatterfy_parser_dataframe(df, source_name=""):
    ensure_chatterfy_parser_table()
    normalized_columns = normalize_dataframe_columns(df)
    name_col = resolve_normalized_dataframe_column(normalized_columns, ["Name"])
    telegram_col = resolve_normalized_dataframe_column(normalized_columns, ["Telegram ID", "TelegramID", "Telegram Id"])
    username_col = resolve_normalized_dataframe_column(normalized_columns, ["Username", "User Name"])
    tags_col = resolve_normalized_dataframe_column(normalized_columns, ["Tags", "Tag"])
    started_col = resolve_normalized_dataframe_column(normalized_columns, ["Started", "Start", "Started At"])
    last_user_col = resolve_normalized_dataframe_column(normalized_columns, ["Last User Message", "Last user message"])
    last_bot_col = resolve_normalized_dataframe_column(normalized_columns, ["Last Bot Message", "Last bot message"])
    status_col = resolve_normalized_dataframe_column(normalized_columns, ["Status"])
    step_col = resolve_normalized_dataframe_column(normalized_columns, ["Step"])
    external_id_col = resolve_normalized_dataframe_column(normalized_columns, ["ID", "Id", "External ID", "External Id"])
    chat_link_col = resolve_normalized_dataframe_column(normalized_columns, ["Chat Link", "chat_link", "chatlink", "Link", "URL", "Url"])

    required_columns = [name_col, telegram_col, tags_col, started_col, status_col]
    if any(not item for item in required_columns):
        return 0

    records = []
    for _, row in df.iterrows():
        tags = safe_text(row.get(tags_col))
        parsed = parse_chatterfy_tags(tags)
        period_info = get_half_month_period_from_date(row.get(started_col))
        records.append(ChatterfyParserRow(
            source_name=source_name,
            name=safe_text(row.get(name_col)),
            telegram_id=safe_text(row.get(telegram_col)),
            username=safe_text(row.get(username_col)) if username_col else "",
            tags=tags,
            started=safe_text(row.get(started_col)),
            last_user_message=safe_text(row.get(last_user_col)) if last_user_col else "",
            last_bot_message=safe_text(row.get(last_bot_col)) if last_bot_col else "",
            status=safe_text(row.get(status_col)),
            step=safe_text(row.get(step_col)) if step_col else "",
            external_id=safe_text(row.get(external_id_col)) if external_id_col else "",
            chat_link=safe_text(row.get(chat_link_col)) if chat_link_col else "",
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
        db.query(ChatterfyParserRow).delete()
        db.commit()
        sync_postgres_sequence("chatterfy_parser_rows")
        for item in records:
            db.add(item)
        db.commit()
    finally:
        db.close()
    return len(records)


def import_chatterfy_ids_dataframe(df):
    ensure_chatterfy_id_table()
    normalized_columns = normalize_dataframe_columns(df)
    telegram_col = resolve_normalized_dataframe_column(normalized_columns, ["TELEGRAM ID", "Telegram ID", "telegram_id", "TelegramID"])
    pp_player_id_col = resolve_normalized_dataframe_column(normalized_columns, ["1xbet_id", "pp_id", "ID игрока", "Player ID", "PP ID"])
    chat_link_col = resolve_normalized_dataframe_column(normalized_columns, ["chatlink", "chat_link", "link", "Chat Link"])
    source_date_col = resolve_normalized_dataframe_column(normalized_columns, ["date", "Date", "Created At"])

    if not telegram_col:
        return 0

    records = []
    for _, row in df.iterrows():
        telegram_id = safe_text(row.get(telegram_col))
        pp_player_id = safe_text(row.get(pp_player_id_col)) if pp_player_id_col else ""
        chat_link = safe_text(row.get(chat_link_col)) if chat_link_col else ""
        source_date = safe_text(row.get(source_date_col)) if source_date_col else ""
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


def import_chatterfy_upload_dataframe(df, source_name="", upload_kind="auto"):
    kind = safe_text(upload_kind).lower() or "auto"
    if kind == "auto":
        kind = detect_chatterfy_upload_kind(df)
    if kind == "main":
        return {"kind": "main", "count": import_chatterfy_dataframe(df, source_name)}
    if kind == "ids":
        return {"kind": "ids", "count": import_chatterfy_ids_dataframe(df)}
    return {"kind": "", "count": 0}


def extract_chatterfy_bot_id(value):
    parsed = urlparse(safe_text(value).strip())
    path = parsed.path or safe_text(value).strip()
    match = re.search(r"/bots/([0-9a-fA-F-]+)/users/?$", path)
    return safe_text(match.group(1)) if match else ""


def build_chatterfy_users_url(bot_id):
    clean_bot_id = safe_text(bot_id).strip()
    if not clean_bot_id:
        return ""
    return f"https://new.chatterfy.ai/bots/{clean_bot_id}/users"


def build_chatterfy_chat_link(bot_id="", chat_id="", fallback_url=""):
    direct_url = safe_text(fallback_url).strip()
    if direct_url.startswith("http"):
        return direct_url
    clean_bot_id = safe_text(bot_id).strip()
    clean_chat_id = safe_text(chat_id).strip()
    if not clean_bot_id or not clean_chat_id:
        return ""
    return f"https://new.chatterfy.ai/bots/{clean_bot_id}/users/{clean_chat_id}"


def chatterfy_parser_stop_requested():
    config = get_chatterfy_parser_config()
    state = safe_text(config.get("sync_state")) or "stopped"
    if state == "pause_pending":
        return True
    return False


def ensure_chatterfy_parser_not_stopped(message="Chatterfy sync stopped by user."):
    if chatterfy_parser_stop_requested():
        raise ChatterfySyncStopped(message)


def chatterfy_tracker_field_map(chat_item):
    result = {}
    for item in chat_item.get("tracker_fields") or []:
        key = safe_text(item.get("key"))
        if key and key not in result:
            result[key] = safe_text(item.get("value"))
    return result


def chatterfy_tag_names(chat_item):
    result = []
    for item in (chat_item.get("tags") or chat_item.get("bot_tags") or []):
        if isinstance(item, dict):
            value = safe_text(item.get("name"))
        else:
            value = safe_text(item)
        if value:
            result.append(value)
    return result


def chatterfy_chat_started_at(chat_item):
    return parse_datetime_flexible(chat_item.get("created_at"))


def chatterfy_chat_matches_period(chat_item, start_dt, end_dt):
    started_at = chatterfy_chat_started_at(chat_item)
    if not started_at:
        return False
    if started_at.tzinfo:
        started_at = started_at.astimezone(LOCAL_TIMEZONE)
    else:
        started_at = started_at.replace(tzinfo=LOCAL_TIMEZONE)
    if start_dt.tzinfo:
        start_dt = start_dt.astimezone(LOCAL_TIMEZONE)
    else:
        start_dt = start_dt.replace(tzinfo=LOCAL_TIMEZONE)
    if end_dt.tzinfo:
        end_dt = end_dt.astimezone(LOCAL_TIMEZONE)
    else:
        end_dt = end_dt.replace(tzinfo=LOCAL_TIMEZONE)
    return start_dt <= started_at <= end_dt


def chatterfy_chat_to_import_row(chat_item, step_map=None, bot_id=""):
    tracker_map = chatterfy_tracker_field_map(chat_item)
    chat_id = safe_text(chat_item.get("id"))
    row = {
        "Name": safe_text(chat_item.get("name")),
        "Telegram ID": safe_text(chat_item.get("external_id")),
        "Username": safe_text(chat_item.get("username")),
        "Tags": ", ".join(chatterfy_tag_names(chat_item)),
        "Started": safe_text(chat_item.get("created_at")),
        "Status": safe_text(chat_item.get("status")),
        "Step": safe_text(chat_item.get("current_step_name")) or safe_text(step_map.get(safe_text(chat_item.get("current_step_id")))),
        "ID": chat_id,
        "External ID": chat_id,
        "Chat Link": build_chatterfy_chat_link(
            bot_id=bot_id,
            chat_id=chat_id,
            fallback_url=chat_item.get("url") or chat_item.get("link") or chat_item.get("chat_link"),
        ),
    }
    for key, value in tracker_map.items():
        row[key] = value
    return row


def chatterfy_api_request(path, method="GET", token="", payload=None):
    url = path if safe_text(path).startswith("http") else f"{CHATTERFY_API_BASE_URL}{path}"
    body = None
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }
    clean_token = safe_text(token).strip()
    if clean_token:
        headers["Authorization"] = clean_token
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, headers=headers, method=method.upper())

    contexts = [ssl.create_default_context(), ssl._create_unverified_context()]
    last_error = None
    for context in contexts:
        try:
            with urllib.request.urlopen(request, timeout=60, context=context) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            last_error = exc
    raise last_error


def chatterfy_auth_signin(email, password):
    response = chatterfy_api_request(
        "/auth/signin",
        method="POST",
        payload={
            "provider": "email",
            "data": {
                "email": safe_text(email).strip(),
                "password": safe_text(password),
            },
        },
    )
    token = safe_text((response or {}).get("token"))
    if not token:
        raise RuntimeError("Chatterfy auth token was not returned by signin API.")
    return token


def chatterfy_login_and_get_token(email, password):
    clean_email = safe_text(email).strip()
    clean_password = safe_text(password)
    if not clean_email or not clean_password:
        raise ValueError("Email and password are required.")
    return chatterfy_auth_signin(clean_email, clean_password)


def chatterfy_fetch_step_map(bot_id, token):
    response = chatterfy_api_request(f"/bots/{bot_id}/steps", token=token)
    steps = response.get("steps") if isinstance(response, dict) else response
    result = {}
    for item in steps or []:
        step_id = safe_text(item.get("id"))
        if step_id:
            result[step_id] = safe_text(item.get("name"))
    return result


def chatterfy_fetch_period_dataframe(bot_id, token, date_start, date_end, progress_callback=None):
    ensure_chatterfy_parser_not_stopped()
    start_dt = parse_datetime_flexible(date_start)
    end_dt = parse_datetime_flexible(date_end)
    if not start_dt or not end_dt:
        raise ValueError("Choose a valid date range.")
    start_dt = datetime.combine(start_dt.date(), datetime.min.time())
    end_dt = datetime.combine(end_dt.date(), datetime.max.time())
    if end_dt < start_dt:
        raise ValueError("End date must be greater than or equal to start date.")

    step_map = chatterfy_fetch_step_map(bot_id, token)
    records = []
    limit = 200
    scroll_id = ""
    batch_number = 0
    while True:
        ensure_chatterfy_parser_not_stopped()
        batch_number += 1
        if progress_callback:
            progress_callback(f"Забираю пачку чатов #{batch_number} из раздела Users.")
        response = chatterfy_api_request(
            f"/bots/{bot_id}/chats/export",
            method="POST",
            token=token,
            payload={
                "filter": None,
                "offset": 0,
                "limit": limit,
                "scroll_id": scroll_id,
                "sort_direction": "desc",
            },
        )
        chats = response.get("chats") or []
        if not chats:
            break
        for index, item in enumerate(chats, 1):
            if index == 1 or index % 25 == 0:
                ensure_chatterfy_parser_not_stopped()
            if chatterfy_chat_matches_period(item, start_dt, end_dt):
                records.append(chatterfy_chat_to_import_row(item, step_map=step_map, bot_id=bot_id))
        if progress_callback:
            progress_callback(f"Пачка #{batch_number} получена. Подходящих записей накоплено: {len(records)}.")
        scroll_id = safe_text(response.get("scroll_id"))
        if len(chats) < limit:
            break
        if not scroll_id:
            break
    return pd.DataFrame(records), start_dt, end_dt


def get_chatterfy_parser_config():
    ensure_upload_dir()
    if not os.path.exists(CHATTERFY_PARSER_CONFIG_PATH):
        return {}
    try:
        with CHATTERFY_CONFIG_LOCK:
            with open(CHATTERFY_PARSER_CONFIG_PATH, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_chatterfy_parser_config(data):
    ensure_upload_dir()
    with CHATTERFY_CONFIG_LOCK:
        with open(CHATTERFY_PARSER_CONFIG_PATH, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)


def update_chatterfy_parser_config(**updates):
    config = get_chatterfy_parser_config()
    config.update(updates)
    save_chatterfy_parser_config(config)
    return config


def chatterfy_parser_append_log(message, kind="info", config=None):
    latest = dict(get_chatterfy_parser_config() or {})
    config = dict(config or latest)
    if latest:
        for key in (
            "auto_sync_enabled", "sync_state", "period_view", "period_label", "date_start", "date_end",
            "bot_url", "email", "password", "last_run_at", "last_success_at", "last_error",
            "last_count", "last_duration_seconds",
        ):
            if key in latest:
                config[key] = latest[key]
    latest_logs = list(latest.get("logs") or [])
    current_logs = list(config.get("logs") or [])
    logs = latest_logs if len(latest_logs) >= len(current_logs) else current_logs
    logs.append({
        "timestamp": get_crm_local_now().strftime("%Y-%m-%d %H:%M:%S"),
        "kind": safe_text(kind) or "info",
        "message": safe_text(message),
    })
    config["logs"] = logs[-20:]
    save_chatterfy_parser_config(config)
    return config


def chatterfy_parser_status_label(config=None):
    config = config or {}
    state = safe_text(config.get("sync_state")) or "stopped"
    if state in {"running", "idle"} and config.get("auto_sync_enabled"):
        return "Active"
    if state == "pause_pending":
        return "Stopping"
    return "Stopped"


def chatterfy_parser_status_color(config=None):
    config = config or {}
    state = safe_text(config.get("sync_state")) or "stopped"
    if state in {"running", "idle"} and config.get("auto_sync_enabled"):
        return "#16a34a"
    if state == "pause_pending":
        return "#dc2626"
    return "#64748b"


def chatterfy_parser_next_run_at(config=None, now=None):
    config = config or {}
    if not config.get("auto_sync_enabled"):
        return None
    current = now or get_crm_local_now()
    if current.tzinfo:
        current = current.astimezone(LOCAL_TIMEZONE)
    else:
        current = current.replace(tzinfo=LOCAL_TIMEZONE)
    next_slot = current.replace(minute=0, second=0, microsecond=0)
    if current >= next_slot:
        next_slot += timedelta(hours=1)
    return next_slot


def chatterfy_parser_next_run_text(config=None):
    config = config or {}
    if safe_text(config.get("sync_state")) == "running":
        return "Running now"
    if not config.get("auto_sync_enabled"):
        return "Paused"
    next_run = chatterfy_parser_next_run_at(config)
    if not next_run:
        return "Paused"
    return next_run.astimezone(LOCAL_TIMEZONE).strftime("%d.%m.%Y\n%H:%M")


def chatterfy_parser_next_run_seconds(config=None):
    config = config or {}
    if safe_text(config.get("sync_state")) in {"running", "pause_pending"}:
        return None
    if not config.get("auto_sync_enabled"):
        return None
    next_run = chatterfy_parser_next_run_at(config)
    if not next_run:
        return None
    seconds = int((next_run - get_crm_local_now()).total_seconds())
    return max(0, seconds)


def format_duration_clock(total_seconds):
    seconds = max(0, int(safe_number(total_seconds)))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def chatterfy_parser_log_badge(kind):
    clean_kind = safe_text(kind) or "info"
    if clean_kind == "success":
        return {
            "label": "OK",
            "style": "color:#6ee7b7;",
        }
    if clean_kind == "error":
        return {
            "label": "ERR",
            "style": "color:#f87171;",
        }
    return {
        "label": "RUN",
        "style": "color:#94a3b8;",
    }


def chatterfy_parser_console_message(message):
    clean = safe_text(message)
    replacements = [
        ("Запуск синхронизации. Начинаю обновление данных из Chatterfy.", "Starting sync"),
        ("Проверяю доступ и вхожу в Chatterfy по вашим данным.", "Signing in to Chatterfy"),
        ("Вход выполнен. Получаю структуру бота и подготавливаю выгрузку.", "Login ok, preparing export"),
        ("Загружаю обновлённые записи в раздел Chatterfy Parser внутри CRM.", "Saving rows to CRM"),
        ("Автосинк остановлен. Больше почасовых выгрузок не будет, пока вы снова не нажмёте Старт.", "Auto sync stopped"),
        ("Пришло время почасовой выгрузки. Запускаю очередное обновление.", "Hourly sync started"),
        ("Нажата Пауза. Делаю последнюю выгрузку и после этого остановлю автосинк.", "Stop requested, finishing current sync"),
    ]
    for old, new in replacements:
        if clean == old:
            return new
    if clean.startswith("Нажат Start."):
        return clean.replace("Нажат Start. Включаю штатный режим для периода ", "Start requested for ").replace(" и запускаю первую выгрузку.", "")
    if clean.startswith("Данные получены. Отфильтровал период "):
        return clean.replace("Данные получены. Отфильтровал период ", "Loaded period ").replace(" и подготовил ", " | rows: ").replace(" записей.", "")
    if clean.startswith("Готово. В разделе Chatterfy Parser обновлено "):
        return clean.replace("Готово. В разделе Chatterfy Parser обновлено ", "Completed | imported ").replace(" записей из Chatterfy.", " rows")
    if clean.startswith("Не получилось обновить данные: "):
        return clean.replace("Не получилось обновить данные: ", "Sync failed: ")
    return clean


def chatterfy_parser_log_entries(logs, limit=14):
    entries = []
    for item in list(logs or [])[-max(1, int(limit or 20)):][::-1]:
        badge = chatterfy_parser_log_badge(item.get("kind"))
        raw_timestamp = safe_text(item.get("timestamp"))
        timestamp = None
        if raw_timestamp.endswith("UTC"):
            utc_base = parse_datetime_flexible(raw_timestamp.replace("UTC", "").strip())
            if utc_base:
                timestamp = utc_base.replace(tzinfo=timezone.utc).astimezone(LOCAL_TIMEZONE)
        else:
            parsed_local = parse_datetime_flexible(raw_timestamp)
            if parsed_local:
                timestamp = parsed_local if parsed_local.tzinfo else parsed_local.replace(tzinfo=LOCAL_TIMEZONE)
        entries.append({
            "timestamp": timestamp.strftime("%d.%m.%Y %H:%M:%S Kyiv") if timestamp else raw_timestamp,
            "message": chatterfy_parser_console_message(item.get("message")),
            "label": badge["label"],
            "style": badge["style"],
        })
    return entries


def chatterfy_parser_runtime_payload(config=None):
    config = config or get_chatterfy_parser_config()
    last_success_dt = parse_datetime_flexible(config.get("last_success_at"))
    if last_success_dt and last_success_dt.tzinfo:
        last_success_dt = last_success_dt.astimezone(LOCAL_TIMEZONE)
    elif last_success_dt:
        last_success_dt = last_success_dt.replace(tzinfo=LOCAL_TIMEZONE)
    last_success_at = last_success_dt.strftime("%d.%m.%Y\n%H:%M") if last_success_dt else "Never"
    sync_state = safe_text(config.get("sync_state")) or "stopped"
    next_run_seconds = chatterfy_parser_next_run_seconds(config)
    is_active = sync_state in {"running", "idle"} and bool(config.get("auto_sync_enabled"))
    is_stopping = sync_state == "pause_pending"
    return {
        "status_label": chatterfy_parser_status_label(config),
        "status_color": chatterfy_parser_status_color(config),
        "next_run_text": chatterfy_parser_next_run_text(config),
        "last_success_at": last_success_at,
        "last_count": int(safe_number(config.get("last_count"))),
        "last_duration_text": format_duration_clock(config.get("last_duration_seconds")) if safe_number(config.get("last_duration_seconds")) > 0 else "Пока не было",
        "next_run_seconds": next_run_seconds,
        "last_error": safe_text(config.get("last_error")),
        "sync_state": sync_state,
        "auto_sync_enabled": bool(config.get("auto_sync_enabled")),
        "button_label": "Stop" if is_active or is_stopping else "Start",
        "button_style": "background:#d94b4b; border-color:#d94b4b; color:#fff;" if is_active or is_stopping else "background:#2563eb; border-color:#2563eb; color:#fff;",
        "logs": chatterfy_parser_log_entries(config.get("logs") or []),
    }


def build_chatterfy_parser_config_payload(
    bot_url="",
    email="",
    password="",
    period_view="",
    period_label="",
    date_start="",
    date_end="",
    auto_sync_enabled=False,
    existing_config=None,
):
    existing_config = existing_config or {}
    clean_password = safe_text(password)
    clean_period_view = safe_text(period_view) or safe_text(existing_config.get("period_view")) or "period"
    resolved_period_label = resolve_period_label(clean_period_view, period_label) or safe_text(period_label) or safe_text(existing_config.get("period_label")) or get_current_period_label()
    period_dates = period_label_to_dates(resolved_period_label)
    return {
        "bot_url": safe_text(bot_url) or safe_text(existing_config.get("bot_url")) or build_chatterfy_users_url(extract_chatterfy_bot_id(bot_url)),
        "email": safe_text(email) or safe_text(existing_config.get("email")) or CHATTERFY_PARSER_ACCOUNT_EMAIL,
        "password": clean_password if clean_password else safe_text(existing_config.get("password")) or CHATTERFY_PARSER_ACCOUNT_PASSWORD,
        "period_view": clean_period_view,
        "period_label": safe_text(period_dates.get("period_label")) or resolved_period_label,
        "date_start": safe_text(period_dates.get("date_start")) or safe_text(date_start) or safe_text(existing_config.get("date_start")) or CHATTERFY_PARSER_DEFAULT_START,
        "date_end": safe_text(period_dates.get("date_end")) or safe_text(date_end) or safe_text(existing_config.get("date_end")) or CHATTERFY_PARSER_DEFAULT_END,
        "auto_sync_enabled": bool(auto_sync_enabled),
        "sync_state": safe_text(existing_config.get("sync_state")) or "stopped",
        "logs": list(existing_config.get("logs") or []),
        "last_run_at": safe_text(existing_config.get("last_run_at")),
        "last_success_at": safe_text(existing_config.get("last_success_at")),
        "last_error": safe_text(existing_config.get("last_error")),
        "last_count": safe_number(existing_config.get("last_count")),
        "last_duration_seconds": safe_number(existing_config.get("last_duration_seconds")),
    }


def perform_chatterfy_parser_sync(config, initiated_by="manual"):
    if not CHATTERFY_SYNC_LOCK.acquire(blocking=False):
        raise RuntimeError("Chatterfy sync is already running.")
    try:
        sync_started_at = get_crm_local_now()
        config = dict(config or {})
        config["sync_state"] = "running"
        config = chatterfy_parser_append_log("Запуск синхронизации. Начинаю обновление данных из Chatterfy.", kind="info", config=config)
        ensure_chatterfy_parser_not_stopped()
        bot_url = safe_text(config.get("bot_url"))
        email = safe_text(config.get("email"))
        password = safe_text(config.get("password"))
        date_start = safe_text(config.get("date_start")) or CHATTERFY_PARSER_DEFAULT_START
        date_end = safe_text(config.get("date_end")) or CHATTERFY_PARSER_DEFAULT_END
        bot_id = extract_chatterfy_bot_id(bot_url)
        if not bot_id:
            raise ValueError("Invalid Chatterfy Users URL.")
        if not email or not password:
            raise ValueError("Chatterfy email and password are required for sync.")
        config = chatterfy_parser_append_log("Проверяю доступ и вхожу в Chatterfy по вашим данным.", kind="info", config=config)
        token = chatterfy_login_and_get_token(email, password)
        config = chatterfy_parser_append_log("Вход выполнен. Получаю структуру бота и подготавливаю выгрузку.", kind="success", config=config)
        def progress(message):
            nonlocal config
            ensure_chatterfy_parser_not_stopped()
            config = chatterfy_parser_append_log(message, kind="info", config=config)
        df, start_dt, end_dt = chatterfy_fetch_period_dataframe(bot_id, token, date_start, date_end, progress_callback=progress)
        if df.empty:
            raise RuntimeError(f"No chats found for {start_dt.strftime('%d.%m.%Y')} - {end_dt.strftime('%d.%m.%Y')}.")
        config = chatterfy_parser_append_log(
            f"Данные получены. Отфильтровал период {start_dt.strftime('%d.%m.%Y')} - {end_dt.strftime('%d.%m.%Y')} и подготовил {len(df)} записей.",
            kind="info",
            config=config,
        )
        source_name = f"Chatterfy API {start_dt.strftime('%d.%m.%Y')} - {end_dt.strftime('%d.%m.%Y')}"
        config = chatterfy_parser_append_log("Загружаю обновлённые записи в раздел Chatterfy Parser внутри CRM.", kind="info", config=config)
        result_count = import_chatterfy_parser_dataframe(df, source_name)
        result = {"count": int(result_count)}
        if result["count"] <= 0:
            raise RuntimeError("Chatterfy data was fetched, but import returned zero rows.")
        latest_config = dict(get_chatterfy_parser_config() or {})
        updated_config = dict(config)
        updated_config.update({k: v for k, v in latest_config.items() if k != "logs"})
        pause_after_sync = safe_text(latest_config.get("sync_state")) == "pause_pending"
        updated_config["last_run_at"] = get_crm_local_now().isoformat()
        updated_config["last_success_at"] = get_crm_local_now().isoformat()
        updated_config["last_error"] = ""
        updated_config["last_count"] = int(result["count"])
        updated_config["last_duration_seconds"] = int((get_crm_local_now() - sync_started_at).total_seconds())
        updated_config["sync_state"] = "stopped" if pause_after_sync or not updated_config.get("auto_sync_enabled") else "idle"
        if pause_after_sync:
            updated_config["auto_sync_enabled"] = False
            updated_config["sync_state"] = "stopped"
        save_chatterfy_parser_config(updated_config)
        chatterfy_parser_append_log(
            f"Готово. В разделе Chatterfy Parser обновлено {result['count']} записей из Chatterfy.",
            kind="success",
            config=updated_config,
        )
        return {
            "count": int(result["count"]),
            "start_dt": start_dt,
            "end_dt": end_dt,
            "source_name": source_name,
            "initiated_by": initiated_by,
        }
    except ChatterfySyncStopped as exc:
        latest_config = dict(get_chatterfy_parser_config() or {})
        updated_config = dict(config or {})
        updated_config.update({k: v for k, v in latest_config.items() if k != "logs"})
        updated_config["auto_sync_enabled"] = False
        updated_config["sync_state"] = "stopped"
        updated_config["last_run_at"] = get_crm_local_now().isoformat()
        updated_config["last_error"] = ""
        updated_config["last_duration_seconds"] = int((get_crm_local_now() - sync_started_at).total_seconds()) if 'sync_started_at' in locals() else 0
        save_chatterfy_parser_config(updated_config)
        chatterfy_parser_append_log("Автосинк остановлен. Больше почасовых выгрузок не будет, пока вы снова не нажмёте Старт.", kind="info", config=updated_config)
        return {
            "count": 0,
            "source_name": "",
            "initiated_by": initiated_by,
            "stopped": True,
            "message": safe_text(exc),
        }
    except Exception as exc:
        updated_config = dict(config or {})
        updated_config["last_run_at"] = get_crm_local_now().isoformat()
        updated_config["last_error"] = safe_text(exc)
        updated_config["last_duration_seconds"] = int((get_crm_local_now() - sync_started_at).total_seconds()) if 'sync_started_at' in locals() else 0
        updated_config["sync_state"] = "stopped" if not updated_config.get("auto_sync_enabled") else "idle"
        save_chatterfy_parser_config(updated_config)
        chatterfy_parser_append_log(f"Не получилось обновить данные: {exc}", kind="error", config=updated_config)
        raise
    finally:
        CHATTERFY_SYNC_LOCK.release()


def run_chatterfy_parser_sync_async(initiated_by="manual"):
    def worker():
        config = get_chatterfy_parser_config()
        try:
            perform_chatterfy_parser_sync(config, initiated_by=initiated_by)
        except Exception:
            pass
        finally:
            latest = get_chatterfy_parser_config()
            if safe_text(latest.get("sync_state")) == "pause_pending":
                latest["auto_sync_enabled"] = False
                latest["sync_state"] = "stopped"
                save_chatterfy_parser_config(latest)
                chatterfy_parser_append_log("Автосинк остановлен. Больше почасовых выгрузок не будет, пока вы снова не нажмёте Старт.", kind="info")
    threading.Thread(target=worker, name=f"chatterfy-sync-{initiated_by}", daemon=True).start()


def chatterfy_parser_sync_worker():
    while True:
        try:
            config = get_chatterfy_parser_config()
            if config.get("auto_sync_enabled") and safe_text(config.get("sync_state")) != "running":
                now = get_crm_local_now()
                current_slot = now.replace(minute=0, second=0, microsecond=0)
                last_run = parse_datetime_flexible(config.get("last_run_at"))
                if last_run:
                    last_run = last_run.astimezone(LOCAL_TIMEZONE) if last_run.tzinfo else last_run.replace(tzinfo=LOCAL_TIMEZONE)
                if now.minute == 0 and (not last_run or last_run < current_slot):
                    chatterfy_parser_append_log("Пришло время почасовой выгрузки. Запускаю очередное обновление.", kind="info", config=config)
                    run_chatterfy_parser_sync_async(initiated_by="auto")
        except Exception:
            pass
        time.sleep(60)


def start_chatterfy_parser_sync_thread():
    global CHATTERFY_SYNC_THREAD_STARTED
    if CHATTERFY_SYNC_THREAD_STARTED:
        return
    worker = threading.Thread(target=chatterfy_parser_sync_worker, name="chatterfy-parser-sync", daemon=True)
    worker.start()
    CHATTERFY_SYNC_THREAD_STARTED = True


def recover_chatterfy_parser_after_startup():
    config = dict(get_chatterfy_parser_config() or {})
    if not config:
        return
    sync_state = safe_text(config.get("sync_state")) or "stopped"
    if sync_state == "pause_pending":
        config["auto_sync_enabled"] = False
        config["sync_state"] = "stopped"
        save_chatterfy_parser_config(config)
        return
    if sync_state == "running":
        config["sync_state"] = "idle" if config.get("auto_sync_enabled") else "stopped"
        save_chatterfy_parser_config(config)
    if not config.get("auto_sync_enabled"):
        return
    now = get_crm_local_now()
    current_slot = now.replace(minute=0, second=0, microsecond=0)
    last_run = parse_datetime_flexible(config.get("last_run_at"))
    if last_run:
        last_run = last_run.astimezone(LOCAL_TIMEZONE) if last_run.tzinfo else last_run.replace(tzinfo=LOCAL_TIMEZONE)
    if not last_run or last_run < current_slot:
        chatterfy_parser_append_log("Приложение перезапущено. Возобновляю Chatterfy Parser и догоняю ближайший цикл.", kind="info", config=config)
        if not CHATTERFY_SYNC_LOCK.locked():
            run_chatterfy_parser_sync_async(initiated_by="recovery")


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
    saved_config = get_chatterfy_parser_config()
    parser_bot_id = extract_chatterfy_bot_id(safe_text(saved_config.get("bot_url")))
    search_lower = safe_text(search).lower()
    for row in rows:
        linked = id_map.get(safe_text(row.telegram_id))
        started_dt = parse_chatterfy_datetime(row.started)
        started_date = started_dt.strftime("%d.%m.%Y") if started_dt else ""
        started_time = started_dt.strftime("%H:%M") if started_dt else ""
        linked_pp = safe_text(linked.pp_player_id) if linked else ""
        linked_chat = safe_text(linked.chat_link) if linked else ""
        chat_link = linked_chat or build_chatterfy_chat_link(
            bot_id=parser_bot_id,
            chat_id=safe_text(getattr(row, "external_id", "")),
        )
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
                chat_link,
                started_date,
                started_time,
            ]).lower()
            if search_lower not in haystack:
                continue
        row.chat_link = chat_link
        filtered.append({
            "row": row,
            "started_date": started_date,
            "started_time": started_time,
            "pp_player_id": linked_pp,
            "chat_link": chat_link,
            "report_date": row_report_date,
            "period_label": row_period_label,
        })
    return filtered


def get_chatterfy_parser_rows(status="", search="", date_filter="", time_filter="", telegram_id="", period_label=""):
    ensure_chatterfy_parser_table()
    telegram_digits = normalize_id_value(telegram_id)
    db = SessionLocal()
    try:
        query = db.query(ChatterfyParserRow)
        if status:
            query = query.filter(ChatterfyParserRow.status == status)
        if period_label:
            query = query.filter(ChatterfyParserRow.period_label == period_label)
        if search:
            search_pattern = f"%{safe_text(search)}%"
            query = query.filter(or_(
                ChatterfyParserRow.name.ilike(search_pattern),
                ChatterfyParserRow.username.ilike(search_pattern),
                ChatterfyParserRow.telegram_id.ilike(search_pattern),
                ChatterfyParserRow.tags.ilike(search_pattern),
                ChatterfyParserRow.status.ilike(search_pattern),
                ChatterfyParserRow.offer.ilike(search_pattern),
                ChatterfyParserRow.manager.ilike(search_pattern),
                ChatterfyParserRow.geo.ilike(search_pattern),
                ChatterfyParserRow.step.ilike(search_pattern),
            ))
        rows = query.order_by(ChatterfyParserRow.id.desc()).all()
    finally:
        db.close()
    filtered = []
    saved_config = get_chatterfy_parser_config()
    parser_bot_id = extract_chatterfy_bot_id(safe_text(saved_config.get("bot_url")))
    search_lower = safe_text(search).lower()
    for row in rows:
        started_dt = parse_chatterfy_datetime(row.started)
        started_date = started_dt.strftime("%d.%m.%Y") if started_dt else ""
        started_time = started_dt.strftime("%H:%M") if started_dt else ""
        row_report_date = safe_text(getattr(row, "report_date", "")) or (started_dt.strftime("%Y-%m-%d") if started_dt else "")
        if date_filter and date_filter != started_date:
            continue
        if time_filter and not started_time.startswith(time_filter):
            continue
        if telegram_digits and normalize_id_value(row.telegram_id) != telegram_digits:
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
                row.step or "",
                row.external_id or "",
                row.chat_link or "",
                started_date,
                started_time,
            ]).lower()
            if search_lower not in haystack:
                continue
        chat_link = safe_text(getattr(row, "chat_link", "")) or build_chatterfy_chat_link(
            bot_id=parser_bot_id,
            chat_id=safe_text(getattr(row, "external_id", "")),
        )
        filtered.append({
            "row": row,
            "started_date": started_date,
            "started_time": started_time,
            "report_date": row_report_date,
            "period_label": safe_text(getattr(row, "period_label", "")),
            "chat_link": chat_link,
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

def get_partner_period_options(period_label=""):
    ensure_partner_table()
    db = SessionLocal()
    try:
        query = db.query(PartnerRow.source_name)
        if period_label:
            query = query.filter(PartnerRow.period_label == safe_text(period_label))
        values = query.distinct().all()
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


def get_partner_rows_by_period(period_value="", period_label="", cabinet_name="", brand="", geo="", search=""):
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

    clean_brand = safe_text(brand)
    clean_geo = normalize_geo_value(geo)
    if clean_brand or clean_geo:
        filtered_rows = []
        for row in rows:
            cabinet_item = cabinet_meta_map.get(safe_text(getattr(row, "cabinet_name", "")))
            brand_tokens = split_list_tokens(getattr(cabinet_item, "brands", "") if cabinet_item else "")
            if clean_brand and clean_brand.lower() not in {item.lower() for item in brand_tokens}:
                continue
            if clean_geo and normalize_geo_value(getattr(row, "country", "")) != clean_geo:
                continue
            filtered_rows.append(row)
        rows = filtered_rows

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
        row.partner_geo_name = format_geo_list_names(getattr(cabinet_item, "geo_list", "")) if cabinet_item else ""
        row.geo_name = geo_display_name(getattr(row, "country", "") or "")
        row_period_label = partner_row_period_label(row)
        scope_key = build_cap_scope_key(getattr(row, "cabinet_name", ""), getattr(row, "country", ""))
        matched_caps = caps_by_scope_period.get((row_period_label, scope_key[0], scope_key[1]), [])
        matched_cap = matched_caps[0] if matched_caps else None
        row.is_qualified_ftd = bool(matched_cap and is_partner_row_qualified_for_cap(row, matched_cap, cabinet_platform_map))
        row.cpa_amount = safe_cap_number(getattr(matched_cap, "rate", 0)) if row.is_qualified_ftd else 0.0
    return rows


def aggregate_partner_totals(rows):
    deposits_total = sum(safe_number(r.deposit_amount) for r in rows)
    bets_total = sum(safe_number(r.bet_amount) for r in rows)
    income_total = sum(safe_number(r.company_income) for r in rows)
    cpa_total = sum(safe_number(r.cpa_amount) for r in rows)
    ftd_count = sum(1 for r in rows if safe_number(r.deposit_amount) > 0)
    return {
        "players": len(rows),
        "deposits": deposits_total,
        "bets": bets_total,
        "income": income_total,
        "cpa": cpa_total,
        "ftd_count": ftd_count,
        "qualified_ftd_count": sum(1 for r in rows if bool(getattr(r, "is_qualified_ftd", False))),
        "avg_deposit": (deposits_total / ftd_count) if ftd_count > 0 else 0.0,
        "sumdep2spend": ((deposits_total / cpa_total) * 100) if cpa_total > 0 else 0.0,
        "bet_to_deposit_ratio": (bets_total / deposits_total) if deposits_total > 0 else 0.0,
        "romi_ratio": ((income_total - cpa_total) / cpa_total) if cpa_total > 0 else 0.0,
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
    flow_bucket_weights = {}
    flow_bucket_counts = {}
    for item in rows:
        ad_key = build_ad_offer_key(item.get("launch_date"), item.get("platform"), item.get("manager"), item.get("geo"), item.get("offer"))
        flow_key = build_flow_key(item.get("platform"), item.get("manager"), item.get("geo"))
        weight = safe_number(item.get("ftd")) or safe_number(item.get("leads")) or safe_number(item.get("spend")) or 1.0
        ad_bucket_weights[ad_key] = ad_bucket_weights.get(ad_key, 0.0) + weight
        ad_bucket_counts[ad_key] = ad_bucket_counts.get(ad_key, 0) + 1
        flow_bucket_weights[flow_key] = flow_bucket_weights.get(flow_key, 0.0) + weight
        flow_bucket_counts[flow_key] = flow_bucket_counts.get(flow_key, 0) + 1
    enriched = []
    for item in rows:
        flow_key = build_flow_key(item.get("platform"), item.get("manager"), item.get("geo"))
        ad_key = build_ad_offer_key(item.get("launch_date"), item.get("platform"), item.get("manager"), item.get("geo"), item.get("offer"))
        flow_stat = partner_by_flow.get(flow_key, {})
        chatter_count = chatterfy_by_ad.get(ad_key, 0.0)
        flow_chatter_count = chatterfy_by_flow.get(flow_key, 0.0)
        row_weight = safe_number(item.get("ftd")) or safe_number(item.get("leads")) or safe_number(item.get("spend")) or 1.0
        bucket_weight = ad_bucket_weights.get(ad_key, 0.0)
        bucket_count = ad_bucket_counts.get(ad_key, 1)
        creative_share = (row_weight / bucket_weight) if bucket_weight > 0 else (1.0 / bucket_count)
        flow_weight = flow_bucket_weights.get(flow_key, 0.0)
        flow_count = flow_bucket_counts.get(flow_key, 1)
        flow_share = (row_weight / flow_weight) if flow_weight > 0 else (1.0 / flow_count)
        chatter_share = (chatter_count / flow_chatter_count) if flow_chatter_count > 0 else 0.0
        clone = dict(item)
        clone["stat_chatterfy"] = chatter_count * creative_share
        clone["stat_total_ftd"] = flow_stat.get("stat_total_ftd", 0.0) * flow_share
        clone["stat_qual_ftd"] = flow_stat.get("stat_qual_ftd", 0.0) * flow_share
        clone["stat_rate"] = flow_stat.get("stat_rate", 0.0)
        clone["stat_income"] = flow_stat.get("stat_income", 0.0) * flow_share
        clone["stat_cap_limit"] = flow_stat.get("stat_cap_limit", 0.0) * flow_share
        clone["stat_cap_fill"] = cap_fill_percent(clone["stat_total_ftd"], clone["stat_cap_limit"])
        clone["stat_has_cap"] = flow_stat.get("stat_has_cap", 0.0) if flow_share > 0 or flow_stat.get("stat_has_cap", 0.0) else 0.0
        clone["stat_chat_share"] = chatter_share
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
        key = "|||".join([
            row.source_name or "",
            row.uploader or "",
            row.account_id or "",
            row.campaign_name or "",
            row.adset_name or "",
            row.ad_name or "",
        ])
        if key not in grouped:
            grouped[key] = {
                "buyer": row.uploader or "",
                "source_name": row.source_name or "",
                "period_label": fb_row_period_label(row),
                "ad_name": row.ad_name or "",
                "adset_name": row.adset_name or "",
                "campaign_name": row.campaign_name or "",
                "budget": row.budget if row.budget is not None else None,
                "account_id": row.account_id or "",
                "launch_date": row.launch_date or "",
                "platform": row.platform or "",
                "manager": row.manager or "",
                "geo": row.geo or "",
                "offer": row.offer or "",
                "creative": row.creative or "",
                "material_views": 0.0,
                "clicks": 0.0,
                "leads": 0.0,
                "reg": 0.0,
                "paid_subscriptions": 0.0,
                "contacts": 0.0,
                "ftd": 0.0,
                "spend": 0.0,
                "frequency_total": 0.0,
                "ctr_total": 0.0,
                "rows_combined": 0,
                "date_start": row.date_start or "",
                "date_end": row.date_end or "",
            }

        grouped[key]["material_views"] += row.material_views or 0
        grouped[key]["clicks"] += row.clicks or 0
        grouped[key]["leads"] += row.leads or 0
        grouped[key]["reg"] += row.reg or 0
        grouped[key]["paid_subscriptions"] += row.paid_subscriptions or 0
        grouped[key]["contacts"] += row.contacts or 0
        grouped[key]["ftd"] += row.ftd or 0
        grouped[key]["spend"] += row.spend or 0
        if row.budget is not None:
            current_budget = grouped[key]["budget"]
            grouped[key]["budget"] = max(current_budget, row.budget) if current_budget is not None else row.budget
        grouped[key]["frequency_total"] += row.frequency or 0
        grouped[key]["ctr_total"] += row.ctr or 0
        grouped[key]["rows_combined"] += 1

    result = list(grouped.values())
    for item in result:
        row_count = item["rows_combined"] or 1
        item["frequency"] = item["frequency_total"] / row_count if row_count else 0
        item["ctr"] = item["ctr_total"] / row_count if row_count else 0
        item["cost_per_content_view"] = item["spend"] / item["material_views"] if item["material_views"] > 0 else 0
        item["cost_per_paid_subscription"] = item["spend"] / item["paid_subscriptions"] if item["paid_subscriptions"] > 0 else 0
        item["cost_per_contact"] = item["spend"] / item["contacts"] if item["contacts"] > 0 else 0
        item["cost_per_completed_registration"] = item["spend"] / item["reg"] if item["reg"] > 0 else 0
        item.update(calc_metrics(item["clicks"], item["reg"], item["ftd"], item["spend"], item["leads"]))
    return result



def aggregate_totals(rows):
    totals = {
        "clicks": sum(r["clicks"] for r in rows),
        "leads": sum(r["leads"] for r in rows),
        "reg": sum(r["reg"] for r in rows),
        "ftd": sum(r["ftd"] for r in rows),
        "spend": sum(r["spend"] for r in rows),
        "active_budget": sum(safe_number(r.get("budget", 0)) for r in rows),
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
    totals["cost_reg"] = totals["spend"] / totals["reg"] if totals["reg"] > 0 else 0
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


def sort_link(label, field, current_sort, current_order, **params):
    next_order = "asc" if current_sort != field or current_order == "desc" else "desc"
    qs = build_query_string(sort_by=field, order=next_order, **params)
    arrow = ""
    if current_sort == field:
        arrow = " ↑" if current_order == "asc" else " ↓"
    return f'<a href="?{qs}">{escape(label)}{arrow}</a>'


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


def load_users():
    db = SessionLocal()
    try:
        return db.query(User).order_by(User.username.asc()).all()
    finally:
        db.close()

@app.get("/fb", response_class=HTMLResponse)
def fb_page(request: Request):
    return _page_routes["fb_page"](request)

@app.get("/1x-parser", response_class=HTMLResponse)
def onex_parser_page(
    request: Request,
    account_id: str = Query(default=""),
    period_label: str = Query(default=""),
    search: str = Query(default=""),
    message: str = Query(default=""),
):
    return _page_routes["onex_parser_page"](request, account_id, period_label, search, message)


@app.get("/1x-parser/status-live")
def onex_parser_status_live(request: Request):
    return _domain_actions["onex_parser_status_live"](request)


@app.post("/1x-parser/start")
def onex_parser_start(
    request: Request,
    account_id: str = Form(...),
):
    return _domain_actions["onex_parser_start"](request, account_id)


@app.post("/1x-parser/launch")
def onex_parser_launch(
    request: Request,
    account_id: str = Form(...),
):
    return _domain_actions["onex_parser_launch"](request, account_id)


@app.post("/1x-parser/pause")
def onex_parser_pause(
    request: Request,
    account_id: str = Form(...),
):
    return _domain_actions["onex_parser_pause"](request, account_id)


@app.post("/1x-parser/clear")
def onex_parser_clear(
    request: Request,
    account_id: str = Form(...),
):
    return _domain_actions["onex_parser_clear"](request, account_id)


@app.post("/1x-parser/run")
def onex_parser_run(
    request: Request,
    account_id: str = Form(...),
):
    return _domain_actions["onex_parser_run"](request, account_id)


@app.post("/1x-parser/confirm")
def onex_parser_confirm(
    request: Request,
    account_id: str = Form(...),
):
    return _domain_actions["onex_parser_confirm"](request, account_id)


@app.get("/1xbet-parser", response_class=HTMLResponse)
def onex_parser_alias(request: Request):
    return _domain_actions["onex_parser_alias"](request)


@app.get("/api/onex-agent/job")
def api_onex_agent_job(
    api_key: str = Query(default=""),
    api_key_header: str = Header(default="", alias="api-key"),
    api_key_alt_header: str = Header(default="", alias="api_key"),
):
    return _domain_actions["api_onex_agent_job"](api_key, api_key_header, api_key_alt_header)


@app.post("/api/onex-agent/heartbeat")
async def api_onex_agent_heartbeat(
    request: Request,
    api_key: str = Query(default=""),
    api_key_header: str = Header(default="", alias="api-key"),
    api_key_alt_header: str = Header(default="", alias="api_key"),
):
    return await _domain_actions["api_onex_agent_heartbeat"](request, api_key, api_key_header, api_key_alt_header)


@app.post("/api/onex-agent/schedule-start")
async def api_onex_agent_schedule_start(
    request: Request,
    api_key: str = Query(default=""),
    api_key_header: str = Header(default="", alias="api-key"),
    api_key_alt_header: str = Header(default="", alias="api_key"),
):
    return await _domain_actions["api_onex_agent_schedule_start"](request, api_key, api_key_header, api_key_alt_header)


@app.post("/api/onex-agent/job/complete")
async def api_onex_agent_job_complete(
    request: Request,
    api_key: str = Query(default=""),
    api_key_header: str = Header(default="", alias="api-key"),
    api_key_alt_header: str = Header(default="", alias="api_key"),
):
    return await _domain_actions["api_onex_agent_job_complete"](request, api_key, api_key_header, api_key_alt_header)


@app.post("/api/onex-agent/upload-session")
async def api_onex_agent_upload_session(
    request: Request,
    api_key: str = Query(default=""),
    api_key_header: str = Header(default="", alias="api-key"),
    api_key_alt_header: str = Header(default="", alias="api_key"),
):
    return await _domain_actions["api_onex_agent_upload_session"](request, api_key, api_key_header, api_key_alt_header)


@app.post("/api/onex-parser/import-rows")
async def api_onex_parser_import_rows(
    request: Request,
    api_key: str = Query(default=""),
    api_key_header: str = Header(default="", alias="api-key"),
    api_key_alt_header: str = Header(default="", alias="api_key"),
):
    return await _domain_actions["api_onex_parser_import_rows"](request, api_key, api_key_header, api_key_alt_header)


@app.get("/chatterfy-parser", response_class=HTMLResponse)
def chatterfy_parser_page(
    request: Request,
    status: str = Query(default=""),
    search: str = Query(default=""),
    date_filter: str = Query(default=""),
    time_filter: str = Query(default=""),
    telegram_id: str = Query(default=""),
    page: int = Query(default=1),
    bot_url: str = Query(default=""),
    period_view: str = Query(default="period"),
    period_label: str = Query(default=""),
    message: str = Query(default=""),
):
    return _page_routes["chatterfy_parser_page"](
        request,
        status,
        search,
        date_filter,
        time_filter,
        telegram_id,
        page,
        bot_url,
        period_view,
        period_label,
        message,
    )


@app.get("/chatterfy-parser/status-live")
def chatterfy_parser_status_live(request: Request):
    return _domain_actions["chatterfy_parser_status_live"](request)


@app.post("/chatterfy-parser/toggle")
def toggle_chatterfy_parser(
    request: Request,
    bot_url: str = Form(...),
    period_view: str = Form(default="period"),
    period_label: str = Form(default=""),
):
    return _domain_actions["toggle_chatterfy_parser"](request, bot_url, period_view, period_label)


# Rebind extracted view/layout functions from dedicated modules while keeping route contracts intact.
globals().update(bind_page_views(globals()))
_original_sidebar_html = sidebar_html


def _patched_sidebar_html(active_page, current_user=None):
    html = _original_sidebar_html(active_page, current_user)
    if not html:
        return html
    html = re.sub(
        r'<a href="/fb" class="sidebar-standalone[^"]*"><span class="side-emoji">.*?</span><span class="side-label">FB</span></a>',
        '',
        html,
        count=1,
        flags=re.S,
    )
    fb_export_icon = """
    <svg viewBox="0 0 64 64" aria-hidden="true" focusable="false">
        <defs>
            <linearGradient id="fbExportGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stop-color="#35c2ff"/>
                <stop offset="100%" stop-color="#1d4ed8"/>
            </linearGradient>
        </defs>
        <rect x="6" y="6" width="52" height="52" rx="12" fill="url(#fbExportGrad)"/>
        <path d="M38 18h6v8h-5c-1.5 0-2 0.7-2 2.1V33h7l-1.2 8H37v15h-8V41h-6v-8h6v-6c0-5.7 3.4-9 9-9Z" fill="#ffffff"/>
    </svg>
    """
    html = re.sub(
        r'(<a href="/grouped" class="[^"]*"><span class="side-emoji side-sub-emoji">)📈(</span><span class="side-label">Export FB</span></a>)',
        r"\1" + fb_export_icon + r"\2",
        html,
        count=1,
    )
    parsers_icon = """
    <svg viewBox="0 0 64 64" aria-hidden="true" focusable="false">
        <defs>
            <linearGradient id="parsersGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stop-color="#7c9cff"/>
                <stop offset="100%" stop-color="#5865f2"/>
            </linearGradient>
        </defs>
        <rect x="6" y="6" width="52" height="52" rx="14" fill="url(#parsersGrad)"/>
        <path d="M20 24c3-2.7 7.1-4 12-4s9 1.3 12 4c2.2 2 3.6 4.6 4 7.6-.7 5.7-3.7 10.2-8.7 13.2l-1.3 4.7-4.7-2.6c-1.4.3-2.9.5-4.3.5-4.9 0-9-1.3-12-4-2.3-2-3.6-4.6-4-7.7.4-3 1.7-5.5 4-7.7Z" fill="#ffffff"/>
        <circle cx="27.5" cy="32.5" r="2.7" fill="#5865f2"/>
        <circle cx="36.5" cy="32.5" r="2.7" fill="#5865f2"/>
    </svg>
    """
    parser_links_pattern = (
        r'(<a href="/chatterfy-parser" class="[^"]*"><span class="side-emoji[^"]*">.*?</span><span class="side-label">Chatterfy Parser</span></a>)'
        r'(<a href="/1x-parser" class="[^"]*"><span class="side-emoji[^"]*">.*?</span><span class="side-label">1x Parser</span></a>)'
    )
    parsers_open = " open" if active_page in {"chatterfyparser", "onexparser"} else ""
    parser_match = re.search(parser_links_pattern, html, flags=re.S)
    if parser_match:
        chatterfy_link = parser_match.group(1)
        onex_link = parser_match.group(2)
        chatterfy_link = re.sub(r'class="[^"]*"', lambda m: 'class="active-link"' if 'active-link' in m.group(0) else '', chatterfy_link, count=1)
        onex_link = re.sub(r'class="[^"]*"', lambda m: 'class="active-link"' if 'active-link' in m.group(0) else '', onex_link, count=1)
        chatterfy_link = chatterfy_link.replace('<span class="side-emoji side-sub-emoji">', '<span class="side-emoji side-sub-emoji">', 1)
        onex_link = onex_link.replace('<span class="side-emoji side-sub-emoji">', '<span class="side-emoji side-sub-emoji">', 1)
        chatterfy_link = chatterfy_link.replace('<span class="side-emoji">', '<span class="side-emoji side-sub-emoji">', 1)
        onex_link = onex_link.replace('<span class="side-emoji">', '<span class="side-emoji side-sub-emoji">', 1)
        chatterfy_link = chatterfy_link.replace('<a  ', '<a ').replace('class=""', '')
        onex_link = onex_link.replace('<a  ', '<a ').replace('class=""', '')
        parsers_group = (
            f'<details class="sidebar-group" data-sidebar-group="parsers"{parsers_open}>'
            f'<summary><span class="side-emoji">{parsers_icon}</span><span class="side-label">Parsers</span></summary>'
            f'<div class="sidebar-links">{chatterfy_link}{onex_link}</div></details>'
        )
        html = re.sub(parser_links_pattern, lambda _m: parsers_group, html, count=1, flags=re.S)
    return html

sidebar_html = _patched_sidebar_html
page_shell.__globals__["sidebar_html"] = _patched_sidebar_html
_page_routes = bind_page_routes(globals())
_domain_actions = {}
for _binder in (bind_analytics_actions, bind_parser_actions, bind_management_actions, bind_report_actions):
    _domain_actions.update(_binder(globals()))


_original_chatterfy_parser_page = _page_routes.get("chatterfy_parser_page")
_original_toggle_chatterfy_parser = _domain_actions.get("toggle_chatterfy_parser")
_original_upload_file = _domain_actions.get("upload_file")
_original_grouped_page = _page_routes.get("show_grouped_table")
_original_dashboard_page = _page_routes.get("render_dashboard_page")
_original_show_dashboard = _page_routes.get("show_dashboard")
_original_show_hierarchy = _page_routes.get("show_hierarchy")
_original_render_stats_cards = render_stats_cards


def _patched_render_stats_cards(totals):
    totals = totals or {}
    cards = [
        ("Spend", format_money(totals.get("spend", 0))),
        ("Leads", format_int_or_float(totals.get("leads", 0))),
        ("Reg", format_int_or_float(totals.get("reg", 0))),
        ("FTD", format_int_or_float(totals.get("ftd", 0))),
        ("Cost Reg", format_money(totals.get("cost_reg", 0))),
        ("CPA", format_money(totals.get("cpa_real", 0))),
        ("Budget", format_money(totals.get("active_budget", 0))),
    ]
    cards_html = "".join(
        f'<div class="stat-card"><div class="name">{escape(name)}</div><div class="value">{escape(value)}</div></div>'
        for name, value in cards
    )
    return f'<div class="panel compact-panel"><div class="stats-grid">{cards_html}</div></div>'


render_stats_cards = _patched_render_stats_cards


def normalize_dashboard_platform(value):
    raw = safe_text(value).strip().lower()
    if raw.startswith("1x"):
        return "1x"
    if raw in {"cellxpert", "cell xpert"}:
        return "cellxpert"
    return raw


def dashboard_offer_matches_cabinet(cabinet_row, offer_value):
    clean_offer = safe_text(offer_value).strip().lower()
    if not clean_offer:
        return True
    brand_tokens = [safe_text(item).strip().lower() for item in split_list_tokens(getattr(cabinet_row, "brands", ""))]
    if not brand_tokens:
        return True
    return clean_offer in brand_tokens


def build_dashboard_cabinet_flow_map():
    result = {}
    for row in get_cabinet_rows():
        platform_key = normalize_dashboard_platform(getattr(row, "platform", ""))
        manager_key = safe_text(getattr(row, "manager_name", ""))
        geos = split_geo_tokens(getattr(row, "geo_list", "")) or [normalize_geo_value(getattr(row, "geo_list", ""))]
        for geo_code in geos:
            if not platform_key or not manager_key or not geo_code:
                continue
            flow_key = build_flow_key(platform_key, manager_key, geo_code)
            result.setdefault(flow_key, []).append(row)
    return result


def pick_dashboard_primary_cabinet(cabinets, fb_item=None):
    if not cabinets:
        return None

    preferred = list(cabinets)
    fb_item = fb_item or {}
    offer_value = safe_text(fb_item.get("offer"))
    haystack = " | ".join([
        safe_text(fb_item.get("campaign_name")),
        safe_text(fb_item.get("adset_name")),
        safe_text(fb_item.get("ad_name")),
    ]).lower()

    offer_matched = [cab for cab in preferred if dashboard_offer_matches_cabinet(cab, offer_value)]
    if offer_matched:
        preferred = offer_matched

    active_only = [cab for cab in preferred if safe_text(getattr(cab, "status", "")).lower() == "active"]
    if active_only:
        preferred = active_only

    name_matched = [cab for cab in preferred if safe_text(getattr(cab, "name", "")).strip().lower() in haystack]
    if name_matched:
        preferred = name_matched

    advertiser_matched = [cab for cab in preferred if safe_text(getattr(cab, "advertiser", "")).strip().lower() in haystack]
    if advertiser_matched:
        preferred = advertiser_matched

    preferred.sort(key=lambda cab: (
        safe_text(getattr(cab, "status", "")).lower() != "active",
        safe_text(getattr(cab, "name", "")).lower(),
        safe_text(getattr(cab, "advertiser", "")).lower(),
    ))
    return preferred[0]


def build_dashboard_caps_flow_map(period_label=""):
    ensure_caps_table()
    db = SessionLocal()
    try:
        query = db.query(CapRow)
        if period_label:
            query = query.filter(CapRow.period_label == period_label)
        caps = query.all()
    finally:
        db.close()

    result = {}
    for cap in caps:
        flow_parts = [part.strip() for part in safe_text(cap.flow).split("/") if part.strip()]
        platform_key = normalize_dashboard_platform(flow_parts[0] if len(flow_parts) > 0 else "")
        manager_key = flow_parts[1] if len(flow_parts) > 1 else safe_text(cap.owner_name)
        geo_key = normalize_geo_value(flow_parts[2] if len(flow_parts) > 2 else (cap.code or cap.geo))
        if not platform_key or not manager_key or not geo_key:
            continue
        flow_key = build_flow_key(platform_key, manager_key, geo_key)
        bucket = result.setdefault(flow_key, {
            "caps_count": 0,
            "cap_total": 0.0,
            "cap_current_ftd": 0.0,
            "cap_promos": set(),
            "cabinet_names": set(),
        })
        bucket["caps_count"] += 1
        bucket["cap_total"] += safe_number(cap.cap_value)
        bucket["cap_current_ftd"] += safe_number(cap.current_ftd)
        if safe_text(cap.promo_code):
            bucket["cap_promos"].add(safe_text(cap.promo_code))
        if safe_text(cap.cabinet_name):
            bucket["cabinet_names"].add(safe_text(cap.cabinet_name))
    for bucket in result.values():
        bucket["cap_fill"] = cap_fill_percent(bucket["cap_current_ftd"], bucket["cap_total"])
    return result


def build_dashboard_hold_flow_map(period_label=""):
    result = {}
    for item in get_hold_wager_rows(period_label=period_label):
        flow_parts = [part.strip() for part in safe_text(item.get("flow")).split("/") if part.strip()]
        platform_key = normalize_dashboard_platform(flow_parts[0] if len(flow_parts) > 0 else "")
        manager_key = flow_parts[1] if len(flow_parts) > 1 else ""
        geo_key = normalize_geo_value(flow_parts[2] if len(flow_parts) > 2 else item.get("country"))
        if not platform_key or not manager_key or not geo_key:
            continue
        flow_key = build_flow_key(platform_key, manager_key, geo_key)
        bucket = result.setdefault(flow_key, {
            "hold_count": 0,
            "baseline_fail_count": 0,
            "wager_fail_count": 0,
            "hold_cabinets": set(),
        })
        bucket["hold_count"] += 1
        reason = safe_text(item.get("reason")).lower()
        if "baseline" in reason:
            bucket["baseline_fail_count"] += 1
        if "wager" in reason:
            bucket["wager_fail_count"] += 1
        if safe_text(item.get("cabinet_name")):
            bucket["hold_cabinets"].add(safe_text(item.get("cabinet_name")))
    return result


def build_dashboard_rows_v2(user, buyer="", period_label=""):
    buyer_scope = resolve_effective_buyer(user, buyer)
    fb_rows = aggregate_grouped_rows(get_filtered_data(buyer=buyer_scope, period_label=period_label))
    fb_rows = enrich_statistic_rows(fb_rows, period_label=period_label)
    cabinet_map = build_dashboard_cabinet_flow_map()
    caps_map = build_dashboard_caps_flow_map(period_label=period_label)
    hold_map = build_dashboard_hold_flow_map(period_label=period_label)

    rows = []

    for item in fb_rows:
        platform_key = normalize_dashboard_platform(item.get("platform"))
        flow_key = build_flow_key(platform_key, item.get("manager"), item.get("geo"))
        related_cabinets = cabinet_map.get(flow_key, [])
        matched_cabinets = [cab for cab in related_cabinets if dashboard_offer_matches_cabinet(cab, item.get("offer"))]
        if matched_cabinets:
            related_cabinets = matched_cabinets

        primary_cabinet = pick_dashboard_primary_cabinet(related_cabinets, item)
        primary_cabinet_name = safe_text(getattr(primary_cabinet, "name", ""))
        primary_advertiser = safe_text(getattr(primary_cabinet, "advertiser", ""))
        cabinet_names = [primary_cabinet_name] if primary_cabinet_name else []
        advertiser_names = [primary_advertiser] if primary_advertiser else []
        active_cabinets = [cab for cab in related_cabinets if safe_text(getattr(cab, "status", "")).lower() == "active"]
        cap_info = caps_map.get(flow_key, {})
        hold_info = hold_map.get(flow_key, {})

        row = {
            "buyer": safe_text(item.get("buyer")),
            "platform": safe_text(item.get("platform")),
            "manager": safe_text(item.get("manager")),
            "geo": safe_text(item.get("geo")),
            "offer": safe_text(item.get("offer")),
            "cabinet_names": cabinet_names,
            "cabinet_text": primary_cabinet_name or "—",
            "advertiser_names": advertiser_names,
            "advertiser_text": primary_advertiser or "—",
            "campaign_name": safe_text(item.get("campaign_name")),
            "adset_name": safe_text(item.get("adset_name")),
            "ad_name": safe_text(item.get("ad_name")),
            "account_id": safe_text(item.get("account_id")),
            "launch_date": safe_text(item.get("launch_date")),
            "budget": safe_number(item.get("budget", 0)),
            "spend": safe_number(item.get("spend", 0)),
            "clicks": safe_number(item.get("clicks", 0)),
            "leads": safe_number(item.get("leads", 0)),
            "reg": safe_number(item.get("reg", 0)),
            "cost_reg": safe_number(item.get("cost_per_completed_registration", 0)),
            "fb_ftd": safe_number(item.get("ftd", 0)),
            "cpa": safe_number(item.get("cpa_real", 0)),
            "chatterfy": safe_number(item.get("stat_chatterfy", 0)),
            "players_ftd": safe_number(item.get("stat_total_ftd", 0)),
            "qual_ftd": safe_number(item.get("stat_qual_ftd", 0)),
            "rate": safe_number(item.get("stat_rate", 0)),
            "income": safe_number(item.get("stat_income", 0)),
            "profit": safe_number(item.get("stat_profit", 0)),
            "roi": safe_number(item.get("stat_roi", 0)),
            "caps_count": safe_number(cap_info.get("caps_count", 0)),
            "cap_total": safe_number(cap_info.get("cap_total", 0)),
            "cap_current_ftd": safe_number(cap_info.get("cap_current_ftd", 0)),
            "cap_fill": safe_number(cap_info.get("cap_fill", 0)),
            "hold_count": safe_number(hold_info.get("hold_count", 0)),
            "hold_split": f'{format_int_or_float(hold_info.get("baseline_fail_count", 0))}B / {format_int_or_float(hold_info.get("wager_fail_count", 0))}W' if hold_info else "0B / 0W",
            "active_cabinets": len(active_cabinets),
            "flow_key": flow_key,
            "flow_label": " / ".join(filter(None, [safe_text(item.get("platform")), safe_text(item.get("manager")), safe_text(item.get("geo"))])),
            "source_name": safe_text(item.get("source_name")),
            "row_kind": "fb",
        }
        rows.append(row)

    return rows


def build_dashboard_summary_cards(rows):
    totals = {
        "rows": len(rows),
        "spend": sum(safe_number(row.get("spend", 0)) for row in rows),
        "leads": sum(safe_number(row.get("leads", 0)) for row in rows),
        "reg": sum(safe_number(row.get("reg", 0)) for row in rows),
        "fb_ftd": sum(safe_number(row.get("fb_ftd", 0)) for row in rows),
        "players_ftd": sum(safe_number(row.get("players_ftd", 0)) for row in rows),
        "chatterfy": sum(safe_number(row.get("chatterfy", 0)) for row in rows),
        "income": sum(safe_number(row.get("income", 0)) for row in rows),
        "profit": sum(safe_number(row.get("profit", 0)) for row in rows),
    }
    cards = [
        ("Rows", format_int_or_float(totals["rows"])),
        ("Spend", format_money(totals["spend"])),
        ("Leads", format_int_or_float(totals["leads"])),
        ("Reg", format_int_or_float(totals["reg"])),
        ("FB FTD", format_int_or_float(totals["fb_ftd"])),
        ("Players FTD", format_int_or_float(totals["players_ftd"])),
        ("Chatterfy", format_int_or_float(totals["chatterfy"])),
        ("Income", format_money(totals["income"])),
        ("Profit", format_money(totals["profit"])),
    ]
    cards_html = "".join(
        f'<div class="stat-card"><div class="name">{escape(name)}</div><div class="value">{escape(value)}</div></div>'
        for name, value in cards
    )
    return f'<div class="panel compact-panel"><div class="stats-grid">{cards_html}</div></div>'


def _dashboard_filter_rows(
    rows,
    platform="",
    manager="",
    geo="",
    offer="",
    cabinet_name="",
    advertiser="",
    campaign_name="",
    adset_name="",
    account_id="",
    ad_name="",
    source_name="",
    has_caps="",
    has_hold="",
    has_chatterfy="",
    has_players="",
    search="",
):
    clean_platform = normalize_dashboard_platform(platform)
    clean_manager = safe_text(manager).strip().lower()
    clean_geo = normalize_geo_value(geo)
    clean_offer = safe_text(offer).strip().lower()
    clean_cabinet = safe_text(cabinet_name).strip().lower()
    clean_advertiser = safe_text(advertiser).strip().lower()
    clean_campaign = safe_text(campaign_name).strip().lower()
    clean_adset = safe_text(adset_name).strip().lower()
    clean_account = safe_text(account_id).strip().lower()
    clean_ad_name = safe_text(ad_name).strip().lower()
    clean_source = safe_text(source_name).strip().lower()
    clean_search = safe_text(search).strip().lower()
    filtered = []
    for row in rows:
        if clean_platform and normalize_dashboard_platform(row.get("platform")) != clean_platform:
            continue
        if clean_manager and safe_text(row.get("manager")).strip().lower() != clean_manager:
            continue
        if clean_geo and normalize_geo_value(row.get("geo")) != clean_geo:
            continue
        if clean_offer and safe_text(row.get("offer")).strip().lower() != clean_offer:
            continue
        if clean_cabinet and clean_cabinet not in [safe_text(item).strip().lower() for item in row.get("cabinet_names", [])]:
            continue
        if clean_advertiser and clean_advertiser not in [safe_text(item).strip().lower() for item in row.get("advertiser_names", [])]:
            continue
        if clean_campaign and safe_text(row.get("campaign_name")).strip().lower() != clean_campaign:
            continue
        if clean_adset and safe_text(row.get("adset_name")).strip().lower() != clean_adset:
            continue
        if clean_account and safe_text(row.get("account_id")).strip().lower() != clean_account:
            continue
        if clean_ad_name and safe_text(row.get("ad_name")).strip().lower() != clean_ad_name:
            continue
        if clean_source and safe_text(row.get("source_name")).strip().lower() != clean_source:
            continue
        if has_caps == "yes" and safe_number(row.get("cap_total", 0)) <= 0:
            continue
        if has_caps == "no" and safe_number(row.get("cap_total", 0)) > 0:
            continue
        if has_hold == "yes" and safe_number(row.get("hold_count", 0)) <= 0:
            continue
        if has_hold == "no" and safe_number(row.get("hold_count", 0)) > 0:
            continue
        if has_chatterfy == "yes" and safe_number(row.get("chatterfy", 0)) <= 0:
            continue
        if has_chatterfy == "no" and safe_number(row.get("chatterfy", 0)) > 0:
            continue
        if has_players == "yes" and safe_number(row.get("players_ftd", 0)) <= 0:
            continue
        if has_players == "no" and safe_number(row.get("players_ftd", 0)) > 0:
            continue
        if clean_search:
            haystack = " | ".join([
                safe_text(row.get("buyer")),
                safe_text(row.get("platform")),
                safe_text(row.get("manager")),
                safe_text(row.get("geo")),
                safe_text(row.get("offer")),
                safe_text(row.get("cabinet_text")),
                safe_text(row.get("advertiser_text")),
                safe_text(row.get("campaign_name")),
                safe_text(row.get("adset_name")),
                safe_text(row.get("ad_name")),
                safe_text(row.get("account_id")),
                safe_text(row.get("flow_label")),
                safe_text(row.get("source_name")),
            ]).lower()
            if clean_search not in haystack:
                continue
        filtered.append(row)
    return filtered


def _dashboard_sort_rows(rows, sort_by="spend", order="desc"):
    numeric_fields = {
        "budget", "spend", "leads", "reg", "cost_reg", "fb_ftd", "cpa", "chatterfy",
        "players_ftd", "qual_ftd", "income", "profit", "roi", "caps_count",
        "cap_total", "cap_current_ftd", "cap_fill", "hold_count", "active_cabinets",
    }
    reverse = safe_text(order).lower() != "asc"
    if sort_by in numeric_fields:
        return sorted(rows, key=lambda item: safe_number(item.get(sort_by, 0)), reverse=reverse)
    return sorted(rows, key=lambda item: safe_text(item.get(sort_by, "")).lower(), reverse=reverse)


def _dashboard_sort_link(label, field, **params):
    current_sort = safe_text(params.get("sort_by") or "spend")
    current_order = safe_text(params.get("order") or "desc")
    next_order = "asc" if current_sort != field or current_order == "desc" else "desc"
    arrow = ""
    if current_sort == field:
        arrow = " ↑" if current_order == "asc" else " ↓"
    qs = build_query_string(**{**params, "sort_by": field, "order": next_order})
    return f'<a href="/dashboard?{qs}" class="dashboard-sort-link">{escape(label)}{arrow}</a>'


def _render_dashboard_page_v2(
    request: Request,
    buyer: str = "",
    manager: str = "",
    geo: str = "",
    offer: str = "",
    search: str = "",
    period_view: str = "current",
    period_label: str = "",
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "hierarchy")

    buyer = resolve_effective_buyer(user, safe_text(buyer))
    effective_period_label = resolve_period_label("period", safe_text(period_label)) or get_current_period_label()
    platform = safe_text(request.query_params.get("platform"))
    cabinet_name = safe_text(request.query_params.get("cabinet_name"))
    advertiser = safe_text(request.query_params.get("advertiser"))
    campaign_name = safe_text(request.query_params.get("campaign_name"))
    adset_name = safe_text(request.query_params.get("adset_name"))
    account_id = safe_text(request.query_params.get("account_id"))
    ad_name = safe_text(request.query_params.get("ad_name"))
    source_name = safe_text(request.query_params.get("source_name"))
    has_caps = safe_text(request.query_params.get("has_caps"))
    has_hold = safe_text(request.query_params.get("has_hold"))
    has_chatterfy = safe_text(request.query_params.get("has_chatterfy"))
    has_players = safe_text(request.query_params.get("has_players"))
    sort_by = safe_text(request.query_params.get("sort_by") or "spend")
    order = safe_text(request.query_params.get("order") or "desc")
    dashboard_state_param = safe_text(request.query_params.get("dashboard_state"))

    base_rows = build_dashboard_rows_v2(user, buyer=buyer, period_label=effective_period_label)
    buyer_values = [value for value, _label in get_fb_buyer_name_options()] or sorted({safe_text(row.get("buyer")) for row in base_rows if safe_text(row.get("buyer"))})
    platform_values = sorted({safe_text(row.get("platform")) for row in base_rows if safe_text(row.get("platform"))})
    manager_values = sorted({safe_text(row.get("manager")) for row in base_rows if safe_text(row.get("manager"))})
    geo_values = sorted({safe_text(row.get("geo")) for row in base_rows if safe_text(row.get("geo"))})
    offer_values = sorted({safe_text(row.get("offer")) for row in base_rows if safe_text(row.get("offer"))})
    cabinet_values = sorted({safe_text(item) for row in base_rows for item in row.get("cabinet_names", []) if safe_text(item)})
    advertiser_values = sorted({safe_text(item) for row in base_rows for item in row.get("advertiser_names", []) if safe_text(item)})
    campaign_values = sorted({safe_text(row.get("campaign_name")) for row in base_rows if safe_text(row.get("campaign_name"))})
    adset_values = sorted({safe_text(row.get("adset_name")) for row in base_rows if safe_text(row.get("adset_name"))})
    account_values = sorted({safe_text(row.get("account_id")) for row in base_rows if safe_text(row.get("account_id"))})
    ad_name_values = sorted({safe_text(row.get("ad_name")) for row in base_rows if safe_text(row.get("ad_name"))})
    source_values = sorted({safe_text(row.get("source_name")) for row in base_rows if safe_text(row.get("source_name"))})

    rows = _dashboard_filter_rows(
        base_rows,
        platform=platform,
        manager=manager,
        geo=geo,
        offer=offer,
        cabinet_name=cabinet_name,
        advertiser=advertiser,
        campaign_name=campaign_name,
        adset_name=adset_name,
        account_id=account_id,
        ad_name=ad_name,
        source_name=source_name,
        has_caps=has_caps,
        has_hold=has_hold,
        has_chatterfy=has_chatterfy,
        has_players=has_players,
        search=search,
    )
    rows = _dashboard_sort_rows(rows, sort_by=sort_by, order=order)

    buyer_options = "".join(
        f'<option value="{escape(value)}" {"selected" if safe_text(buyer) == safe_text(value) else ""}>{escape(value)}</option>'
        for value in buyer_values
    )
    period_options = "".join(
        f'<option value="{escape(value)}" {"selected" if safe_text(effective_period_label) == safe_text(value) else ""}>{escape(value)}</option>'
        for value in build_period_options()
    )
    platform_options = make_options(platform_values, platform)
    manager_options = make_options(manager_values, manager)
    geo_options = make_options(geo_values, geo)
    offer_options = make_options(offer_values, offer)
    cabinet_options = make_options(cabinet_values, cabinet_name)
    advertiser_options = make_options(advertiser_values, advertiser)
    campaign_options = make_options(campaign_values, campaign_name)
    adset_options = make_options(adset_values, adset_name)
    account_options = make_options(account_values, account_id)
    ad_name_options = make_options(ad_name_values, ad_name)
    source_options = make_options(source_values, source_name)
    yes_no_options = lambda selected: (
        '<option value="">All</option>'
        f'<option value="yes" {"selected" if selected == "yes" else ""}>Yes</option>'
        f'<option value="no" {"selected" if selected == "no" else ""}>No</option>'
    )

    filter_params = {
        "buyer": buyer,
        "platform": platform,
        "manager": manager,
        "geo": geo,
        "offer": offer,
        "cabinet_name": cabinet_name,
        "advertiser": advertiser,
        "campaign_name": campaign_name,
        "adset_name": adset_name,
        "account_id": account_id,
        "ad_name": ad_name,
        "source_name": source_name,
        "has_caps": has_caps,
        "has_hold": has_hold,
        "has_chatterfy": has_chatterfy,
        "has_players": has_players,
        "search": search,
        "period_view": "period",
        "period_label": effective_period_label,
        "sort_by": sort_by,
        "order": order,
        "dashboard_state": dashboard_state_param,
    }

    dashboard_numeric_fields = [
        "budget", "spend", "clicks", "leads", "reg", "rate", "cost_reg", "fb_ftd", "cpa",
        "chatterfy", "players_ftd", "qual_ftd", "hold_count", "cap_total", "cap_fill",
        "income", "profit", "roi",
    ]

    hierarchy_levels = [
        ("platform", "platform"),
        ("geo", "geo"),
        ("manager", "manager"),
        ("campaign_name", "campaign_name"),
        ("adset_name", "adset_name"),
    ]

    def aggregate_dashboard_metrics(items):
        totals = {field: 0.0 for field in dashboard_numeric_fields}
        for item in items:
            for field in dashboard_numeric_fields:
                totals[field] += safe_number(item.get(field, 0))
        return totals

    def hierarchy_bucket_sort_key(bucket_name, bucket_rows):
        text_value = safe_text(bucket_name).strip().lower()
        metric_field = sort_by if sort_by in dashboard_numeric_fields else "spend"
        metric_value = sum(safe_number(item.get(metric_field, 0)) for item in bucket_rows)
        reverse_metric = -metric_value if safe_text(order).lower() != "asc" else metric_value
        return (reverse_metric, text_value)

    node_counter = 0

    def build_dashboard_tree(items, levels, path=None):
        nonlocal node_counter
        path = path or []
        if not levels:
            return []
        field, _column = levels[0]
        buckets = {}
        for item in items:
            bucket_name = safe_text(item.get(field)).strip() or "—"
            buckets.setdefault(bucket_name, []).append(item)

        result = []
        sorted_bucket_items = sorted(
            buckets.items(),
            key=lambda item: hierarchy_bucket_sort_key(item[0], item[1]),
        )
        for bucket_name, bucket_rows in sorted_bucket_items:
            node_counter += 1
            node_path = [*path, f"{field}:{bucket_name}"]
            node_id = "dashboard-node-" + "|".join(node_path)
            result.append({
                "id": node_id,
                "field": field,
                "column": field,
                "label": bucket_name,
                "rows": bucket_rows,
                "metrics": aggregate_dashboard_metrics(bucket_rows),
                "children": build_dashboard_tree(bucket_rows, levels[1:], node_path),
            })
        return result

    def render_dashboard_metric_cells(values):
        return "".join([
            f'<td class="dashboard-tree-summary-cell" data-col="budget">{format_money(values.get("budget", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="spend">{format_money(values.get("spend", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="clicks">{format_int_or_float(values.get("clicks", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="leads">{format_int_or_float(values.get("leads", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="reg">{format_int_or_float(values.get("reg", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="rate">{format_money(values.get("rate", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="cost_reg">{format_money(values.get("cost_reg", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="fb_ftd">{format_int_or_float(values.get("fb_ftd", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="cpa">{format_money(values.get("cpa", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="chatterfy">{format_int_or_float(values.get("chatterfy", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="players_ftd">{format_int_or_float(values.get("players_ftd", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="qual_ftd">{format_int_or_float(values.get("qual_ftd", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="hold_count">{format_int_or_float(values.get("hold_count", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="hold_split">—</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="cap_total">{format_int_or_float(values.get("cap_total", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="cap_fill">{format_percent(values.get("cap_fill", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="income">{format_money(values.get("income", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="profit">{format_money(values.get("profit", 0))}</td>',
            f'<td class="dashboard-tree-summary-cell" data-col="roi">{format_percent(values.get("roi", 0))}</td>',
        ])

    def render_hierarchy_label(node, level, variant="caret"):
        icon_html = (
            '<span class="dashboard-tree-caret">▸</span>'
            if variant == "caret"
            else '<span class="dashboard-tree-plus"></span>'
        )
        toggle = (
            f'<button type="button" class="dashboard-tree-toggle dashboard-tree-toggle-{escape(variant)}" '
            f'data-target="{escape(node["id"])}" aria-expanded="false" '
            f'onclick="window.dashboardTreeToggle && window.dashboardTreeToggle(this); return false;">{icon_html}'
            f'<span class="dashboard-tree-label">{escape(node["label"])}</span></button>'
        )
        return f'<div class="dashboard-tree-cell dashboard-tree-level-{level}">{toggle}</div>'

    tree = build_dashboard_tree(rows, hierarchy_levels)

    table_headers = [
        ("platform", "Brand"),
        ("geo", "Geo"),
        ("manager", "Cabinet"),
        ("campaign_name", "Campaign"),
        ("adset_name", "Adset"),
        ("ad_name", "Ad"),
        ("buyer", "Buyer"),
        ("offer", "Offer"),
        ("cabinet_text", "Cabinets"),
        ("advertiser_text", "Advertiser"),
        ("account_id", "Account"),
        ("budget", "Budget"),
        ("spend", "Spend"),
        ("clicks", "Clicks"),
        ("leads", "Leads"),
        ("reg", "Reg"),
        ("rate", "Rate"),
        ("cost_reg", "Cost Reg"),
        ("fb_ftd", "FB FTD"),
        ("cpa", "CPA"),
        ("chatterfy", "Chatterfy"),
        ("players_ftd", "Players FTD"),
        ("qual_ftd", "Qual FTD"),
        ("hold_count", "Hold"),
        ("hold_split", "Hold Split"),
        ("cap_total", "Cap"),
        ("cap_fill", "Cap Fill"),
        ("income", "Income"),
        ("profit", "Profit"),
        ("roi", "ROI"),
    ]

    head_html = "".join(
        f'<th data-col="{escape(field)}">{_dashboard_sort_link(label, field, **filter_params)}</th>'
        for field, label in table_headers
    )

    column_chips = "".join(
        f'<label class="column-chip"><input type="checkbox" class="dashboard-column-toggle" value="{escape(field)}" checked> {escape(label)}</label>'
        for field, label in table_headers
    )

    def render_leaf_row(row, parent_id="", ancestors=None):
        ancestors = ancestors or []
        row_class = "soft-green" if safe_number(row.get("profit", 0)) > 0 else ("soft-red" if safe_number(row.get("profit", 0)) < 0 else "")
        hidden_attr = ' hidden' if parent_id else ''
        row_key = "leaf|" + "|".join([
            safe_text(parent_id) or "root",
            safe_text(row.get("platform")).strip() or "—",
            safe_text(row.get("geo")).strip() or "—",
            safe_text(row.get("manager")).strip() or "—",
            safe_text(row.get("campaign_name")).strip() or "—",
            safe_text(row.get("adset_name")).strip() or "—",
            safe_text(row.get("ad_name")).strip() or "—",
            safe_text(row.get("account_id")).strip() or "—",
        ])
        return f"""
        <tr class="dashboard-leaf-row {row_class}" data-parent-id="{escape(parent_id)}" data-ancestors="{escape(','.join(ancestors))}" data-row-key="{escape(row_key)}" onclick="window.dashboardHandleRowClick && window.dashboardHandleRowClick(this, event)"{hidden_attr}>
            <td data-col="platform"></td>
            <td data-col="geo"></td>
            <td data-col="manager"></td>
            <td data-col="campaign_name"></td>
            <td data-col="adset_name"></td>
            <td data-col="ad_name">{escape(row.get("ad_name") or "—")}</td>
            <td data-col="buyer">{escape(row.get("buyer") or "—")}</td>
            <td data-col="offer">{escape(row.get("offer") or "—")}</td>
            <td data-col="cabinet_text">{escape(row.get("cabinet_text") or "—")}</td>
            <td data-col="advertiser_text">{escape(row.get("advertiser_text") or "—")}</td>
            <td data-col="account_id">{escape(row.get("account_id") or "—")}</td>
            <td data-col="budget">{format_money(row.get("budget", 0))}</td>
            <td data-col="spend">{format_money(row.get("spend", 0))}</td>
            <td data-col="clicks">{format_int_or_float(row.get("clicks", 0))}</td>
            <td data-col="leads">{format_int_or_float(row.get("leads", 0))}</td>
            <td data-col="reg">{format_int_or_float(row.get("reg", 0))}</td>
            <td data-col="rate">{format_money(row.get("rate", 0))}</td>
            <td data-col="cost_reg">{format_money(row.get("cost_reg", 0))}</td>
            <td data-col="fb_ftd">{format_int_or_float(row.get("fb_ftd", 0))}</td>
            <td data-col="cpa">{format_money(row.get("cpa", 0))}</td>
            <td data-col="chatterfy">{format_int_or_float(row.get("chatterfy", 0))}</td>
            <td data-col="players_ftd">{format_int_or_float(row.get("players_ftd", 0))}</td>
            <td data-col="qual_ftd">{format_int_or_float(row.get("qual_ftd", 0))}</td>
            <td data-col="hold_count">{format_int_or_float(row.get("hold_count", 0))}</td>
            <td data-col="hold_split">{escape(row.get("hold_split") or "0B / 0W")}</td>
            <td data-col="cap_total">{format_int_or_float(row.get("cap_total", 0))}</td>
            <td data-col="cap_fill">{format_percent(row.get("cap_fill", 0))}</td>
            <td data-col="income">{format_money(row.get("income", 0))}</td>
            <td data-col="profit">{format_money(row.get("profit", 0))}</td>
            <td data-col="roi">{format_percent(row.get("roi", 0))}</td>
        </tr>
        """

    def render_tree_rows(nodes, parent_id="", ancestors=None, level=0, variant="caret"):
        ancestors = ancestors or []
        html = ""
        for node in nodes:
            hidden_attr = ' hidden' if parent_id else ''
            current_ancestors = [*ancestors, node["id"]]
            html += f"""
            <tr class="dashboard-tree-row dashboard-tree-row-level-{level}" data-node-id="{escape(node["id"])}" data-row-key="{escape(node["id"])}" data-parent-id="{escape(parent_id)}" data-ancestors="{escape(','.join(ancestors))}" onclick="window.dashboardHandleRowClick && window.dashboardHandleRowClick(this, event)"{hidden_attr}>
                <td data-col="platform">{render_hierarchy_label(node, level, variant=variant) if node["column"] == "platform" else ""}</td>
                <td data-col="geo">{render_hierarchy_label(node, level, variant=variant) if node["column"] == "geo" else ""}</td>
                <td data-col="manager">{render_hierarchy_label(node, level, variant=variant) if node["column"] == "manager" else ""}</td>
                <td data-col="campaign_name">{render_hierarchy_label(node, level, variant=variant) if node["column"] == "campaign_name" else ""}</td>
                <td data-col="adset_name">{render_hierarchy_label(node, level, variant=variant) if node["column"] == "adset_name" else ""}</td>
                <td data-col="ad_name"></td>
                <td class="dashboard-tree-summary-cell" data-col="buyer"></td>
                <td class="dashboard-tree-summary-cell" data-col="offer"></td>
                <td class="dashboard-tree-summary-cell" data-col="cabinet_text"></td>
                <td class="dashboard-tree-summary-cell" data-col="advertiser_text"></td>
                <td class="dashboard-tree-summary-cell" data-col="account_id"></td>
                {render_dashboard_metric_cells(node["metrics"])}
            </tr>
            """
            if node["children"]:
                html += render_tree_rows(node["children"], parent_id=node["id"], ancestors=current_ancestors, level=level + 1, variant=variant)
            else:
                for leaf_row in _dashboard_sort_rows(node["rows"], sort_by=sort_by, order=order):
                    html += render_leaf_row(leaf_row, parent_id=node["id"], ancestors=current_ancestors)
        return html

    rows_html = render_tree_rows(tree, variant="caret")

    matrix_metric_fields = [
        ("budget", "Budget"),
        ("spend", "Spend"),
        ("clicks", "Clicks"),
        ("leads", "Leads"),
        ("reg", "Reg"),
        ("rate", "Rate"),
        ("cost_reg", "Cost Reg"),
        ("fb_ftd", "FB FTD"),
        ("cpa", "CPA"),
        ("chatterfy", "Chatterfy"),
        ("players_ftd", "Players FTD"),
        ("qual_ftd", "Qual FTD"),
        ("hold_count", "Hold"),
        ("cap_total", "Cap"),
        ("cap_fill", "Cap Fill"),
        ("income", "Income"),
        ("profit", "Profit"),
        ("roi", "ROI"),
    ]

    def serialize_metric_values(values):
        return {
            "budget": format_money(values.get("budget", 0)),
            "spend": format_money(values.get("spend", 0)),
            "clicks": format_int_or_float(values.get("clicks", 0)),
            "leads": format_int_or_float(values.get("leads", 0)),
            "reg": format_int_or_float(values.get("reg", 0)),
            "rate": format_money(values.get("rate", 0)),
            "cost_reg": format_money(values.get("cost_reg", 0)),
            "fb_ftd": format_int_or_float(values.get("fb_ftd", 0)),
            "cpa": format_money(values.get("cpa", 0)),
            "chatterfy": format_int_or_float(values.get("chatterfy", 0)),
            "players_ftd": format_int_or_float(values.get("players_ftd", 0)),
            "qual_ftd": format_int_or_float(values.get("qual_ftd", 0)),
            "hold_count": format_int_or_float(values.get("hold_count", 0)),
            "cap_total": format_int_or_float(values.get("cap_total", 0)),
            "cap_fill": format_percent(values.get("cap_fill", 0)),
            "income": format_money(values.get("income", 0)),
            "profit": format_money(values.get("profit", 0)),
            "roi": format_percent(values.get("roi", 0)),
        }

    def serialize_leaf_payload(row, parent_id="", ancestors=None):
        ancestors = ancestors or []
        leaf_key = "leaf|" + "|".join([
            safe_text(parent_id) or "root",
            safe_text(row.get("platform")).strip() or "—",
            safe_text(row.get("geo")).strip() or "—",
            safe_text(row.get("manager")).strip() or "—",
            safe_text(row.get("campaign_name")).strip() or "—",
            safe_text(row.get("adset_name")).strip() or "—",
            safe_text(row.get("ad_name")).strip() or "—",
            safe_text(row.get("account_id")).strip() or "—",
        ])
        return {
            "id": leaf_key,
            "label": safe_text(row.get("ad_name")).strip() or "—",
            "account_id": safe_text(row.get("account_id")).strip() or "—",
            "buyer": safe_text(row.get("buyer")).strip() or "—",
            "offer": safe_text(row.get("offer")).strip() or "—",
            "cabinet_text": safe_text(row.get("cabinet_text")).strip() or "—",
            "advertiser_text": safe_text(row.get("advertiser_text")).strip() or "—",
            "metrics": serialize_metric_values(row),
            "ancestors": ancestors,
        }

    def serialize_tree_payload(nodes, ancestors=None):
        ancestors = ancestors or []
        payload = []
        for node in nodes:
            node_ancestors = [*ancestors, node["id"]]
            children = serialize_tree_payload(node["children"], node_ancestors)
            leaf_rows = [
                serialize_leaf_payload(leaf_row, parent_id=node["id"], ancestors=node_ancestors)
                for leaf_row in _dashboard_sort_rows(node["rows"], sort_by=sort_by, order=order)
            ] if not node["children"] else []
            payload.append({
                "id": node["id"],
                "label": node["label"],
                "column": node["column"],
                "metrics": serialize_metric_values(node["metrics"]),
                "children": children,
                "ads": leaf_rows,
            })
        return payload

    matrix_tree_json = json.dumps(serialize_tree_payload(tree), ensure_ascii=False)
    matrix_metric_fields_json = json.dumps(matrix_metric_fields, ensure_ascii=False)

    buyer_filter_html = ""
    if is_admin_role(user) or user.get("role") == "operator":
        buyer_filter_html = f'<label class="dashboard-filter-field"><span>Buyer</span><select name="buyer"><option value="">Все</option>{buyer_options}</select></label>'
    else:
        buyer_filter_html = f'<input type="hidden" name="buyer" value="{escape(buyer)}">'

    content = f"""
    <style>
    .dashboard-v2 {{
        display:grid;
        gap:18px;
        width:100%;
        min-width:0;
        max-width:100%;
        overflow-x:hidden;
        font-family:"Segoe UI", "Segoe UI Variable Text", Tahoma, Geneva, Verdana, sans-serif;
    }}
    .dashboard-v2 .dashboard-filters-panel {{
        padding:14px 14px 12px;
        width:100%;
        min-width:0;
        overflow:hidden;
    }}
    .dashboard-v2 .dashboard-filter-grid {{
        display:grid;
        grid-template-columns:repeat(auto-fit, minmax(130px, 1fr));
        gap:8px;
        align-items:end;
        width:100%;
        min-width:0;
    }}
    .dashboard-v2 .dashboard-filter-field {{
        display:grid;
        gap:5px;
        min-width:0;
    }}
    .dashboard-v2 .dashboard-filter-field span {{
        font-size:10px;
        line-height:1;
        letter-spacing:.08em;
        text-transform:uppercase;
        color:#637494;
        font-weight:800;
        padding-left:2px;
    }}
    .dashboard-v2 .dashboard-filter-field select,
    .dashboard-v2 .dashboard-filter-field input {{
        height:36px;
        border-radius:12px;
        font-size:13px;
        padding:0 12px;
    }}
    .dashboard-v2 .dashboard-period-picker {{
        display:grid;
        grid-template-columns:44px minmax(0, 1fr) 44px;
        gap:8px;
        align-items:end;
        min-width:0;
    }}
    .dashboard-v2 .dashboard-period-picker .period-jump-btn {{
        width:44px;
        height:36px;
        border-radius:12px;
        padding:0;
        font-size:20px;
        line-height:1;
    }}
    .dashboard-v2 .dashboard-filter-actions {{
        display:flex;
        gap:10px;
        align-items:end;
        justify-content:flex-end;
    }}
    .dashboard-v2 .dashboard-filter-actions .btn,
    .dashboard-v2 .dashboard-filter-actions .ghost-btn {{
        min-width:96px;
        height:36px;
        border-radius:12px;
    }}
    .dashboard-v2 .dashboard-summary-wrap .stats-grid {{
        grid-template-columns:repeat(auto-fit, minmax(140px, 1fr));
        gap:8px;
    }}
    .dashboard-v2 .dashboard-summary-wrap .stat-card {{
        min-height:84px;
        padding:12px 14px;
        border-radius:16px;
    }}
    .dashboard-v2 .dashboard-summary-wrap .stat-card .name {{
        font-size:10px;
        letter-spacing:.08em;
        text-transform:uppercase;
        color:#6e7f9d;
    }}
    .dashboard-v2 .dashboard-summary-wrap .stat-card .value {{
        font-size:23px;
        line-height:1.05;
    }}
    .dashboard-v2 .dashboard-table-panel {{
        padding:14px;
        min-width:0;
        overflow:hidden;
    }}
    .dashboard-v2 .dashboard-table-header {{
        display:flex;
        justify-content:space-between;
        align-items:flex-end;
        gap:16px;
        margin-bottom:12px;
    }}
    .dashboard-v2 .dashboard-table-title {{
        display:grid;
        gap:4px;
    }}
    .dashboard-v2 .dashboard-table-title .panel-title {{
        margin:0;
    }}
    .dashboard-v2 .dashboard-table-title .panel-subtitle {{
        margin:0;
        max-width:980px;
    }}
    .dashboard-v2 .dashboard-table-wrap {{
        border:1px solid rgba(191, 212, 244, 0.9);
        border-radius:16px;
        overflow-x:auto;
        overflow-y:visible;
        background:#fdfefe;
        width:100%;
        min-width:0;
        max-width:100%;
    }}
    .dashboard-v2 .dashboard-matrix-wrap {{
        display:grid;
        gap:14px;
    }}
    .dashboard-v2 .dashboard-matrix-path {{
        display:flex;
        flex-wrap:wrap;
        gap:8px;
        align-items:center;
        min-height:34px;
    }}
    .dashboard-v2 .dashboard-matrix-path-chip {{
        display:inline-flex;
        align-items:center;
        gap:8px;
        padding:8px 12px;
        border-radius:999px;
        background:#eef5ff;
        color:#18345d;
        font-size:13px;
        font-weight:600;
    }}
    .dashboard-v2 .dashboard-matrix-board {{
        display:grid;
        grid-template-columns:repeat(6, minmax(180px, 1fr));
        gap:12px;
        align-items:start;
    }}
    .dashboard-v2 .dashboard-matrix-column {{
        border:1px solid rgba(197, 214, 241, 0.95);
        border-radius:18px;
        background:#fcfdff;
        overflow:hidden;
    }}
    .dashboard-v2 .dashboard-matrix-column-head {{
        padding:12px 14px;
        background:#eef5ff;
        color:#223a60;
        font-size:11px;
        font-weight:700;
        letter-spacing:.04em;
        text-transform:uppercase;
        border-bottom:1px solid rgba(214, 228, 248, 0.95);
    }}
    .dashboard-v2 .dashboard-matrix-column-body {{
        display:grid;
        gap:4px;
        padding:8px;
        max-height:420px;
        overflow:auto;
    }}
    .dashboard-v2 .dashboard-matrix-item {{
        width:100%;
        display:grid;
        gap:4px;
        text-align:left;
        border:0;
        border-radius:14px;
        padding:10px 12px;
        background:transparent;
        color:#20385f;
        cursor:pointer;
        transition:background .14s ease, box-shadow .14s ease, transform .14s ease;
    }}
    .dashboard-v2 .dashboard-matrix-item:hover {{
        background:#f5f9ff;
    }}
    .dashboard-v2 .dashboard-matrix-item.is-active {{
        background:#beddff;
        box-shadow:inset 0 0 0 1px rgba(54, 116, 209, 0.26);
    }}
    .dashboard-v2 .dashboard-matrix-item-label {{
        font-size:14px;
        font-weight:600;
        line-height:1.2;
        word-break:break-word;
    }}
    .dashboard-v2 .dashboard-matrix-item-sub {{
        display:flex;
        gap:10px;
        flex-wrap:wrap;
        color:#6f84a7;
        font-size:11px;
    }}
    .dashboard-v2 .dashboard-matrix-empty {{
        padding:18px 14px;
        color:#8da0bf;
        font-size:13px;
    }}
    .dashboard-v2 .dashboard-matrix-detail {{
        border:1px solid rgba(197, 214, 241, 0.95);
        border-radius:18px;
        background:#fcfdff;
        overflow:hidden;
    }}
    .dashboard-v2 .dashboard-matrix-detail-head {{
        padding:14px 16px;
        background:#eef5ff;
        border-bottom:1px solid rgba(214, 228, 248, 0.95);
    }}
    .dashboard-v2 .dashboard-matrix-detail-title {{
        color:#1a335b;
        font-size:18px;
        font-weight:700;
    }}
    .dashboard-v2 .dashboard-matrix-detail-subtitle {{
        margin-top:4px;
        color:#6f84a7;
        font-size:13px;
    }}
    .dashboard-v2 .dashboard-matrix-metrics {{
        display:grid;
        grid-template-columns:repeat(auto-fit, minmax(118px, 1fr));
        gap:8px;
        padding:14px 16px;
        border-bottom:1px solid rgba(232, 240, 250, 0.95);
    }}
    .dashboard-v2 .dashboard-matrix-metric {{
        padding:10px 12px;
        border-radius:14px;
        background:#f6f9ff;
    }}
    .dashboard-v2 .dashboard-matrix-metric-label {{
        color:#7287a8;
        font-size:10px;
        font-weight:700;
        text-transform:uppercase;
        letter-spacing:.04em;
    }}
    .dashboard-v2 .dashboard-matrix-metric-value {{
        margin-top:6px;
        color:#19355e;
        font-size:18px;
        font-weight:700;
    }}
    .dashboard-v2 .dashboard-matrix-ads {{
        padding:14px 16px 18px;
    }}
    .dashboard-v2 .dashboard-matrix-ads-title {{
        color:#1f3760;
        font-size:13px;
        font-weight:700;
        margin-bottom:10px;
        text-transform:uppercase;
        letter-spacing:.04em;
    }}
    .dashboard-v2 .dashboard-matrix-ads-table {{
        width:100%;
        border-collapse:separate;
        border-spacing:0;
        font-size:12px;
    }}
    .dashboard-v2 .dashboard-matrix-ads-table th,
    .dashboard-v2 .dashboard-matrix-ads-table td {{
        padding:8px 10px;
        border-bottom:1px solid rgba(229, 238, 249, 0.95);
        text-align:left;
        vertical-align:top;
    }}
    .dashboard-v2 .dashboard-matrix-ads-table th {{
        color:#6e83a6;
        font-size:10px;
        font-weight:700;
        text-transform:uppercase;
        letter-spacing:.04em;
    }}
    .dashboard-v2 #dashboardUnifiedTable {{
        min-width:2740px;
        table-layout:fixed;
        border-collapse:separate;
        border-spacing:0;
        font-size:12px;
        font-family:"Segoe UI", "Segoe UI Variable Text", Tahoma, Geneva, Verdana, sans-serif;
    }}
    .dashboard-v2 #dashboardUnifiedTable thead th {{
        position:sticky;
        top:0;
        z-index:6;
        padding:4px 7px;
        font-size:10px;
        line-height:1.1;
        letter-spacing:.01em;
        text-transform:uppercase;
        white-space:normal;
        word-break:break-word;
        border-bottom:1px solid rgba(191, 212, 244, 0.9);
        border-right:1px solid rgba(221, 233, 248, 0.9);
        color:#213252;
        background:#eef5ff;
    }}
    .dashboard-v2 #dashboardUnifiedTable thead th a {{
        color:inherit;
        text-decoration:none;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr {{
        height:15px;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody td {{
        height:15px;
        padding:0 7px;
        line-height:1.05;
        vertical-align:middle;
        border-bottom:1px solid rgba(221, 233, 248, 0.9);
        border-right:1px solid rgba(229, 238, 249, 0.9);
        white-space:nowrap;
        overflow:hidden;
        text-overflow:ellipsis;
        color:#1e2d4a;
        background:#ffffff;
        user-select:none;
        -webkit-user-select:none;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr:hover td {{
        background:#f6faff;
    }}
    .dashboard-v2 table[data-dashboard-tree-table] tbody tr {{
        cursor:pointer;
    }}
    .dashboard-v2 table[data-dashboard-tree-table] tbody tr.dashboard-row-selected td {{
        background:#b7f0c2 !important;
        color:#153726 !important;
        box-shadow:inset 0 0 0 1px rgba(39, 148, 84, 0.42);
        opacity:1 !important;
    }}
    .dashboard-v2 table[data-dashboard-tree-table] tbody tr.dashboard-row-selected:hover td {{
        background:#b7f0c2 !important;
        color:#153726 !important;
        box-shadow:inset 0 0 0 1px rgba(39, 148, 84, 0.42);
        opacity:1 !important;
    }}
    .dashboard-v2 table[data-dashboard-tree-table].dashboard-has-row-selection tbody tr:not(.dashboard-row-selected) td {{
        opacity:.2;
    }}
    .dashboard-v2 table[data-dashboard-tree-table] td.dashboard-cell-selected {{
        background:#9ae6ae !important;
        color:#143523 !important;
        box-shadow:inset 0 0 0 1px rgba(28, 135, 71, 0.45);
    }}
    .dashboard-v2 table[data-dashboard-tree-table] th.dashboard-column-selected,
    .dashboard-v2 table[data-dashboard-tree-table] td.dashboard-column-selected {{
        background:#c7f3d1 !important;
        color:#163726 !important;
        box-shadow:inset 0 0 0 1px rgba(33, 138, 74, 0.35);
    }}
    .dashboard-v2 table[data-dashboard-tree-table] td.dashboard-cell-selected.dashboard-column-selected {{
        background:#8de0a3 !important;
    }}
    .dashboard-v2 table[data-dashboard-tree-table] td.dashboard-cell-selected .dashboard-tree-toggle,
    .dashboard-v2 table[data-dashboard-tree-table] td.dashboard-column-selected .dashboard-tree-toggle {{
        color:#16325c;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.dashboard-tree-row td {{
        font-weight:600;
        border-top:1px solid rgba(138, 159, 194, 0.14);
        border-bottom:1px solid rgba(138, 159, 194, 0.14);
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.dashboard-tree-row.dashboard-tree-row-open td.dashboard-tree-summary-cell {{
        color:transparent !important;
        text-shadow:none !important;
        user-select:none;
        -webkit-user-select:none;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.dashboard-tree-row.dashboard-tree-row-open td.dashboard-tree-summary-cell::selection {{
        background:transparent;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.dashboard-tree-row td.dashboard-tree-summary-cell {{
        color:#8091af;
        font-weight:500;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.dashboard-tree-row-level-0 td {{
        background:#eef5ff;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.dashboard-tree-row-level-1 td {{
        background:#f7faff;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.dashboard-tree-row-level-2 td {{
        background:#fbfcff;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.dashboard-tree-row-level-3 td {{
        background:#ffffff;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.dashboard-tree-row-level-4 td {{
        background:#ffffff;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.dashboard-tree-row-level-5 td {{
        background:#ffffff;
    }}
    .dashboard-v2 #dashboardUnifiedTable .dashboard-tree-cell {{
        display:flex;
        align-items:center;
        justify-content:flex-start;
        min-height:15px;
    }}
    .dashboard-v2 #dashboardUnifiedTable .dashboard-tree-level-1 {{
        padding-left:0;
    }}
    .dashboard-v2 #dashboardUnifiedTable .dashboard-tree-level-2 {{
        padding-left:0;
    }}
    .dashboard-v2 #dashboardUnifiedTable .dashboard-tree-level-3 {{
        padding-left:0;
    }}
    .dashboard-v2 #dashboardUnifiedTable .dashboard-tree-level-4 {{
        padding-left:0;
    }}
    .dashboard-v2 #dashboardUnifiedTable .dashboard-tree-level-5 {{
        padding-left:0;
    }}
    .dashboard-v2 #dashboardUnifiedTable .dashboard-tree-toggle {{
        display:inline-flex;
        align-items:center;
        gap:6px;
        border:0;
        background:transparent;
        padding:0;
        margin:0;
        color:#213252;
        font:inherit;
        cursor:pointer;
        justify-content:flex-start;
        user-select:none;
        -webkit-user-select:none;
        font-weight:600;
    }}
    .dashboard-v2 #dashboardUnifiedTable .dashboard-tree-caret {{
        width:10px;
        display:inline-flex;
        justify-content:center;
        color:#647da8;
        transition:transform .18s ease;
    }}
    .dashboard-v2 #dashboardUnifiedTable .dashboard-tree-toggle[aria-expanded="true"] .dashboard-tree-caret {{
        transform:rotate(90deg);
    }}
    .dashboard-v2 .dashboard-tree-plus {{
        width:14px;
        height:14px;
        display:inline-flex;
        align-items:center;
        justify-content:center;
        border:1px solid rgba(95, 120, 165, 0.35);
        border-radius:4px;
        color:#4e6ba1;
        font-size:11px;
        font-weight:900;
        line-height:1;
        background:#ffffff;
    }}
    .dashboard-v2 .dashboard-tree-toggle[aria-expanded="true"] .dashboard-tree-plus {{
        font-size:13px;
    }}
    .dashboard-v2 .dashboard-tree-toggle[aria-expanded="true"] .dashboard-tree-plus::before {{
        content:"−";
    }}
    .dashboard-v2 .dashboard-tree-toggle-plus .dashboard-tree-plus {{
        position:relative;
    }}
    .dashboard-v2 .dashboard-tree-toggle-plus .dashboard-tree-plus::before {{
        content:"+";
    }}
    .dashboard-v2 #dashboardUnifiedTable .dashboard-tree-label {{
        overflow:hidden;
        text-overflow:ellipsis;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.dashboard-leaf-row td {{
        font-weight:400;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="buyer"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="buyer"] {{
        width:78px;
        min-width:78px;
        max-width:78px;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="platform"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="platform"] {{
        width:96px;
        min-width:96px;
        max-width:96px;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="manager"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="manager"] {{
        width:110px;
        min-width:110px;
        max-width:110px;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="geo"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="geo"] {{
        width:64px;
        min-width:64px;
        max-width:64px;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="offer"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="offer"] {{
        width:102px;
        min-width:102px;
        max-width:102px;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="cabinet_text"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="cabinet_text"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="advertiser_text"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="advertiser_text"] {{
        width:104px;
        min-width:104px;
        max-width:104px;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="account_id"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="account_id"] {{
        width:122px;
        min-width:122px;
        max-width:122px;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="campaign_name"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="campaign_name"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="adset_name"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="adset_name"] {{
        width:188px;
        min-width:188px;
        max-width:188px;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="ad_name"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="ad_name"] {{
        width:220px;
        min-width:220px;
        max-width:220px;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="budget"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="budget"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="spend"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="spend"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="rate"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="rate"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="cost_reg"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="cost_reg"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="cpa"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="cpa"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="income"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="income"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="profit"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="profit"] {{
        width:92px;
        min-width:92px;
        max-width:92px;
        text-align:right;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="clicks"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="clicks"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="leads"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="leads"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="reg"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="reg"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="fb_ftd"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="fb_ftd"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="chatterfy"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="chatterfy"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="players_ftd"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="players_ftd"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="qual_ftd"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="qual_ftd"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="hold_count"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="hold_count"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="cap_total"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="cap_total"] {{
        width:70px;
        min-width:70px;
        max-width:70px;
        text-align:right;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="hold_split"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="hold_split"] {{
        width:82px;
        min-width:82px;
        max-width:82px;
        text-align:center;
    }}
    .dashboard-v2 #dashboardUnifiedTable td[data-col="cap_fill"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="cap_fill"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="roi"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="roi"] {{
        width:72px;
        min-width:72px;
        max-width:72px;
        text-align:right;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="platform"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="platform"] {{
        background:#ecf5ff;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="buyer"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="buyer"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="manager"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="manager"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="geo"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="geo"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="offer"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="offer"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="cabinet_text"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="cabinet_text"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="advertiser_text"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="advertiser_text"] {{
        background:#f7fbff;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="budget"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="budget"] {{
        background:#eef9ef;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="spend"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="spend"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="clicks"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="clicks"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="leads"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="leads"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="reg"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="reg"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="fb_ftd"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="fb_ftd"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="cpa"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="cpa"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="cost_reg"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="cost_reg"] {{
        background:#fff4e8;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="chatterfy"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="chatterfy"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="players_ftd"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="players_ftd"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="qual_ftd"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="qual_ftd"] {{
        background:#f3efff;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="hold_count"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="hold_count"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="hold_split"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="hold_split"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="cap_total"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="cap_total"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="cap_fill"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="cap_fill"] {{
        background:#fff8df;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="income"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="income"] {{
        background:#edf8e7;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="profit"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="profit"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="roi"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="roi"] {{
        background:#e7f8fb;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.soft-green td[data-col="profit"],
    .dashboard-v2 #dashboardUnifiedTable tbody tr.soft-green td[data-col="roi"] {{
        color:#0f8c58;
        font-weight:800;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr.soft-red td[data-col="profit"],
    .dashboard-v2 #dashboardUnifiedTable tbody tr.soft-red td[data-col="roi"] {{
        color:#d84c57;
        font-weight:800;
    }}
    .dashboard-v2 table[data-dashboard-tree-table] {{
        table-layout:auto;
        width:max-content;
        min-width:100%;
    }}
    .dashboard-v2 table[data-dashboard-tree-table] tbody td {{
        overflow:visible;
        text-overflow:clip;
    }}
    .dashboard-v2 table[data-dashboard-tree-table] th[data-col="buyer"],
    .dashboard-v2 table[data-dashboard-tree-table] td[data-col="buyer"],
    .dashboard-v2 table[data-dashboard-tree-table] th[data-col="offer"],
    .dashboard-v2 table[data-dashboard-tree-table] td[data-col="offer"],
    .dashboard-v2 table[data-dashboard-tree-table] th[data-col="cabinet_text"],
    .dashboard-v2 table[data-dashboard-tree-table] td[data-col="cabinet_text"],
    .dashboard-v2 table[data-dashboard-tree-table] th[data-col="advertiser_text"],
    .dashboard-v2 table[data-dashboard-tree-table] td[data-col="advertiser_text"],
    .dashboard-v2 table[data-dashboard-tree-table] th[data-col="account_id"],
    .dashboard-v2 table[data-dashboard-tree-table] td[data-col="account_id"] {{
        width:auto !important;
        min-width:0 !important;
        max-width:none !important;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="platform"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="platform"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="geo"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="geo"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="manager"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="manager"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="campaign_name"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="campaign_name"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="adset_name"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="adset_name"],
    .dashboard-v2 #dashboardUnifiedTable th[data-col="ad_name"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="ad_name"] {{
        position:sticky;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="platform"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="platform"] {{
        left:var(--sticky-left-platform, 0px);
        z-index:7;
        box-shadow:none;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="geo"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="geo"] {{
        left:var(--sticky-left-geo, 0px);
        z-index:7;
        box-shadow:none;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="manager"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="manager"] {{
        left:var(--sticky-left-manager, 0px);
        z-index:7;
        box-shadow:none;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="campaign_name"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="campaign_name"] {{
        left:var(--sticky-left-campaign_name, 0px);
        z-index:7;
        box-shadow:none;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="adset_name"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="adset_name"] {{
        left:var(--sticky-left-adset_name, 0px);
        z-index:7;
        box-shadow:none;
    }}
    .dashboard-v2 #dashboardUnifiedTable th[data-col="ad_name"],
    .dashboard-v2 #dashboardUnifiedTable td[data-col="ad_name"] {{
        left:var(--sticky-left-ad_name, 0px);
        z-index:7;
        box-shadow:none;
    }}
    .dashboard-v2 #dashboardUnifiedTable thead th[data-col="platform"],
    .dashboard-v2 #dashboardUnifiedTable thead th[data-col="geo"],
    .dashboard-v2 #dashboardUnifiedTable thead th[data-col="manager"],
    .dashboard-v2 #dashboardUnifiedTable thead th[data-col="campaign_name"],
    .dashboard-v2 #dashboardUnifiedTable thead th[data-col="adset_name"],
    .dashboard-v2 #dashboardUnifiedTable thead th[data-col="ad_name"] {{
        z-index:9;
    }}
    .dashboard-v2 #dashboardUnifiedTable tbody tr:hover td[data-col="platform"],
    .dashboard-v2 #dashboardUnifiedTable tbody tr:hover td[data-col="geo"],
    .dashboard-v2 #dashboardUnifiedTable tbody tr:hover td[data-col="manager"],
    .dashboard-v2 #dashboardUnifiedTable tbody tr:hover td[data-col="campaign_name"],
    .dashboard-v2 #dashboardUnifiedTable tbody tr:hover td[data-col="adset_name"],
    .dashboard-v2 #dashboardUnifiedTable tbody tr:hover td[data-col="ad_name"] {{
        background:#f6faff;
    }}
    @media (max-width: 1500px) {{
        .dashboard-v2 .dashboard-filter-grid {{
            grid-template-columns:repeat(auto-fit, minmax(140px, 1fr));
        }}
        .dashboard-v2 .dashboard-summary-wrap .stats-grid {{
            grid-template-columns:repeat(auto-fit, minmax(150px, 1fr));
        }}
        .dashboard-v2 .dashboard-table-header {{
            flex-direction:column;
            align-items:stretch;
        }}
    }}
    </style>
    <div class="dashboard-v2">
    {render_active_period_banner(effective_period_label)}

    <div class="panel compact-panel dashboard-filters-panel">
        <form method="get" action="/dashboard" data-persist-filters="dashboard-v2" class="dashboard-filter-grid">
            {buyer_filter_html}
            <input type="hidden" name="period_view" value="period">
            <div class="dashboard-period-picker" style="grid-column:span 3;">
                <button type="button" class="ghost-btn small-btn period-jump-btn" data-period-jump="-1" aria-label="Previous period">‹</button>
                <label class="dashboard-filter-field">
                    <span>Period</span>
                    <select name="period_label" id="dashboardPeriodSelect">{period_options}</select>
                </label>
                <button type="button" class="ghost-btn small-btn period-jump-btn" data-period-jump="1" aria-label="Next period">›</button>
            </div>
            <label class="dashboard-filter-field">
                <span>Platform</span>
                <select name="platform">{platform_options}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Manager</span>
                <select name="manager">{manager_options}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Geo</span>
                <select name="geo">{geo_options}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Offer</span>
                <select name="offer">{offer_options}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Cabinet</span>
                <select name="cabinet_name">{cabinet_options}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Advertiser</span>
                <select name="advertiser">{advertiser_options}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Campaign</span>
                <select name="campaign_name">{campaign_options}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Ad Group</span>
                <select name="adset_name">{adset_options}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Ad</span>
                <select name="ad_name">{ad_name_options}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Account</span>
                <select name="account_id">{account_options}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Source</span>
                <select name="source_name">{source_options}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Caps</span>
                <select name="has_caps">{yes_no_options(has_caps)}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Hold</span>
                <select name="has_hold">{yes_no_options(has_hold)}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Chat</span>
                <select name="has_chatterfy">{yes_no_options(has_chatterfy)}</select>
            </label>
            <label class="dashboard-filter-field">
                <span>Players</span>
                <select name="has_players">{yes_no_options(has_players)}</select>
            </label>
            <label class="dashboard-filter-field" style="grid-column:span 3;">
                <span>Search</span>
                <input type="text" name="search" value="{escape(search)}" placeholder="Campaign, ad, cabinet, advertiser, geo, account...">
            </label>
            <input type="hidden" name="sort_by" value="{escape(sort_by)}">
            <input type="hidden" name="order" value="{escape(order)}">
            <input type="hidden" name="dashboard_state" id="dashboardStateInput" value="{escape(dashboard_state_param)}">
            <div class="dashboard-filter-actions" style="grid-column:span 2;">
                <button type="submit" class="btn small-btn">Filter</button>
                <a href="/dashboard?period_view=period&period_label={quote_plus(effective_period_label)}&dashboard_state={quote_plus(dashboard_state_param)}" class="ghost-btn small-btn" data-reset-filters="dashboard-v2">Reset</a>
            </div>
        </form>
    </div>

    <div class="panel compact-panel dashboard-table-panel">
        <div class="dashboard-table-header">
            <div class="dashboard-table-title">
                <div class="panel-title">CRM Analytics Matrix</div>
                <div class="panel-subtitle">Power BI style analytical drill-down across Brand, GEO, Cabinet, Campaign, Adset and Ad.</div>
            </div>
        </div>
        <div class="dashboard-matrix-wrap">
            <div class="dashboard-matrix-path" id="dashboardMatrixPath"></div>
            <div class="dashboard-matrix-board" id="dashboardMatrixBoard"></div>
            <div class="dashboard-matrix-detail" id="dashboardMatrixDetail"></div>
        </div>
    </div>

    <div class="panel compact-panel dashboard-table-panel">
        <div class="dashboard-table-header">
            <div class="dashboard-table-title">
                <div class="panel-title">CRM Analytics Table</div>
            </div>
            <details class="upload-menu upload-menu-right" id="dashboardColumnsMenu">
                <summary class="ghost-btn small-btn">Columns</summary>
                <div class="upload-menu-list" style="width:min(560px, calc(100vw - 48px));">
                    <div class="panel-subtitle">Choose which columns to keep visible in Dashboard.</div>
                    <div style="display:flex; gap:10px; margin-top:10px; flex-wrap:wrap;">
                        <button type="button" class="ghost-btn small-btn" id="dashboardShowAllColumns">Show all</button>
                    </div>
                    <div style="display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:10px; margin-top:12px;">
                        {column_chips}
                    </div>
                </div>
            </details>
        </div>
        <div class="dashboard-table-wrap">
            <table id="dashboardUnifiedTable" data-dashboard-tree-table>
                <thead><tr>{head_html}</tr></thead>
                <tbody>{rows_html if rows_html else '<tr><td colspan="31">No dashboard rows for the selected filters</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    </div>

    <script>
    (() => {{
        const periodSelect = document.getElementById("dashboardPeriodSelect");
        if (!periodSelect) return;
        const form = periodSelect.closest('form');
        const dashboardStateInput = document.getElementById('dashboardStateInput');
        const initialDashboardStateParam = {json.dumps(dashboard_state_param)};
        const applyDashboardStateToLinks = (serializedState) => {{
            const value = serializedState || '';
            document.querySelectorAll('.dashboard-sort-link, [data-reset-filters="dashboard-v2"]').forEach((link) => {{
                try {{
                    const url = new URL(link.href, window.location.origin);
                    if (value) url.searchParams.set('dashboard_state', value);
                    else url.searchParams.delete('dashboard_state');
                    link.href = url.pathname + url.search;
                }} catch (_error) {{}}
            }});
            if (dashboardStateInput) dashboardStateInput.value = value;
        }};
        const syncDashboardStateParam = () => {{
            try {{
                const rawState = localStorage.getItem(window.teambeadStorageKey('dashboard-ui-state')) || '';
                applyDashboardStateToLinks(rawState);
            }} catch (_error) {{
                applyDashboardStateToLinks('');
            }}
        }};
        if (initialDashboardStateParam) {{
            try {{
                localStorage.setItem(window.teambeadStorageKey('dashboard-ui-state'), initialDashboardStateParam);
            }} catch (_error) {{}}
        }}
        const persistDashboardUiState = () => {{
            if (window.dashboardPersistAllTreeState) window.dashboardPersistAllTreeState();
            syncDashboardStateParam();
        }};
        document.querySelectorAll('.period-jump-btn').forEach((button) => {{
            button.addEventListener('click', () => {{
                const direction = Number(button.dataset.periodJump || '0');
                const options = Array.from(periodSelect.options).filter(option => option.value);
                const currentIndex = options.findIndex(option => option.value === periodSelect.value);
                if (currentIndex < 0) return;
                const targetIndex = currentIndex + direction;
                if (targetIndex < 0 || targetIndex >= options.length) return;
                periodSelect.value = options[targetIndex].value;
                if (form) {{
                    persistDashboardUiState();
                    form.requestSubmit();
                }}
            }});
        }});

        window.dashboardTreeToggle = (button) => {{
            if (!button) return false;
            const table = button.closest('[data-dashboard-tree-table]');
            if (!table) return false;
            const parentRow = button.closest('tr[data-row-key]');
            if (parentRow && window.dashboardToggleRowSelection) {{
                window.dashboardToggleRowSelection(parentRow, true);
            }}
            const nodeId = button.dataset.target || '';
            if (!nodeId) return false;
            const treeRows = Array.from(table.querySelectorAll('tbody tr'));
            const treeButtons = Array.from(table.querySelectorAll('.dashboard-tree-toggle'));
            const syncExpandedSummaryRows = () => {{
                treeRows.forEach((row) => {{
                    if (!row.dataset.nodeId) return;
                    const rowButton = row.querySelector('.dashboard-tree-toggle');
                    row.classList.toggle('dashboard-tree-row-open', rowButton?.getAttribute('aria-expanded') === 'true');
                }});
            }};
            const hideDescendants = (currentNodeId) => {{
                treeRows.forEach((row) => {{
                    const ancestors = (row.dataset.ancestors || '').split(',').filter(Boolean);
                    if (!ancestors.includes(currentNodeId)) return;
                    row.hidden = true;
                    if (row.dataset.nodeId) {{
                        const nestedButton = row.querySelector('.dashboard-tree-toggle');
                        if (nestedButton) nestedButton.setAttribute('aria-expanded', 'false');
                        row.classList.remove('dashboard-tree-row-open');
                    }}
                }});
            }};
            const showDirectChildren = (currentNodeId) => {{
                treeRows.forEach((row) => {{
                    if ((row.dataset.parentId || '') !== currentNodeId) return;
                    row.hidden = false;
                }});
            }};
            const expanded = button.getAttribute('aria-expanded') === 'true';
            if (expanded) {{
                button.setAttribute('aria-expanded', 'false');
                hideDescendants(nodeId);
            }} else {{
                button.setAttribute('aria-expanded', 'true');
                showDirectChildren(nodeId);
            }}
            const openNodes = treeButtons
                .filter((item) => item.getAttribute('aria-expanded') === 'true')
                .map((item) => item.dataset.target || '')
                .filter(Boolean);
            if (window.dashboardWriteState) {{
                const state = window.dashboardReadState();
                state.expanded = state.expanded || {{}};
                state.expanded[table.id || 'dashboard-tree-table'] = openNodes;
                window.dashboardWriteState(state);
            }}
            syncExpandedSummaryRows();
            if (window.dashboardTreeAutoSize) window.dashboardTreeAutoSize(table);
            return false;
        }};

        window.dashboardTreeAutoSize = (table) => {{
            if (!table) return;
            const autoCols = [
                'platform', 'geo', 'manager', 'campaign_name', 'adset_name', 'ad_name',
                'buyer', 'offer', 'cabinet_text', 'advertiser_text', 'account_id',
            ];
            const stickyCols = ['platform', 'geo', 'manager', 'campaign_name', 'adset_name', 'ad_name'];
            autoCols.forEach((col) => {{
                const allCells = Array.from(table.querySelectorAll(`[data-col="${{col}}"]`));
                allCells.forEach((cell) => {{
                    cell.style.width = '';
                    cell.style.minWidth = '';
                    cell.style.maxWidth = '';
                }});
            }});
            table.style.tableLayout = 'auto';
            requestAnimationFrame(() => {{
                const measureColumnWidth = (col) => {{
                    const cells = Array.from(table.querySelectorAll(`[data-col="${{col}}"]`)).filter((cell) => {{
                        if (!cell) return false;
                        const style = window.getComputedStyle(cell);
                        return style.display !== 'none';
                    }});
                    if (!cells.length) return 0;
                    let width = 0;
                    cells.forEach((cell) => {{
                        const style = window.getComputedStyle(cell);
                        const paddingLeft = parseFloat(style.paddingLeft || '0') || 0;
                        const paddingRight = parseFloat(style.paddingRight || '0') || 0;
                        const contentWidth = Math.max(cell.scrollWidth, cell.firstElementChild?.scrollWidth || 0, cell.textContent?.trim() ? cell.scrollWidth : 0);
                        width = Math.max(width, Math.ceil(contentWidth + paddingLeft + paddingRight + 2));
                    }});
                    return width;
                }};
                const widths = {{}};
                autoCols.forEach((col) => {{
                    const measuredWidth = measureColumnWidth(col);
                    if (!measuredWidth) return;
                    widths[col] = measuredWidth;
                    Array.from(table.querySelectorAll(`[data-col="${{col}}"]`)).forEach((cell) => {{
                        cell.style.width = `${{measuredWidth}}px`;
                        cell.style.minWidth = `${{measuredWidth}}px`;
                        cell.style.maxWidth = `${{measuredWidth}}px`;
                    }});
                }});
                let currentLeft = 0;
                stickyCols.forEach((col) => {{
                    table.style.setProperty(`--sticky-left-${{col}}`, `${{currentLeft}}px`);
                    currentLeft += widths[col] || 0;
                }});
            }});
        }};

        const dashboardStateKey = window.teambeadStorageKey('dashboard-ui-state');
        window.dashboardReadState = () => {{
            try {{
                const parsed = JSON.parse(localStorage.getItem(dashboardStateKey) || '{{}}');
                return parsed && typeof parsed === 'object' ? parsed : {{}};
            }} catch (_error) {{
                return {{}};
            }}
        }};
        window.dashboardWriteState = (state) => {{
            try {{
                localStorage.setItem(dashboardStateKey, JSON.stringify(state || {{}}));
            }} catch (_error) {{}}
            syncDashboardStateParam();
        }};
        window.dashboardPersistAllTreeState = () => {{
            const state = window.dashboardReadState();
            state.expanded = state.expanded || {{}};
            document.querySelectorAll('[data-dashboard-tree-table]').forEach((table) => {{
                const openNodes = Array.from(table.querySelectorAll('.dashboard-tree-toggle'))
                    .filter((button) => button.getAttribute('aria-expanded') === 'true')
                    .map((button) => button.dataset.target || '')
                    .filter(Boolean);
                state.expanded[table.id || 'dashboard-tree-table'] = openNodes;
            }});
            window.dashboardWriteState(state);
        }};
        window.dashboardApplySelectedRows = (table) => {{
            if (!table) return;
            const state = window.dashboardReadState();
            const selectedRows = state.selectedRows || {{}};
            const selectedKeys = new Set(
                Array.isArray(selectedRows[table.id || 'dashboard-tree-table'])
                    ? selectedRows[table.id || 'dashboard-tree-table']
                    : []
            );
            Array.from(table.querySelectorAll('tbody tr[data-row-key]')).forEach((row) => {{
                row.classList.toggle('dashboard-row-selected', selectedKeys.has(row.dataset.rowKey || ''));
            }});
            table.classList.toggle(
                'dashboard-has-row-selection',
                Array.from(table.querySelectorAll('tbody tr.dashboard-row-selected')).length > 0
            );
        }};
        window.dashboardPersistSelectedRows = (table) => {{
            if (!table) return;
            const state = window.dashboardReadState();
            state.selectedRows = state.selectedRows || {{}};
            state.selectedRows[table.id || 'dashboard-tree-table'] = Array.from(
                table.querySelectorAll('tbody tr.dashboard-row-selected[data-row-key]')
            )
                .map((row) => row.dataset.rowKey || '')
                .filter(Boolean);
            window.dashboardWriteState(state);
        }};
        window.dashboardToggleRowSelection = (row, forceSelect = false) => {{
            if (!row) return;
            const table = row.closest('[data-dashboard-tree-table]');
            if (!table) return;
            const rowKey = row.dataset.rowKey || '';
            const ancestors = (row.dataset.ancestors || '').split(',').filter(Boolean);
            const selectionPath = rowKey ? [...ancestors, rowKey] : [...ancestors];
            const selectedKeys = new Set(
                Array.from(table.querySelectorAll('tbody tr.dashboard-row-selected[data-row-key]'))
                    .map((selectedRow) => selectedRow.dataset.rowKey || '')
                    .filter(Boolean)
            );
            const shouldSelect = forceSelect || !selectionPath.every((key) => selectedKeys.has(key));
            selectionPath.forEach((key) => {{
                if (!key) return;
                if (shouldSelect) selectedKeys.add(key);
                else selectedKeys.delete(key);
            }});
            Array.from(table.querySelectorAll('tbody tr[data-row-key]')).forEach((candidateRow) => {{
                const candidateKey = candidateRow.dataset.rowKey || '';
                candidateRow.classList.toggle('dashboard-row-selected', selectedKeys.has(candidateKey));
            }});
            table.classList.toggle(
                'dashboard-has-row-selection',
                Array.from(table.querySelectorAll('tbody tr.dashboard-row-selected')).length > 0
            );
            window.dashboardPersistSelectedRows(table);
        }};
        window.dashboardHandleRowClick = (row, event) => {{
            if (!row) return false;
            if (event?.target?.closest('.dashboard-tree-toggle, a, input, select, label, summary, button')) return false;
            if (window.dashboardToggleRowSelection) window.dashboardToggleRowSelection(row);
            return false;
        }};
        window.dashboardApplySelectedColumns = (table) => {{
            if (!table) return;
            const state = window.dashboardReadState();
            const selectedColumns = state.selectedColumns || {{}};
            const selectedCols = new Set(
                Array.isArray(selectedColumns[table.id || 'dashboard-tree-table'])
                    ? selectedColumns[table.id || 'dashboard-tree-table']
                    : []
            );
            Array.from(table.querySelectorAll('[data-col]')).forEach((cell) => {{
                cell.classList.toggle('dashboard-column-selected', selectedCols.has(cell.dataset.col || ''));
            }});
        }};
        window.dashboardPersistSelectedColumns = (table) => {{
            if (!table) return;
            const state = window.dashboardReadState();
            state.selectedColumns = state.selectedColumns || {{}};
            const uniqueCols = Array.from(new Set(
                Array.from(table.querySelectorAll('.dashboard-column-selected[data-col]'))
                    .map((cell) => cell.dataset.col || '')
                    .filter(Boolean)
            ));
            state.selectedColumns[table.id || 'dashboard-tree-table'] = uniqueCols;
            window.dashboardWriteState(state);
        }};
        window.dashboardToggleColumnSelection = (table, columnName) => {{
            if (!table || !columnName) return;
            const columnCells = Array.from(table.querySelectorAll(`[data-col="${{columnName}}"]`));
            const shouldSelect = !columnCells.every((cell) => cell.classList.contains('dashboard-column-selected'));
            columnCells.forEach((cell) => {{
                cell.classList.toggle('dashboard-column-selected', shouldSelect);
            }});
            window.dashboardPersistSelectedColumns(table);
        }};
        window.dashboardApplySelectedCells = (table) => {{
            if (!table) return;
            const state = window.dashboardReadState();
            const selectedCells = state.selectedCells || {{}};
            const selectedKeys = new Set(
                Array.isArray(selectedCells[table.id || 'dashboard-tree-table'])
                    ? selectedCells[table.id || 'dashboard-tree-table']
                    : []
            );
            Array.from(table.querySelectorAll('tbody td[data-col]')).forEach((cell) => {{
                const row = cell.closest('tr[data-row-key]');
                const rowKey = row?.dataset.rowKey || '';
                const cellKey = rowKey && cell.dataset.col ? `${{rowKey}}::${{cell.dataset.col}}` : '';
                cell.classList.toggle('dashboard-cell-selected', selectedKeys.has(cellKey));
            }});
        }};
        window.dashboardPersistSelectedCells = (table) => {{
            if (!table) return;
            const state = window.dashboardReadState();
            state.selectedCells = state.selectedCells || {{}};
            state.selectedCells[table.id || 'dashboard-tree-table'] = Array.from(
                table.querySelectorAll('tbody td.dashboard-cell-selected[data-col]')
            ).map((cell) => {{
                const row = cell.closest('tr[data-row-key]');
                const rowKey = row?.dataset.rowKey || '';
                return rowKey && cell.dataset.col ? `${{rowKey}}::${{cell.dataset.col}}` : '';
            }}).filter(Boolean);
            window.dashboardWriteState(state);
        }};
        window.dashboardToggleCellSelection = (cell) => {{
            if (!cell) return;
            const table = cell.closest('[data-dashboard-tree-table]');
            if (!table) return;
            cell.classList.toggle('dashboard-cell-selected');
            window.dashboardPersistSelectedCells(table);
        }};
        window.restoreDashboardUiState = () => {{
            document.querySelectorAll('[data-dashboard-tree-table]').forEach((table) => {{
            const getTreeButtons = () => Array.from(table.querySelectorAll('.dashboard-tree-toggle'));
            const getTreeRows = () => Array.from(table.querySelectorAll('tbody tr'));
            const getButtonMap = () => new Map(getTreeButtons().map((button) => [button.dataset.target || '', button]));
            const readExpandedNodes = () => {{
                const state = window.dashboardReadState();
                const expanded = state.expanded || {{}};
                const value = expanded[table.id || 'dashboard-tree-table'];
                return Array.isArray(value) ? value : [];
            }};
            const hideDescendants = (nodeId) => {{
                getTreeRows().forEach((row) => {{
                    const ancestors = (row.dataset.ancestors || '').split(',').filter(Boolean);
                    if (!ancestors.includes(nodeId)) return;
                    row.hidden = true;
                    if (row.dataset.nodeId) {{
                        const button = row.querySelector('.dashboard-tree-toggle');
                        if (button) button.setAttribute('aria-expanded', 'false');
                    }}
                }});
            }};
            const saveExpandedState = () => {{
                const openNodes = getTreeButtons()
                    .filter((button) => button.getAttribute('aria-expanded') === 'true')
                    .map((button) => button.dataset.target || '')
                    .filter(Boolean);
                const state = window.dashboardReadState();
                state.expanded = state.expanded || {{}};
                state.expanded[table.id || 'dashboard-tree-table'] = openNodes;
                window.dashboardWriteState(state);
            }};
            const showDirectChildren = (nodeId) => {{
                getTreeRows().forEach((row) => {{
                    if ((row.dataset.parentId || '') !== nodeId) return;
                    row.hidden = false;
                }});
            }};
            const expandNode = (button) => {{
                const nodeId = button.dataset.target || '';
                if (!nodeId) return;
                button.setAttribute('aria-expanded', 'true');
                showDirectChildren(nodeId);
            }};
            const collapseNode = (button) => {{
                const nodeId = button.dataset.target || '';
                if (!nodeId) return;
                button.setAttribute('aria-expanded', 'false');
                hideDescendants(nodeId);
            }};
                getTreeRows().forEach((row) => {{
                    if (row.dataset.parentId) row.hidden = true;
                    if (row.dataset.nodeId) {{
                        const button = row.querySelector('.dashboard-tree-toggle');
                        if (button) button.setAttribute('aria-expanded', 'false');
                    }}
                }});
                readExpandedNodes().forEach((nodeId) => {{
                    const button = getButtonMap().get(nodeId);
                    if (!button) return;
                    const parentRow = button.closest('tr');
                    const ancestors = (parentRow?.dataset.ancestors || '').split(',').filter(Boolean);
                    ancestors.forEach((ancestorId) => {{
                        const ancestorButton = getButtonMap().get(ancestorId);
                        if (ancestorButton) expandNode(ancestorButton);
                    }});
                    expandNode(button);
                }});
                getTreeRows().forEach((row) => {{
                    if (!row.dataset.nodeId) return;
                    const rowButton = row.querySelector('.dashboard-tree-toggle');
                    row.classList.toggle('dashboard-tree-row-open', rowButton?.getAttribute('aria-expanded') === 'true');
                }});
                window.dashboardTreeAutoSize(table);
                if (window.dashboardApplySelectedRows) window.dashboardApplySelectedRows(table);
                if (window.dashboardApplySelectedColumns) window.dashboardApplySelectedColumns(table);
                if (window.dashboardApplySelectedCells) window.dashboardApplySelectedCells(table);
            }});
        }};
        const scheduleDashboardUiRestore = () => {{
            requestAnimationFrame(() => {{
                if (window.restoreDashboardUiState) window.restoreDashboardUiState();
            }});
            window.setTimeout(() => {{
                if (window.restoreDashboardUiState) window.restoreDashboardUiState();
            }}, 60);
            window.setTimeout(() => {{
                if (window.restoreDashboardUiState) window.restoreDashboardUiState();
            }}, 180);
        }};
        scheduleDashboardUiRestore();
        syncDashboardStateParam();
        document.querySelectorAll('.dashboard-sort-link').forEach((link) => {{
            link.addEventListener('click', () => {{
                persistDashboardUiState();
            }});
        }});
        document.querySelectorAll('.dashboard-filter-actions .btn, .dashboard-filter-actions .ghost-btn').forEach((button) => {{
            button.addEventListener('click', () => {{
                persistDashboardUiState();
            }});
        }});
        form?.addEventListener('submit', () => {{
            persistDashboardUiState();
        }});
        window.addEventListener('pagehide', () => {{
            persistDashboardUiState();
        }});
        window.addEventListener('beforeunload', () => {{
            persistDashboardUiState();
        }});
        window.addEventListener('resize', () => {{
            document.querySelectorAll('[data-dashboard-tree-table]').forEach((table) => {{
                window.dashboardTreeAutoSize(table);
            }});
        }});
        window.addEventListener('pageshow', () => {{
            scheduleDashboardUiRestore();
        }});
        document.addEventListener('visibilitychange', () => {{
            if (!document.hidden) {{
                scheduleDashboardUiRestore();
            }}
        }});

        const hiddenKey = window.teambeadStorageKey('dashboard-columns-hidden');
        const toggles = Array.from(document.querySelectorAll('.dashboard-column-toggle'));
        let dashboardCellPaintSelection = null;
        const applyColumns = () => {{
            let hidden = [];
            try {{
                const state = window.dashboardReadState();
                hidden = Array.isArray(state.hiddenColumns) ? state.hiddenColumns : JSON.parse(localStorage.getItem(hiddenKey) || '[]');
            }} catch (_error) {{
                hidden = [];
            }}
            toggles.forEach((toggle) => {{
                toggle.checked = !hidden.includes(toggle.value);
            }});
            document.querySelectorAll('[data-dashboard-tree-table] [data-col]').forEach((cell) => {{
                cell.style.display = hidden.includes(cell.dataset.col) ? 'none' : '';
            }});
            document.querySelectorAll('[data-dashboard-tree-table]').forEach((table) => {{
                window.dashboardTreeAutoSize(table);
            }});
        }};
        document.querySelectorAll('.dashboard-filter-grid select, .dashboard-filter-grid input').forEach((field) => {{
            const eventName = field.tagName === 'SELECT' ? 'change' : 'input';
            field.addEventListener(eventName, () => {{
                persistDashboardUiState();
            }});
            if (eventName !== 'change') {{
                field.addEventListener('change', () => {{
                    persistDashboardUiState();
                }});
            }}
        }});
        const saveColumns = () => {{
            const hidden = toggles.filter((toggle) => !toggle.checked).map((toggle) => toggle.value);
            const state = window.dashboardReadState();
            state.hiddenColumns = hidden;
            window.dashboardWriteState(state);
            localStorage.setItem(hiddenKey, JSON.stringify(hidden));
            applyColumns();
        }};
        toggles.forEach((toggle) => toggle.addEventListener('change', saveColumns));
        const showAllButton = document.getElementById('dashboardShowAllColumns');
        if (showAllButton) {{
            showAllButton.addEventListener('click', () => {{
                const state = window.dashboardReadState();
                state.hiddenColumns = [];
                window.dashboardWriteState(state);
                localStorage.setItem(hiddenKey, JSON.stringify([]));
                applyColumns();
            }});
        }}
        applyColumns();
        document.querySelectorAll('[data-dashboard-tree-table]').forEach((table) => {{
            table.addEventListener('mousedown', (event) => {{
                if (event.target.closest('.dashboard-tree-toggle, a, input, select, label, summary, button')) return;
                const cell = event.target.closest('tbody td[data-col]');
                if (!cell || !table.contains(cell)) return;
                event.preventDefault();
                event.stopPropagation();
                dashboardCellPaintSelection = {{ table }};
                if (window.dashboardToggleCellSelection) window.dashboardToggleCellSelection(cell);
            }});
            table.addEventListener('mouseover', (event) => {{
                if (!dashboardCellPaintSelection || dashboardCellPaintSelection.table !== table) return;
                const cell = event.target.closest('tbody td[data-col]');
                if (!cell || !table.contains(cell)) return;
                cell.classList.add('dashboard-cell-selected');
                if (window.dashboardPersistSelectedCells) window.dashboardPersistSelectedCells(table);
            }});
            table.addEventListener('click', (event) => {{
                if (event.target.closest('tbody td[data-col]')) {{
                    event.preventDefault();
                    event.stopPropagation();
                    return;
                }}
            }});
            table.addEventListener('dblclick', (event) => {{
                if (event.target.closest('a, input, select, label, summary')) return;
                const cell = event.target.closest('[data-col]');
                const columnName = cell?.dataset.col || '';
                if (!columnName || !table.contains(cell)) return;
                if (window.dashboardToggleColumnSelection) window.dashboardToggleColumnSelection(table, columnName);
            }});
            if (window.dashboardApplySelectedRows) window.dashboardApplySelectedRows(table);
            if (window.dashboardApplySelectedColumns) window.dashboardApplySelectedColumns(table);
            if (window.dashboardApplySelectedCells) window.dashboardApplySelectedCells(table);
        }});
        document.addEventListener('mouseup', () => {{
            dashboardCellPaintSelection = null;
        }});
        requestAnimationFrame(() => {{
            scheduleDashboardUiRestore();
        }});
    }})();
    </script>
    <script>
    (() => {{
        const treeData = {matrix_tree_json};
        const metricFields = {matrix_metric_fields_json};
        const stateKey = window.teambeadStorageKey('dashboard-matrix-state');
        const pathEl = document.getElementById('dashboardMatrixPath');
        const boardEl = document.getElementById('dashboardMatrixBoard');
        const detailEl = document.getElementById('dashboardMatrixDetail');
        if (!pathEl || !boardEl || !detailEl) return;

        const columns = [
            {{ key: 'platform', label: 'Brand' }},
            {{ key: 'geo', label: 'Geo' }},
            {{ key: 'manager', label: 'Cabinet' }},
            {{ key: 'campaign_name', label: 'Campaign' }},
            {{ key: 'adset_name', label: 'Adset' }},
            {{ key: 'ad_name', label: 'Ad' }},
        ];

        const readState = () => {{
            try {{
                const parsed = JSON.parse(localStorage.getItem(stateKey) || '{{}}');
                return parsed && typeof parsed === 'object' ? parsed : {{}};
            }} catch (_error) {{
                return {{}};
            }}
        }};

        const writeState = (state) => {{
            try {{
                localStorage.setItem(stateKey, JSON.stringify(state || {{}}));
            }} catch (_error) {{}}
        }};

        const savePath = (path) => {{
            const state = readState();
            state.path = Array.isArray(path) ? path : [];
            writeState(state);
        }};

        const loadPath = () => {{
            const state = readState();
            return Array.isArray(state.path) ? state.path.filter(Boolean).slice(0, 6) : [];
        }};

        const findNodeByPath = (path) => {{
            let branch = treeData;
            let current = null;
            for (const id of path) {{
                current = (branch || []).find((item) => item.id === id) || null;
                if (!current) break;
                branch = current.children || [];
            }}
            return current;
        }};

        const getBranchItems = (path, levelIndex) => {{
            if (levelIndex === 0) return treeData;
            let branch = treeData;
            for (let i = 0; i < levelIndex; i += 1) {{
                const currentId = path[i];
                const currentNode = (branch || []).find((item) => item.id === currentId);
                if (!currentNode) return [];
                if (i === levelIndex - 1) return currentNode.children || [];
                branch = currentNode.children || [];
            }}
            return [];
        }};

        const clearNode = (node) => {{
            while (node.firstChild) node.removeChild(node.firstChild);
        }};

        const make = (tag, className, text) => {{
            const node = document.createElement(tag);
            if (className) node.className = className;
            if (typeof text === 'string') node.textContent = text;
            return node;
        }};

        const renderPath = (path) => {{
            clearNode(pathEl);
            if (!path.length) {{
                pathEl.appendChild(make('span', 'dashboard-matrix-path-chip', 'Choose Brand to start drill-down'));
                return;
            }}
            let branch = treeData;
            path.forEach((id) => {{
                const node = (branch || []).find((item) => item.id === id);
                if (!node) return;
                pathEl.appendChild(make('span', 'dashboard-matrix-path-chip', node.label));
                branch = node.children || [];
            }});
        }};

        const renderMetrics = (container, metrics) => {{
            const metricsWrap = make('div', 'dashboard-matrix-metrics');
            metricFields.forEach(([field, label]) => {{
                const metric = make('div', 'dashboard-matrix-metric');
                metric.appendChild(make('div', 'dashboard-matrix-metric-label', label));
                metric.appendChild(make('div', 'dashboard-matrix-metric-value', (metrics && metrics[field]) ? String(metrics[field]) : '—'));
                metricsWrap.appendChild(metric);
            }});
            container.appendChild(metricsWrap);
        }};

        const renderAdsTable = (container, ads) => {{
            const adsWrap = make('div', 'dashboard-matrix-ads');
            adsWrap.appendChild(make('div', 'dashboard-matrix-ads-title', 'Ads inside selected branch'));
            const table = make('table', 'dashboard-matrix-ads-table');
            const thead = document.createElement('thead');
            const headRow = document.createElement('tr');
            ['Ad', 'Account', 'Buyer', 'Offer', 'Spend', 'Clicks', 'Leads', 'Reg'].forEach((label) => {{
                headRow.appendChild(make('th', '', label));
            }});
            thead.appendChild(headRow);
            table.appendChild(thead);
            const tbody = document.createElement('tbody');
            (ads || []).forEach((ad) => {{
                const row = document.createElement('tr');
                [
                    ad.label,
                    ad.account_id,
                    ad.buyer,
                    ad.offer,
                    ad.metrics?.spend || '—',
                    ad.metrics?.clicks || '—',
                    ad.metrics?.leads || '—',
                    ad.metrics?.reg || '—',
                ].forEach((value) => row.appendChild(make('td', '', String(value || '—'))));
                tbody.appendChild(row);
            }});
            table.appendChild(tbody);
            adsWrap.appendChild(table);
            container.appendChild(adsWrap);
        }};

        const renderDetail = (path) => {{
            clearNode(detailEl);
            const currentNode = findNodeByPath(path);
            const head = make('div', 'dashboard-matrix-detail-head');
            if (!currentNode) {{
                head.appendChild(make('div', 'dashboard-matrix-detail-title', 'No selection'));
                head.appendChild(make('div', 'dashboard-matrix-detail-subtitle', 'Choose Brand, then keep drilling to the right.'));
                detailEl.appendChild(head);
                return;
            }}

            head.appendChild(make('div', 'dashboard-matrix-detail-title', currentNode.label));
            const currentColumn = columns.find((item) => item.key === currentNode.column);
            head.appendChild(make('div', 'dashboard-matrix-detail-subtitle', ((currentColumn && currentColumn.label) || 'Level') + ' metrics'));
            detailEl.appendChild(head);
            renderMetrics(detailEl, currentNode.metrics);
            if (Array.isArray(currentNode.ads) && currentNode.ads.length) {{
                renderAdsTable(detailEl, currentNode.ads);
            }}
        }};

        const renderBoard = (path) => {{
            clearNode(boardEl);
            columns.forEach((column, levelIndex) => {{
                const colEl = make('div', 'dashboard-matrix-column');
                colEl.appendChild(make('div', 'dashboard-matrix-column-head', column.label));
                const bodyEl = make('div', 'dashboard-matrix-column-body');
                let items = [];
                if (column.key === 'ad_name') {{
                    const currentNode = findNodeByPath(path);
                    items = currentNode && Array.isArray(currentNode.ads) ? currentNode.ads : [];
                }} else {{
                    items = getBranchItems(path, levelIndex);
                }}
                if (!items.length) {{
                    bodyEl.appendChild(make('div', 'dashboard-matrix-empty', 'No items on this level yet'));
                }} else {{
                    items.forEach((item) => {{
                        const button = make('button', 'dashboard-matrix-item' + ((path[levelIndex] || '') === item.id ? ' is-active' : ''));
                        button.type = 'button';
                        button.dataset.matrixLevel = String(levelIndex);
                        button.dataset.matrixId = item.id;
                        button.appendChild(make('div', 'dashboard-matrix-item-label', item.label));
                        const sub = make('div', 'dashboard-matrix-item-sub');
                        sub.appendChild(make('span', '', String((item.metrics && item.metrics.spend) || '—') + ' spend'));
                        sub.appendChild(make('span', '', String((item.metrics && item.metrics.leads) || '—') + ' leads'));
                        sub.appendChild(make('span', '', String((item.metrics && item.metrics.clicks) || '—') + ' clicks'));
                        button.appendChild(sub);
                        bodyEl.appendChild(button);
                    }});
                }}
                colEl.appendChild(bodyEl);
                boardEl.appendChild(colEl);
            }});
        }};

        const rerender = (path) => {{
            renderPath(path);
            renderBoard(path);
            renderDetail(path);
            savePath(path);
        }};

        boardEl.addEventListener('click', (event) => {{
            const button = event.target.closest('[data-matrix-level][data-matrix-id]');
            if (!button) return;
            const level = Number(button.dataset.matrixLevel || '0');
            const id = button.dataset.matrixId || '';
            const nextPath = loadPath().slice(0, level);
            if (id) nextPath[level] = id;
            rerender(nextPath);
        }});

        rerender(loadPath());
    }})();
    </script>
    """
    return page_shell("Dashboard", content, active_page="dashboard", current_user=user)


_page_routes["render_dashboard_page"] = _render_dashboard_page_v2
_page_routes["show_dashboard"] = _render_dashboard_page_v2
_page_routes["show_hierarchy"] = _render_dashboard_page_v2


def _inject_chatterfy_parser_live_button_refresh(html: str) -> str:
    if not html:
        return html
    if isinstance(html, Response):
        body = html.body.decode("utf-8", errors="ignore")
        patched_body = _inject_chatterfy_parser_live_button_refresh(body)
        if patched_body == body:
            return html
        headers = dict(html.headers)
        media_type = getattr(html, "media_type", None) or headers.get("content-type", "text/html")
        return HTMLResponse(content=patched_body, status_code=html.status_code, headers=headers, media_type=media_type)
    update_fn_old = """            async function refresh() {
"""
    update_fn_new = """            function updateToggleButton(data) {
                const form = document.querySelector('form[action="/chatterfy-parser/toggle"]');
                if (!form) return;
                const button = form.querySelector('button[type="submit"]');
                if (!button) return;
                if (data.button_label) button.textContent = data.button_label;
                if (data.button_style) {
                    data.button_style.split(';').forEach(function(rule) {
                        const parts = rule.split(':');
                        if (parts.length < 2) return;
                        const prop = parts[0].trim();
                        const value = parts.slice(1).join(':').trim();
                        if (!prop) return;
                        button.style.setProperty(prop, value);
                    });
                }
            }
            async function refresh() {
"""
    if update_fn_old in html and "function updateToggleButton(data)" not in html:
        html = html.replace(update_fn_old, update_fn_new, 1)
    html = html.replace(
        'grid-template-columns:minmax(320px, 420px) minmax(360px, 1fr); gap:18px; align-items:stretch;',
        'grid-template-columns:minmax(300px, 360px) minmax(360px, 1fr); gap:14px; align-items:start;',
        1,
    )
    html = html.replace(
        'border:1px solid #dbe5f2; border-radius:24px; padding:18px 20px; background:linear-gradient(180deg, rgba(255,255,255,0.98), rgba(245,249,255,0.96)); box-shadow:0 18px 40px rgba(27,55,102,0.08);',
        'border:1px solid #dbe5f2; border-radius:22px; padding:14px 16px; background:linear-gradient(180deg, rgba(255,255,255,0.98), rgba(245,249,255,0.96)); box-shadow:0 18px 40px rgba(27,55,102,0.08);',
        1,
    )
    html = html.replace(
        'display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:8px; margin-top:14px;',
        'display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:6px; margin-top:10px;',
        1,
    )
    html = html.replace(
        'padding:12px; border-radius:18px; background:#f8fbff; border:1px solid #dbe5f2; min-height:104px; overflow:hidden;',
        'padding:10px 11px; border-radius:16px; background:#f8fbff; border:1px solid #dbe5f2; min-height:88px; overflow:hidden;',
    )
    html = html.replace(
        'margin-top:14px; display:grid; gap:10px;',
        'margin-top:10px; display:grid; gap:8px;',
        1,
    )
    html = html.replace(
        'id="chatterfyParserLastSuccess" style="margin-top:8px; font-size:18px; font-weight:800; line-height:1.2; color:#1f2f4f; word-break:break-word;"',
        'id="chatterfyParserLastSuccess" style="margin-top:8px; font-size:15px; font-weight:800; line-height:1.18; color:#1f2f4f; white-space:pre-line; word-break:normal; overflow-wrap:normal;"',
        1,
    )
    html = html.replace(
        'id="chatterfyParserNextRunText" style="margin-top:8px; font-size:18px; font-weight:800; line-height:1.2; color:#1f2f4f; word-break:break-word;"',
        'id="chatterfyParserNextRunText" style="margin-top:8px; font-size:15px; font-weight:800; line-height:1.18; color:#1f2f4f; white-space:pre-line; word-break:normal; overflow-wrap:normal;"',
        1,
    )
    html = html.replace(
        'id="chatterfyParserLogs" style="max-height:320px; overflow:auto; background:transparent;"',
        'id="chatterfyParserLogs" style="min-height:132px; max-height:320px; overflow:auto; background:transparent;"',
        1,
    )
    html = html.replace(
        '<table style="min-width:1800px;">',
        '<table style="min-width:1560px; width:100%; table-layout:fixed;"><colgroup><col style="width:88px;"><col style="width:84px;"><col style="width:180px;"><col style="width:126px;"><col style="width:150px;"><col style="width:360px;"><col style="width:86px;"><col style="width:78px;"><col style="width:110px;"><col style="width:84px;"><col style="width:102px;"><col style="width:98px;"><col style="width:210px;"><col style="width:210px;"><col style="width:96px;"></colgroup>',
        1,
    )
    html = html.replace('<th>Name</th>', '<th style="width:180px;">Name</th>', 1)
    html = html.replace('<th>Username</th>', '<th style="width:150px;">Username</th>', 1)
    html = html.replace('<th>Tags</th>', '<th style="width:360px;">Tags</th>', 1)
    html = html.replace('<th>Step</th>', '<th style="width:210px;">Step</th>', 1)
    html = html.replace('<th>External ID</th>', '<th style="width:210px;">External ID</th>', 1)
    html = html.replace(
        """        <div style="display:flex; justify-content:space-between; align-items:center; gap:12px; margin-top:14px; flex-wrap:wrap;">
            <div class="user-chip">{len(rows)} / {total_count}</div>
            <div style="display:flex; gap:8px; align-items:center;">
                {f'<a href="{prev_link}" class="ghost-btn small-btn">Prev</a>' if prev_link else '<span class="ghost-btn small-btn" style="opacity:0.45; pointer-events:none;">Prev</span>'}
                <span class="user-chip">Page {page} / {total_pages}</span>
                {f'<a href="{next_link}" class="ghost-btn small-btn">Next</a>' if next_link else '<span class="ghost-btn small-btn" style="opacity:0.45; pointer-events:none;">Next</span>'}
            </div>
        </div>
""",
        """        <div style="display:flex; justify-content:flex-start; align-items:center; gap:12px; margin-top:14px; flex-wrap:wrap;">
            <div class="user-chip">{total_count} chats</div>
        </div>
""",
        1,
    )
    html = html.replace(
        "logsRoot.innerHTML = '<div style=\"padding:14px 12px; color:#ffffff; font-family:SFMono-Regular, Menlo, Monaco, Consolas, \\'Liberation Mono\\', \\'Courier New\\', monospace; font-size:12px;\">[idle] parser console is empty</div>';",
        "logsRoot.style.height = '132px';\n                    logsRoot.innerHTML = '<div style=\"padding:14px 12px; color:#ffffff; font-family:SFMono-Regular, Menlo, Monaco, Consolas, \\'Liberation Mono\\', \\'Courier New\\', monospace; font-size:12px;\">[idle] parser console is empty</div>';",
        1,
    )
    html = html.replace(
        "                logsRoot.innerHTML = logs.map(function(item) {\n",
        "                const targetHeight = Math.min(Math.max(logs.length * 42, 132), 420);\n                logsRoot.style.height = targetHeight + 'px';\n                logsRoot.innerHTML = logs.map(function(item) {\n",
        1,
    )
    html = html.replace(
        "                    renderLogs(data.logs);\n                } catch (error) {\n",
        "                    updateToggleButton(data);\n                    renderLogs(data.logs);\n                } catch (error) {\n",
        1,
    )
    html = html.replace(
        "            window.setInterval(refresh, 5000);\n",
        "            refresh();\n            window.setInterval(refresh, 5000);\n",
        1,
    )
    return html


def _strip_chatterfy_parser_pagination(html: str, total_count: int) -> str:
    if not html:
        return html
    if isinstance(html, Response):
        body = html.body.decode("utf-8", errors="ignore")
        patched_body = _strip_chatterfy_parser_pagination(body, total_count)
        if patched_body == body:
            return html
        headers = dict(html.headers)
        media_type = getattr(html, "media_type", None) or headers.get("content-type", "text/html")
        return HTMLResponse(content=patched_body, status_code=html.status_code, headers=headers, media_type=media_type)
    return re.sub(
        r'<div style="display:flex; justify-content:space-between; align-items:center; gap:12px; margin-top:14px; flex-wrap:wrap;">\s*<div class="user-chip">.*?</div>\s*<div style="display:flex; gap:8px; align-items:center;">.*?</div>\s*</div>',
        f'<div style="display:flex; justify-content:flex-start; align-items:center; gap:12px; margin-top:14px; flex-wrap:wrap;"><div class="user-chip">{int(total_count or 0)} chats</div></div>',
        html,
        count=1,
        flags=re.S,
    )


def _inject_grouped_upload_period_context(html: str) -> str:
    if not html:
        return html
    if isinstance(html, Response):
        body = html.body.decode("utf-8", errors="ignore")
        patched_body = _inject_grouped_upload_period_context(body)
        if patched_body == body:
            return html
        headers = dict(html.headers)
        media_type = getattr(html, "media_type", None) or headers.get("content-type", "text/html")
        return HTMLResponse(content=patched_body, status_code=html.status_code, headers=headers, media_type=media_type)
    target = '<form method="post" action="/upload" enctype="multipart/form-data">'
    if target not in html or 'name="period_view"' in html.split(target, 1)[1][:900]:
        return html
    replacement = """<form method="post" action="/upload" enctype="multipart/form-data">
                        <input type="hidden" name="period_view" value="{escape(period_view)}">
                        <input type="hidden" name="period_label" value="{escape(effective_period_label)}">
                        <input type="hidden" name="brand" value="{escape(brand)}">
                        <input type="hidden" name="manager" value="{escape(manager)}">
                        <input type="hidden" name="geo" value="{escape(geo)}">
                        <input type="hidden" name="ad_name" value="{escape(ad_name)}">
                        <input type="hidden" name="adset_name" value="{escape(adset_name)}">
                        <input type="hidden" name="creative" value="{escape(creative)}">
                        <input type="hidden" name="search" value="{escape(search)}">
                        <input type="hidden" name="sort_by" value="{escape(sort_by)}">
                        <input type="hidden" name="order" value="{escape(order)}">"""
    return html.replace(target, replacement, 1)


def _patched_show_grouped_table(
    request: Request,
    buyer: str = Query(default=""),
    brand: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    ad_name: str = Query(default=""),
    adset_name: str = Query(default=""),
    creative: str = Query(default=""),
    search: str = Query(default=""),
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
    source_name: str = Query(default=""),
    sort_by: str = Query(default="spend"),
    order: str = Query(default="desc"),
):
    html = _original_grouped_page(
        request, buyer, brand, manager, geo, ad_name, adset_name, creative, search, period_view, period_label, source_name, sort_by, order
    )
    return _inject_grouped_upload_period_context(html)


if _original_grouped_page is not None:
    _original_grouped_page.__globals__["render_stats_cards"] = _patched_render_stats_cards


async def _patched_upload_file(
    request: Request,
    buyer: str = Form(...),
    file: UploadFile = File(...),
    period_view: str = Form(default="period"),
    period_label: str = Form(default=""),
    brand: str = Form(default=""),
    manager: str = Form(default=""),
    geo: str = Form(default=""),
    ad_name: str = Form(default=""),
    adset_name: str = Form(default=""),
    creative: str = Form(default=""),
    search: str = Form(default=""),
    sort_by: str = Form(default="spend"),
    order: str = Form(default="desc"),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    require_any_role(user, "superadmin", "admin")
    ensure_fb_table()
    original_name = file.filename or ""
    ext = os.path.splitext(original_name)[1].lower() or ".csv"
    filename = f"temp_{uuid.uuid4()}{ext}"
    clean_buyer = safe_text(buyer).strip()
    effective_period_label = resolve_period_label(period_view, period_label) or get_current_period_label()
    selected_period_data = period_label_to_dates(effective_period_label)

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

        detected_period = detect_fb_upload_period(df) or {}
        source_name = build_fb_source_name(clean_buyer, detected_period or selected_period_data)
        rows_to_insert = parse_uploaded_dataframe(
            df,
            clean_buyer,
            source_name=source_name,
            period_label=effective_period_label,
            period_date_start=safe_text(selected_period_data.get("date_start")),
            period_date_end=safe_text(selected_period_data.get("date_end")),
        )
        if not rows_to_insert:
            redirect_url = (
                f"/grouped?buyer={quote_plus(clean_buyer)}"
                f"&period_view=period"
                f"&period_label={quote_plus(effective_period_label)}"
                f"&brand={quote_plus(safe_text(brand))}"
                f"&manager={quote_plus(safe_text(manager))}"
                f"&geo={quote_plus(safe_text(geo))}"
                f"&ad_name={quote_plus(safe_text(ad_name))}"
                f"&adset_name={quote_plus(safe_text(adset_name))}"
                f"&creative={quote_plus(safe_text(creative))}"
                f"&search={quote_plus(safe_text(search))}"
                f"&sort_by={quote_plus(safe_text(sort_by) or 'spend')}"
                f"&order={quote_plus(safe_text(order) or 'desc')}"
                f"&message=FB+upload+is+empty"
            )
            return RedirectResponse(url=redirect_url, status_code=303)

        db = SessionLocal()
        try:
            db.query(FBRow).filter(FBRow.source_name == source_name).delete()
            db.commit()
            for item in rows_to_insert:
                db.add(item)
            db.commit()
        finally:
            db.close()

        redirect_url = (
            f"/grouped?buyer={quote_plus(clean_buyer)}"
            f"&period_view=period"
            f"&period_label={quote_plus(effective_period_label)}"
            f"&source_name={quote_plus(source_name)}"
            f"&brand={quote_plus(safe_text(brand))}"
            f"&manager={quote_plus(safe_text(manager))}"
            f"&geo={quote_plus(safe_text(geo))}"
            f"&ad_name={quote_plus(safe_text(ad_name))}"
            f"&adset_name={quote_plus(safe_text(adset_name))}"
            f"&creative={quote_plus(safe_text(creative))}"
            f"&search={quote_plus(safe_text(search))}"
            f"&sort_by={quote_plus(safe_text(sort_by) or 'spend')}"
            f"&order={quote_plus(safe_text(order) or 'desc')}"
        )
        return RedirectResponse(url=redirect_url, status_code=303)
    finally:
        if os.path.exists(filename):
            os.remove(filename)


def _patched_chatterfy_parser_page(
    request: Request,
    status: str = Query(default=""),
    search: str = Query(default=""),
    date_filter: str = Query(default=""),
    time_filter: str = Query(default=""),
    telegram_id: str = Query(default=""),
    page: int = Query(default=1),
    bot_url: str = Query(default=""),
    period_view: str = Query(default="period"),
    period_label: str = Query(default=""),
    message: str = Query(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "chatterfyparser")

    rows = get_chatterfy_parser_rows(
        status=status,
        search=search,
        date_filter=date_filter,
        time_filter=time_filter,
        telegram_id=telegram_id,
        period_label=period_label,
    )
    html = chatterfy_parser_page_html(
        user,
        rows,
        form_data={
            "bot_url": bot_url,
            "period_view": period_view,
            "period_label": period_label,
        },
        status=status,
        search=search,
        date_filter=date_filter,
        time_filter=time_filter,
        telegram_id=telegram_id,
        period_view=period_view,
        period_label=period_label,
        page=1,
        total_count=len(rows),
        per_page=max(1, len(rows)),
        success_text=message,
    )
    html = _strip_chatterfy_parser_pagination(html, len(rows))
    return _inject_chatterfy_parser_live_button_refresh(html)


def _patched_toggle_chatterfy_parser(
    request: Request,
    bot_url: str = Form(...),
    period_view: str = Form(default="period"),
    period_label: str = Form(default=""),
):
    user = get_current_user(request)
    if not user:
        return auth_redirect_response()
    enforce_page_access(user, "chatterfyparser")

    existing_config = get_chatterfy_parser_config()
    form_data = build_chatterfy_parser_config_payload(
        bot_url=bot_url,
        period_view=period_view,
        period_label=period_label,
        auto_sync_enabled=bool(existing_config.get("auto_sync_enabled")),
        existing_config=existing_config,
    )
    bot_id = extract_chatterfy_bot_id(bot_url)
    if not bot_id:
        return HTMLResponse(
            chatterfy_parser_page_html(
                user,
                [],
                form_data=form_data,
                period_view=period_view,
                period_label=form_data.get("period_label", ""),
                error_text="Invalid Chatterfy Users URL. Expected format: /bots/<bot_id>/users",
            ),
            status_code=400,
        )

    try:
        existing_state = safe_text(existing_config.get("sync_state"))
        is_running_now = CHATTERFY_SYNC_LOCK.locked() or existing_state == "running"
        is_active = bool(existing_config.get("auto_sync_enabled")) or existing_state in {"running", "pause_pending", "idle"}
        if is_active:
            updated = dict(form_data)
            if is_running_now:
                updated["auto_sync_enabled"] = True
                updated["sync_state"] = "pause_pending"
                save_chatterfy_parser_config(updated)
                chatterfy_parser_append_log(
                    "Нажата Пауза. Делаю последнюю выгрузку и после этого остановлю автосинк.",
                    kind="info",
                    config=updated,
                )
                success_message = "Stop accepted. The parser will finish the current sync and stop."
            else:
                updated["auto_sync_enabled"] = False
                updated["sync_state"] = "stopped"
                save_chatterfy_parser_config(updated)
                chatterfy_parser_append_log(
                    "Автосинк остановлен. Больше почасовых выгрузок не будет, пока вы снова не нажмёте Старт.",
                    kind="info",
                    config=updated,
                )
                success_message = "Auto sync stopped."
        else:
            updated = dict(form_data)
            updated["auto_sync_enabled"] = True
            updated["sync_state"] = "idle"
            save_chatterfy_parser_config(updated)
            chatterfy_parser_append_log(
                f"Нажат Start. Включаю штатный режим для периода {updated['period_label']} и запускаю первую выгрузку.",
                kind="success",
                config=updated,
            )
            if not CHATTERFY_SYNC_LOCK.locked():
                run_chatterfy_parser_sync_async(initiated_by="start")
            success_message = "Auto sync is active. The first run has started, then it will continue every hour."
        return RedirectResponse(
            url=(
                f"/chatterfy-parser?bot_url={quote_plus(build_chatterfy_users_url(bot_id))}"
                f"&period_view={quote_plus(safe_text(form_data['period_view']))}"
                f"&period_label={quote_plus(safe_text(form_data['period_label']))}"
                f"&message={quote_plus(success_message)}"
            ),
            status_code=303,
        )
    except Exception as exc:
        return HTMLResponse(
            chatterfy_parser_page_html(
                user,
                [],
                form_data=form_data,
                period_view=period_view,
                period_label=form_data.get("period_label", ""),
                error_text=f"Could not import Chatterfy data: {exc}",
            ),
            status_code=400,
        )


_page_routes["chatterfy_parser_page"] = _patched_chatterfy_parser_page
_page_routes["show_grouped_table"] = _patched_show_grouped_table
_domain_actions["toggle_chatterfy_parser"] = _patched_toggle_chatterfy_parser
_domain_actions["upload_file"] = _patched_upload_file

# =========================================
# BLOCK 7.5 — USERS
# =========================================
@app.get("/users", response_class=HTMLResponse)
def users_page(request: Request, edit: str = Query(default=""), message: str = Query(default="")):
    return _page_routes["users_page"](request, edit, message)


@app.get("/tasks", response_class=HTMLResponse)
def tasks_page(
    request: Request,
    status: str = Query(default=""),
    assignee: str = Query(default=""),
    search: str = Query(default=""),
    message: str = Query(default=""),
):
    return _page_routes["tasks_page"](request, status, assignee, search, message)


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
    return _domain_actions["save_task"](request, assigned_to_username, title, description, due_year, due_month, due_day, due_hour, due_minute, notes, status_filter, assignee_filter, search_filter)


@app.post("/tasks/upload")
async def upload_tasks_file(
    request: Request,
    assigned_to_username: str = Form(...),
    file: UploadFile = File(...),
):
    return await _domain_actions["upload_tasks_file"](request, assigned_to_username, file)


@app.post("/tasks/delete")
def delete_task(
    request: Request,
    task_id: str = Form(...),
    status_filter: str = Form(default=""),
    assignee_filter: str = Form(default=""),
    search_filter: str = Form(default=""),
):
    return _domain_actions["delete_task"](request, task_id, status_filter, assignee_filter, search_filter)


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
    return _domain_actions["respond_task"](request, task_id, status, response_text, status_filter, assignee_filter, search_filter)


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
    return _domain_actions["save_user"](request, edit_user_id, display_name, username, password, role, buyer_name, is_active)


@app.post("/users/delete")
def delete_user(request: Request, user_id: str = Form(...)):
    return _domain_actions["delete_user"](request, user_id)


# =========================================
# BLOCK 8 — UPLOAD
# =========================================
@app.post("/upload")
async def upload_file(
    request: Request,
    buyer: str = Form(...),
    file: UploadFile = File(...),
    period_view: str = Form(default="period"),
    period_label: str = Form(default=""),
    brand: str = Form(default=""),
    manager: str = Form(default=""),
    geo: str = Form(default=""),
    ad_name: str = Form(default=""),
    adset_name: str = Form(default=""),
    creative: str = Form(default=""),
    search: str = Form(default=""),
    sort_by: str = Form(default="spend"),
    order: str = Form(default="desc"),
):
    return await _domain_actions["upload_file"](
        request, buyer, file, period_view, period_label, brand, manager, geo, ad_name, adset_name, creative, search, sort_by, order
    )


@app.post("/upload/partner")
async def upload_partner_file(request: Request, file: UploadFile = File(...), cabinet_name: str = Form(default="")):
    return await _domain_actions["upload_partner_file"](request, file, cabinet_name)


# =========================================
# BLOCK 9 — EXPORT
# =========================================
@app.get("/export/grouped")
def export_grouped_csv(
    request: Request,
    buyer: str = Query(default=""),
    brand: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    ad_name: str = Query(default=""),
    adset_name: str = Query(default=""),
    creative: str = Query(default=""),
    search: str = Query(default=""),
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
    source_name: str = Query(default=""),
    sort_by: str = Query(default="spend"),
    order: str = Query(default="desc"),
):
    return _domain_actions["export_grouped_csv"](
        request, buyer, brand, manager, geo, ad_name, adset_name, creative, search, period_view, period_label, source_name, sort_by, order
    )


@app.get("/export/hierarchy")
def export_hierarchy_csv(
    request: Request,
    buyer: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
    period_view: str = Query(default="period"),
    period_label: str = Query(default=""),
):
    return _domain_actions["export_hierarchy_csv"](request, buyer, manager, geo, offer, search, period_view, period_label)


# =========================================
# BLOCK 10 — GROUPED PAGE
# =========================================
@app.get("/grouped", response_class=HTMLResponse)
def show_grouped_table(
    request: Request,
    buyer: str = Query(default=""),
    brand: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    ad_name: str = Query(default=""),
    adset_name: str = Query(default=""),
    creative: str = Query(default=""),
    search: str = Query(default=""),
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
    source_name: str = Query(default=""),
    sort_by: str = Query(default="spend"),
    order: str = Query(default="desc"),
):
    return _page_routes["show_grouped_table"](
        request, buyer, brand, manager, geo, ad_name, adset_name, creative, search, period_view, period_label, source_name, sort_by, order
    )


@app.post("/grouped/delete-upload")
def delete_grouped_upload(
    request: Request,
    source_name: str = Form(default=""),
    buyer: str = Form(default=""),
    brand: str = Form(default=""),
    manager: str = Form(default=""),
    geo: str = Form(default=""),
    ad_name: str = Form(default=""),
    adset_name: str = Form(default=""),
    creative: str = Form(default=""),
    search: str = Form(default=""),
    period_view: str = Form(default="current"),
    period_label: str = Form(default=""),
    sort_by: str = Form(default="spend"),
    order: str = Form(default="desc"),
):
    return _domain_actions["delete_grouped_upload"](
        request, source_name, buyer, brand, manager, geo, ad_name, adset_name, creative, search, period_view, period_label, sort_by, order
    )


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
    return _page_routes["render_dashboard_page"](request, buyer, manager, geo, offer, search, period_view, period_label)


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
    return _page_routes["show_hierarchy"](request, buyer, manager, geo, offer, search, period_view, period_label)


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
    return _page_routes["show_dashboard"](request, buyer, manager, geo, offer, search, period_view, period_label)


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
    return _page_routes["finance_page"](
        request,
        message,
        period_view,
        period_label,
        date_from,
        date_to,
        year,
        edit_wallet,
        edit_expense,
        edit_income,
        edit_transfer,
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
    return _domain_actions["save_finance_wallet"](request, edit_id, category, description, owner_name, wallet, amount, date_from, date_to, year)


@app.post("/finance/upload")
async def upload_finance_file(request: Request, file: UploadFile = File(...)):
    return await _domain_actions["upload_finance_file"](request, file)


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
    return _domain_actions["save_finance_expense"](request, edit_id, expense_date, category, wallet_name, amount, from_wallet, paid_by, comment, date_from, date_to, year)


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
    return _domain_actions["save_finance_income"](request, edit_id, income_date, category, wallet_name, amount, from_wallet, comment, date_from, date_to, year)


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
    return _domain_actions["save_finance_transfer"](request, edit_id, transfer_date, category, amount, from_wallet, to_wallet, comment, date_from, date_to, year)


@app.post("/finance/wallets/delete")
def delete_finance_wallet(request: Request, wallet_id: str = Form(...), date_from: str = Form(default=""), date_to: str = Form(default=""), year: str = Form(default="")):
    return _domain_actions["delete_finance_wallet"](request, wallet_id, date_from, date_to, year)


@app.post("/finance/expenses/delete")
def delete_finance_expense(request: Request, expense_id: str = Form(...), date_from: str = Form(default=""), date_to: str = Form(default=""), year: str = Form(default="")):
    return _domain_actions["delete_finance_expense"](request, expense_id, date_from, date_to, year)


@app.post("/finance/income/delete")
def delete_finance_income(request: Request, income_id: str = Form(...), date_from: str = Form(default=""), date_to: str = Form(default=""), year: str = Form(default="")):
    return _domain_actions["delete_finance_income"](request, income_id, date_from, date_to, year)


@app.post("/finance/transfers/delete")
def delete_finance_transfer(request: Request, transfer_id: str = Form(...), date_from: str = Form(default=""), date_to: str = Form(default=""), year: str = Form(default="")):
    return _domain_actions["delete_finance_transfer"](request, transfer_id, date_from, date_to, year)


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
    return _page_routes["caps_page"](
        request,
        search,
        period_view,
        period_label,
        sort_by,
        order,
        buyer,
        code,
        edit,
        message,
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
    return _domain_actions["save_cap"](request, edit_id, advertiser, owner_name, period_view, period_label, buyer, code_filter, search, sort_by, order, cabinet_name, flow, code, geo, rate, baseline, cap_value, current_ftd, promo_code, kpi, link, comments, agent)

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
    return _domain_actions["delete_cap"](request, cap_id, period_view, period_label, buyer, code, search, sort_by, order)

@app.get("/api/partner/current-period")
def api_partner_current_period(request: Request):
    return _domain_actions["api_partner_current_period"](request)


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
    return await _domain_actions["api_partner_import"](request, file, source_name, cabinet_name, date_start, date_end, period_mode, api_key)


@app.get("/cabinets", response_class=HTMLResponse)
def cabinets_page(
    request: Request,
    search: str = Query(default=""),
    status: str = Query(default=""),
    edit: str = Query(default=""),
    message: str = Query(default=""),
):
    return _page_routes["cabinets_page"](request, search, status, edit, message)


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
    return _domain_actions["save_cabinet"](request, edit_id, advertiser, platform, name, geo_list, brands, team_name, manager_name, manager_contact, chat_name, wallet, comments, status, search)


@app.post("/cabinets/delete")
def delete_cabinet(
    request: Request,
    cabinet_id: str = Form(...),
    search: str = Form(default=""),
):
    return _domain_actions["delete_cabinet"](request, cabinet_id, search)


@app.get("/partner-report", response_class=HTMLResponse)
def partner_report_page(
    request: Request,
    source_name: str = Query(default=""),
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
    cabinet_name: str = Query(default=""),
    brand: str = Query(default=""),
    geo: str = Query(default=""),
    search: str = Query(default=""),
    sort_by: str = Query(default="id"),
    order: str = Query(default="desc"),
    message: str = Query(default=""),
):
    return _page_routes["partner_report_page"](
        request,
        source_name,
        period_view,
        period_label,
        cabinet_name,
        brand,
        geo,
        search,
        sort_by,
        order,
        message,
    )


@app.get("/partner-report/export")
def export_partner_report_csv(
    request: Request,
    source_name: str = Query(default=""),
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
    cabinet_name: str = Query(default=""),
    brand: str = Query(default=""),
    geo: str = Query(default=""),
    search: str = Query(default=""),
    sort_by: str = Query(default="id"),
    order: str = Query(default="desc"),
):
    return _domain_actions["export_partner_report_csv"](request, source_name, period_view, period_label, cabinet_name, brand, geo, search, sort_by, order)


@app.post("/partner-report/upload")
async def upload_partner_report_file(
    request: Request,
    partner_platform: str = Form(default="1xbet"),
    period_view: str = Form(default="current"),
    period_label: str = Form(default=""),
    cabinet_name: str = Form(default=""),
    file: UploadFile = File(...),
):
    return await _domain_actions["upload_partner_report_file"](request, partner_platform, period_view, period_label, cabinet_name, file)


@app.post("/partner-report/delete-upload")
def delete_partner_upload(
    request: Request,
    source_name: str = Form(default=""),
    period_view: str = Form(default="current"),
    period_label: str = Form(default=""),
    cabinet_name: str = Form(default=""),
    brand: str = Form(default=""),
    geo: str = Form(default=""),
    search: str = Form(default=""),
    sort_by: str = Form(default="id"),
    order: str = Form(default="desc"),
):
    return _domain_actions["delete_partner_upload"](request, source_name, period_view, period_label, cabinet_name, brand, geo, search, sort_by, order)


@app.post("/partner-report/flags/save")
def save_partner_row_flags(
    request: Request,
    partner_row_id: str = Form(...),
    return_to: str = Form(default="/partner-report"),
    manual_hold: str = Form(default="0"),
    manual_blocked: str = Form(default="0"),
):
    return _domain_actions["save_partner_row_flags"](request, partner_row_id, return_to, manual_hold, manual_blocked)


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
    return _page_routes["chatterfy_page"](
        request,
        status,
        search,
        period_view,
        period_label,
        date_filter,
        time_filter,
        telegram_id,
        pp_player_id,
        sort_by,
        order,
        page,
        message,
    )


@app.post("/chatterfy/upload")
async def upload_chatterfy_file(request: Request, file: UploadFile = File(...)):
    return await _domain_actions["upload_chatterfy_file"](request, file)


@app.post("/chatterfy/upload-ids")
async def upload_chatterfy_ids_file(request: Request, file: UploadFile = File(...)):
    return await _domain_actions["upload_chatterfy_ids_file"](request, file)


@app.get("/hold-wager", response_class=HTMLResponse)
def hold_wager_page(
    request: Request,
    period_view: str = Query(default="current"),
    period_label: str = Query(default=""),
    cabinet_name: str = Query(default=""),
    search: str = Query(default=""),
):
    return _page_routes["hold_wager_page"](request, period_view, period_label, cabinet_name, search)
