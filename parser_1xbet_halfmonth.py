import os
import calendar
import sys
import json
import argparse
import re
from pathlib import Path
from datetime import date, datetime

MISSING_DEPENDENCIES = []

try:
    import requests
except ModuleNotFoundError:
    requests = None
    MISSING_DEPENDENCIES.append("requests")

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    load_dotenv = None
    MISSING_DEPENDENCIES.append("python-dotenv")

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except ModuleNotFoundError:
    sync_playwright = None
    PlaywrightTimeoutError = TimeoutError
    MISSING_DEPENDENCIES.append("playwright")

if load_dotenv:
    load_dotenv()

PARTNER_LOGIN = os.getenv("PARTNER_LOGIN", "").strip()
PARTNER_PASSWORD = os.getenv("PARTNER_PASSWORD", "").strip()
CRM_IMPORT_URL = os.getenv("CRM_IMPORT_URL", "https://crm.teambead.work/api/partner/import").strip()
CRM_IMPORT_API_KEY = os.getenv("TEAMBEAD_PARTNER_IMPORT_KEY", "").strip()
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
SESSION_STATE_PATH = Path(os.getenv("SESSION_STATE_PATH", "playwright_state_1xbet.json")).resolve()
STATUS_FILE_PATH = Path(os.getenv("PARSER_STATUS_FILE", "parser_1xbet_status.json")).resolve()
PARSER_MODE = os.getenv("PARSER_MODE", "run").strip().lower() or "run"
ACCOUNT_IDS_ENV = os.getenv("ONEXBET_ACCOUNT_IDS", "").strip()
ACCOUNTS_FILE_PATH = Path(os.getenv("ONEXBET_ACCOUNTS_FILE", "uploaded_data/onexbet_runtime/accounts.json")).resolve()

LOGIN_URL = "https://1xpartners.com/ru/sign-in"
PLAYERS_REPORT_URL = "https://1xpartners.com/ru/partner/reports/players"

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)


def slugify_account_id(value):
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", (value or "").strip().lower()).strip("_")
    return slug or f"account_{abs(hash(value or 'default')) % 100000}"


def default_accounts():
    items = []
    if PARTNER_LOGIN and PARTNER_PASSWORD:
        items.append({
            "id": slugify_account_id(PARTNER_LOGIN),
            "label": PARTNER_LOGIN,
            "login": PARTNER_LOGIN,
            "password": PARTNER_PASSWORD,
        })
    return items


def load_accounts():
    accounts = []
    if ACCOUNTS_FILE_PATH.exists():
        try:
            raw = json.loads(ACCOUNTS_FILE_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                raw = raw.get("accounts", [])
            if isinstance(raw, list):
                for item in raw:
                    if not isinstance(item, dict):
                        continue
                    login = str(item.get("login") or item.get("username") or "").strip()
                    password = str(item.get("password") or "").strip()
                    if not login or not password:
                        continue
                    account_id = str(item.get("id") or slugify_account_id(login)).strip()
                    accounts.append({
                        "id": account_id,
                        "label": str(item.get("label") or login).strip(),
                        "login": login,
                        "password": password,
                    })
        except Exception as exc:
            raise RuntimeError(f"Не удалось прочитать accounts.json: {exc}")

    if not accounts:
        accounts = default_accounts()

    return accounts


def resolve_selected_accounts(selected_ids_text=""):
    selected_ids = [item.strip() for item in (selected_ids_text or ACCOUNT_IDS_ENV).split(",") if item.strip()]
    all_accounts = load_accounts()
    if not all_accounts:
        raise RuntimeError("Не найдено ни одного кабинета 1xBet для запуска")
    if not selected_ids:
        return all_accounts
    selected_set = set(selected_ids)
    selected_accounts = [item for item in all_accounts if item["id"] in selected_set]
    if not selected_accounts:
        raise RuntimeError("Выбранные кабинеты не найдены в accounts.json")
    return selected_accounts


def session_path_for_account(account):
    return SESSION_STATE_PATH.parent / f"{account['id']}_state.json"


def log(*args):
    message = " ".join(str(arg) for arg in args)
    print("[1XBET_PARSER]", message, flush=True)
    set_status(message=message)


def set_status(status=None, message=None, **extra):
    payload = {}
    if STATUS_FILE_PATH.exists():
        try:
            payload = json.loads(STATUS_FILE_PATH.read_text(encoding="utf-8"))
        except Exception:
            payload = {}

    payload.setdefault("mode", PARSER_MODE)
    payload["updated_at"] = datetime.now().isoformat(timespec="seconds")
    if status is not None:
        payload["status"] = status
    if message is not None:
        payload["message"] = str(message)
    payload.update(extra)

    STATUS_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_FILE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def update_account_status(account, status, message="", **extra):
    payload = {}
    if STATUS_FILE_PATH.exists():
        try:
            payload = json.loads(STATUS_FILE_PATH.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
    account_statuses = payload.get("account_statuses") or {}
    account_statuses[account["id"]] = {
        "status": status,
        "message": message,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        **extra,
    }
    set_status(account_statuses=account_statuses)


def is_authenticated(page):
    current_url = page.url or ""
    if "/sign-in" in current_url:
        return False

    account_markers = [
        'text=Выйти',
        'text=Logout',
        'text=Reports',
        'text=Отчеты',
        'text=Профиль',
        '[href*="/partner/"]',
        '[href*="/profile"]',
        '[href*="/logout"]',
    ]
    for sel in account_markers:
        try:
            locator = page.locator(sel)
            if locator.count() and locator.first.is_visible():
                return True
        except Exception:
            pass

    return "/partner/" in current_url


def page_has_captcha(page):
    try:
        content = page.content().lower()
    except Exception:
        return False
    return any(marker in content for marker in ("recaptcha", "g-recaptcha", "captcha"))


def save_session_state(context, session_path):
    try:
        session_path.parent.mkdir(parents=True, exist_ok=True)
        context.storage_state(path=str(session_path))
        log("Сессия сохранена:", session_path)
        set_status(session_saved=True, session_state_path=str(session_path))
    except Exception as exc:
        log("Не удалось сохранить сессию:", exc)


def ensure_runtime_ready():
    if MISSING_DEPENDENCIES:
        missing_text = ", ".join(MISSING_DEPENDENCIES)
        raise RuntimeError(
            "Не хватает Python-зависимостей для парсера: "
            f"{missing_text}. Установи `pip install -r requirements.txt` "
            "и затем `python3 -m playwright install chromium`."
        )

    if not load_accounts():
        raise RuntimeError("Не найдено ни одного кабинета 1xBet для запуска")


def get_half_month_period(today: date | None = None):
    if today is None:
        today = date.today()

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


def fill_first_visible(page, selectors, value, label):
    for sel in selectors:
        try:
            locator = page.locator(sel).first
            locator.wait_for(state="visible", timeout=15000)
            locator.fill(value)
            log(f"{label}: заполнено через {sel}")
            return True
        except Exception:
            pass
    return False


def click_first(page, selectors, label, timeout=15000):
    for sel in selectors:
        try:
            locator = page.locator(sel).first
            locator.wait_for(state="visible", timeout=timeout)
            locator.click(timeout=timeout)
            log(f"{label}: клик через {sel}")
            return True
        except Exception:
            pass
    return False


def login_to_partner(page, account):
    update_account_status(account, "authenticating", "Открываю логин и выполняю вход")
    log("Открываю страницу логина")
    page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(4000)

    if is_authenticated(page):
        log("Уже есть активная сессия, повторный логин не нужен")
        return

    login_ok = fill_first_visible(
        page,
        [
            'input[name="login"]',
            'input[name="username"]',
            'input[autocomplete="username"]',
            'input[type="email"]',
            'input[type="text"]',
        ],
        account["login"],
        "login",
    )
    if not login_ok:
        raise RuntimeError("Не удалось найти поле логина")

    password_ok = fill_first_visible(
        page,
        [
            'input[name="password"]',
            'input[autocomplete="current-password"]',
            'input[type="password"]',
        ],
        account["password"],
        "password",
    )
    if not password_ok:
        raise RuntimeError("Не удалось найти поле пароля")

    clicked = click_first(
        page,
        [
            'button[type="submit"]',
            'button:has-text("Войти")',
            'button:has-text("Login")',
            'button:has-text("Sign in")',
            'text=Войти',
        ],
        "login_button",
    )
    if not clicked:
        raise RuntimeError("Не удалось найти кнопку входа")

    page.wait_for_timeout(8000)

    if is_authenticated(page):
        log("Логин выполнен")
        update_account_status(account, "authorized", "Вход подтвержден")
        return

    if not HEADLESS and page_has_captcha(page):
        log("Обнаружена captcha, жду ручной вход в открытом браузере")
        set_status(status="waiting_for_user", needs_user_action=True, message="Нужно пройти captcha и завершить вход в открытом окне браузера")
        update_account_status(account, "waiting_for_user", "Нужно пройти captcha")
        deadline = datetime.now().timestamp() + 180
        while datetime.now().timestamp() < deadline:
            page.wait_for_timeout(3000)
            if is_authenticated(page):
                log("Логин выполнен после ручного подтверждения")
                set_status(needs_user_action=False)
                update_account_status(account, "authorized", "Вход подтвержден после captcha")
                return

    if page_has_captcha(page):
        set_status(status="waiting_for_user", needs_user_action=True)
        update_account_status(account, "waiting_for_user", "Сайт требует captcha")
        raise RuntimeError(
            "1xPartners требует captcha при входе. "
            "Открой режим авторизации в CRM через кнопку Open 1xBet Login, выполни вход вручную один раз и затем повтори запуск."
        )

    update_account_status(account, "error", "Логин не завершился успешно")
    raise RuntimeError("Не удалось войти в 1xPartners: после отправки формы кабинет не открылся")


def open_players_report(page):
    log("Открываю отчет по игрокам")
    page.goto(PLAYERS_REPORT_URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(6000)
    if not is_authenticated(page):
        raise RuntimeError("1xPartners не открыл отчет по игрокам: сессия неактивна или доступ не предоставлен")


def open_date_picker(page):
    log("Открываю выбор периода")

    # 1. Если список уже открыт — сразу выбираем "Произвольный период"
    direct_selectors = [
        'text=Произвольный период',
        'div:has-text("Произвольный период")',
        'li:has-text("Произвольный период")',
    ]
    for sel in direct_selectors:
        try:
            loc = page.locator(sel)
            for i in range(loc.count()):
                item = loc.nth(i)
                if item.is_visible():
                    item.click(timeout=5000)
                    page.wait_for_timeout(1200)
                    log(f"period_mode: выбран через {sel}")
                    return
        except Exception:
            pass

    # 2. Сначала открываем dropdown периода
    opener_selectors = [
        'text=Произвольн',
        'div:has-text("Произвольн")',
        'span:has-text("Произвольн")',
        'label:has-text("Период")',
        'div:has-text("Период")',
        '[class*="select"]',
        '[class*="dropdown"]',
    ]
    for sel in opener_selectors:
        try:
            loc = page.locator(sel)
            for i in range(loc.count()):
                item = loc.nth(i)
                if item.is_visible():
                    item.click(timeout=5000)
                    page.wait_for_timeout(1000)

                    option = page.locator('text=Произвольный период').first
                    if option.is_visible():
                        option.click(timeout=5000)
                        page.wait_for_timeout(1200)
                        log(f"period_mode: открыт через {sel} и выбран 'Произвольный период'")
                        return
        except Exception:
            pass

    raise RuntimeError("Не удалось выбрать 'Произвольный период'")


def set_date_range_via_inputs(page, period):
    start = period["date_start"]
    end = period["date_end"]

    input_selectors = [
        'input[placeholder*="Начало"]',
        'input[placeholder*="Конец"]',
        'input[placeholder*="2026"]',
        'input[readonly]',
        'input[type="text"]',
    ]

    visible_inputs = []

    for sel in input_selectors:
        try:
            loc = page.locator(sel)
            for i in range(loc.count()):
                item = loc.nth(i)
                if item.is_visible():
                    visible_inputs.append(item)
            if len(visible_inputs) >= 2:
                break
        except Exception:
            pass

    # убираем дубликаты по порядку
    unique_inputs = []
    seen = set()
    for item in visible_inputs:
        try:
            box = item.bounding_box()
            key = (round(box["x"]), round(box["y"])) if box else id(item)
            if key not in seen:
                seen.add(key)
                unique_inputs.append(item)
        except Exception:
            unique_inputs.append(item)

    if len(unique_inputs) < 2:
        raise RuntimeError("Не найдены поля Начало / Конец")

    start_input = unique_inputs[0]
    end_input = unique_inputs[1]

    try:
        start_input.click(timeout=5000)
        start_input.fill("")
        start_input.type(start, delay=40)
        start_input.press("Enter")
        page.wait_for_timeout(600)

        end_input.click(timeout=5000)
        end_input.fill("")
        end_input.type(end, delay=40)
        end_input.press("Enter")
        page.wait_for_timeout(1200)

        log(f"date_range: заполнен через input {start} - {end}")
        return True
    except Exception as e:
        raise RuntimeError(f"Не удалось ввести даты в поля: {e}")


def get_visible_calendar_root(page):
    candidates = [
        '.daterangepicker',
        '.drp-calendar',
        '.datepicker',
        '.calendar',
        '[class*="calendar"]',
        '[class*="date"]',
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel)
            for i in range(loc.count()):
                item = loc.nth(i)
                if item.is_visible():
                    return item
        except Exception:
            pass
    return page


def click_calendar_day(container, day_text, used_indexes=None):
    if used_indexes is None:
        used_indexes = set()

    day_text = str(int(day_text))
    selectors = [
        f'td:has-text("{day_text}")',
        f'button:has-text("{day_text}")',
        f'div:has-text("{day_text}")',
        f'span:has-text("{day_text}")',
    ]

    for sel in selectors:
        try:
            loc = container.locator(sel)
            count = loc.count()
            for i in range(count):
                key = (sel, i)
                if key in used_indexes:
                    continue
                item = loc.nth(i)
                try:
                    if not item.is_visible():
                        continue
                    text = item.inner_text().strip()
                    if text != day_text:
                        continue
                    cls = (item.get_attribute("class") or "").lower()
                    if "off" in cls or "disabled" in cls:
                        continue
                    item.click(timeout=5000)
                    used_indexes.add(key)
                    return True
                except Exception:
                    continue
        except Exception:
            pass
    return False


def set_date_range_via_calendar(page, period):
    start = period["date_start"]
    end = period["date_end"]

    start_day = start.split("-")[2]
    end_day = end.split("-")[2]

    calendar_root = get_visible_calendar_root(page)
    used_indexes = set()

    start_clicked = click_calendar_day(calendar_root, start_day, used_indexes=used_indexes)
    if not start_clicked:
        raise RuntimeError(f"Не удалось выбрать дату начала: {start_day}")

    page.wait_for_timeout(700)

    end_clicked = click_calendar_day(calendar_root, end_day, used_indexes=used_indexes)
    if not end_clicked:
        page.wait_for_timeout(700)
        calendar_root = get_visible_calendar_root(page)
        end_clicked = click_calendar_day(calendar_root, end_day, used_indexes=set())

    if not end_clicked:
        raise RuntimeError(f"Не удалось выбрать дату конца: {end_day}")

    page.wait_for_timeout(1500)
    log("date_range: диапазон выбран через календарь")
    return True


def set_date_range(page, period):
    log(f"date_range: ставлю {period['date_start']} - {period['date_end']}")

    # сначала выбираем режим "Произвольный период"
    open_date_picker(page)

    # сначала пытаемся ввести в поля Начало / Конец
    try:
        if set_date_range_via_inputs(page, period):
            return
    except Exception as e:
        log("date_range input fallback:", e)

    # если не получилось — пробуем календарь
    try:
        if set_date_range_via_calendar(page, period):
            return
    except Exception as e:
        log("date_range calendar fallback:", e)

    raise RuntimeError("Не удалось заполнить даты периода")


def check_new_players(page):
    try:
        label = page.locator('label:has-text("Только новые игроки")')
        if label.count() > 0:
            checkbox = label.locator('input[type="checkbox"]').first
            checkbox.wait_for(state="attached", timeout=5000)
            if not checkbox.is_checked():
                checkbox.check()
            log("checkbox: включена галочка 'Только новые игроки'")
            return
    except Exception:
        pass

    try:
        page.locator('text=Только новые игроки').first.click(timeout=5000)
        log("checkbox: клик по тексту 'Только новые игроки'")
        return
    except Exception:
        pass

    try:
        checkboxes = page.locator('input[type="checkbox"]')
        if checkboxes.count() > 0:
            cb = checkboxes.first
            if not cb.is_checked():
                cb.check()
            log("checkbox: включена первая доступная checkbox")
            return
    except Exception:
        pass

    raise RuntimeError("Не удалось включить галочку 'Только новые игроки'")


def click_generate_report(page):
    generated = click_first(
        page,
        [
            'button:has-text("СГЕНЕРИРОВАТЬ ОТЧЕТ")',
            'button:has-text("Сгенерировать отчет")',
            'text=СГЕНЕРИРОВАТЬ ОТЧЕТ',
        ],
        "generate_report",
        timeout=20000,
    )
    if not generated:
        raise RuntimeError("Не удалось нажать 'СГЕНЕРИРОВАТЬ ОТЧЕТ'")

    page.wait_for_timeout(8000)


def download_csv(page):
    file_path = None

    try:
        with page.expect_download(timeout=90000) as download_info:
            try:
                page.locator('text=CSV').first.click(timeout=10000)
            except Exception:
                opened = click_first(
                    page,
                    [
                        'button:has-text("ЭКСПОРТ")',
                        'text=ЭКСПОРТ',
                    ],
                    "export_menu",
                    timeout=15000,
                )
                if not opened:
                    raise RuntimeError("Не удалось открыть меню 'ЭКСПОРТ'")

                page.wait_for_timeout(2000)

                clicked_csv = click_first(
                    page,
                    [
                        'text=CSV',
                        'a:has-text("CSV")',
                        'button:has-text("CSV")',
                    ],
                    "csv_button",
                    timeout=15000,
                )
                if not clicked_csv:
                    raise RuntimeError("Не удалось нажать 'CSV'")

        download = download_info.value
        filename = download.suggested_filename or f"1xbet_players_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        file_path = DOWNLOAD_DIR / filename
        download.save_as(str(file_path))
        log("Файл скачан:", file_path)

    except PlaywrightTimeoutError as e:
        raise RuntimeError(f"Таймаут при скачивании CSV: {e}")

    if not file_path or not file_path.exists():
        raise RuntimeError("CSV не был скачан")

    return file_path


def export_players_report(account):
    ensure_runtime_ready()

    period = get_half_month_period()
    session_path = session_path_for_account(account)
    log(f"Кабинет {account['label']}: период {period}")
    set_status(current_account=account["id"], current_account_label=account["label"])
    update_account_status(account, "running", f"Готовлю выгрузку за {period['period_label']}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-zygote",
                "--single-process",
            ],
        )

        context_kwargs = {
            "accept_downloads": True,
            "locale": "ru-RU",
            "ignore_https_errors": True,
        }
        if session_path.exists():
            context_kwargs["storage_state"] = str(session_path)
            log("Использую сохраненную сессию:", session_path)

        context = browser.new_context(**context_kwargs)
        context.set_default_timeout(90000)
        context.set_default_navigation_timeout(120000)

        page = context.new_page()

        try:
            page.goto(PLAYERS_REPORT_URL, wait_until="domcontentloaded", timeout=120000)
            page.wait_for_timeout(5000)
            if not is_authenticated(page):
                login_to_partner(page, account)
                save_session_state(context, session_path)
            else:
                log("Отчет открыт по сохраненной сессии")

            open_players_report(page)
            set_date_range(page, period)
            check_new_players(page)
            click_generate_report(page)
            file_path = download_csv(page)
            return file_path, period
        finally:
            browser.close()


def authorize_session(account):
    ensure_runtime_ready()
    session_path = session_path_for_account(account)
    log(f"Режим авторизации: открываю браузер для входа в 1xPartners для кабинета {account['label']}")
    set_status(current_account=account["id"], current_account_label=account["label"])
    update_account_status(account, "authenticating", "Открываю браузер для сохранения сессии")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-zygote",
                "--single-process",
            ],
        )

        context_kwargs = {
            "accept_downloads": False,
            "locale": "ru-RU",
            "ignore_https_errors": True,
        }
        if session_path.exists():
            context_kwargs["storage_state"] = str(session_path)
            log("Использую сохраненную сессию:", session_path)

        context = browser.new_context(**context_kwargs)
        context.set_default_timeout(90000)
        context.set_default_navigation_timeout(120000)
        page = context.new_page()

        try:
            page.goto(PLAYERS_REPORT_URL, wait_until="domcontentloaded", timeout=120000)
            page.wait_for_timeout(5000)
            if not is_authenticated(page):
                login_to_partner(page, account)
            else:
                log("Активная сессия уже есть")

            if not is_authenticated(page):
                raise RuntimeError("Сессия не была подтверждена")

            save_session_state(context, session_path)
            log("Авторизация сохранена, теперь можно запускать парсер из CRM")
            update_account_status(account, "authorized", "Сессия сохранена")
            return {"authorized": True, "session_state_path": str(session_path), "account_id": account["id"]}
        finally:
            browser.close()


def upload_to_crm(file_path: Path, period: dict, account: dict):
    headers = {}
    if CRM_IMPORT_API_KEY:
        headers["X-API-Key"] = CRM_IMPORT_API_KEY

    log("Отправка в CRM:", file_path)

    with open(file_path, "rb") as f:
        response = requests.post(
            CRM_IMPORT_URL,
            headers=headers,
            data={
                "source_name": f"1xbet_players_{account['id']}",
                "date_start": period["date_start"],
                "date_end": period["date_end"],
                "period_mode": "half_month",
            },
            files={"file": (file_path.name, f)},
            timeout=180,
        )

    response.raise_for_status()
    result = response.json()
    log("Ответ CRM:", result)
    return result


def run_parser(selected_accounts):
    results = []
    for index, account in enumerate(selected_accounts, start=1):
        set_status(
            message=f"Обрабатываю кабинет {index}/{len(selected_accounts)}: {account['label']}",
            current_account=account["id"],
            current_account_label=account["label"],
        )
        try:
            exported_file, current_period = export_players_report(account)
            result = upload_to_crm(exported_file, current_period, account)
            update_account_status(account, "success", "Импорт завершен успешно", period=current_period["period_label"])
            results.append({
                "account_id": account["id"],
                "account_label": account["label"],
                "period": current_period,
                "crm_result": result,
            })
        except Exception as exc:
            update_account_status(account, "error", str(exc))
            raise
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["run", "auth"], default=PARSER_MODE)
    parser.add_argument("--accounts", default=ACCOUNT_IDS_ENV)
    args = parser.parse_args()

    try:
        selected_accounts = resolve_selected_accounts(args.accounts)
        set_status(
            status="running",
            mode=args.mode,
            needs_user_action=False,
            session_saved=any(session_path_for_account(account).exists() for account in selected_accounts),
            started_at=datetime.now().isoformat(timespec="seconds"),
            finished_at=None,
            error=None,
            selected_accounts=[{"id": item["id"], "label": item["label"]} for item in selected_accounts],
        )

        if args.mode == "auth":
            result = [authorize_session(account) for account in selected_accounts]
        else:
            result = run_parser(selected_accounts)

        set_status(
            status="success",
            mode=args.mode,
            needs_user_action=False,
            finished_at=datetime.now().isoformat(timespec="seconds"),
            result=result,
            session_saved=any(session_path_for_account(account).exists() for account in selected_accounts),
        )
        print("crm_result:", result)
        return 0
    except Exception as exc:
        current_status = "waiting_for_user" if "captcha" in str(exc).lower() else "error"
        try:
            accounts_for_status = resolve_selected_accounts(args.accounts)
            session_saved = any(session_path_for_account(account).exists() for account in accounts_for_status)
        except Exception:
            session_saved = False
        set_status(
            status=current_status,
            mode=args.mode,
            finished_at=datetime.now().isoformat(timespec="seconds"),
            error=str(exc),
            needs_user_action=current_status == "waiting_for_user",
            session_saved=session_saved,
        )
        print(f"[1XBET_PARSER_ERROR] {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
