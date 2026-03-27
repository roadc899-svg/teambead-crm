import os
import calendar
from pathlib import Path
from datetime import date, datetime

import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

load_dotenv()

PARTNER_LOGIN = os.getenv("PARTNER_LOGIN", "").strip()
PARTNER_PASSWORD = os.getenv("PARTNER_PASSWORD", "").strip()
CRM_IMPORT_URL = os.getenv("CRM_IMPORT_URL", "https://crm.teambead.work/api/partner/import").strip()
CRM_IMPORT_API_KEY = os.getenv("TEAMBEAD_PARTNER_IMPORT_KEY", "").strip()
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

LOGIN_URL = "https://1xpartners.com/ru/sign-in"
PLAYERS_REPORT_URL = "https://1xpartners.com/ru/partner/reports/players"

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)


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


def log(*args):
    print("[1XBET_PARSER]", *args, flush=True)


def fill_first_visible(page, selectors, value, label):
    for sel in selectors:
        try:
            locator = page.locator(sel).first
            locator.wait_for(state="visible", timeout=10000)
            locator.fill(value)
            log(f"{label}: заполнено через {sel}")
            return True
        except Exception:
            pass
    return False


def click_first(page, selectors, label, timeout=10000):
    for sel in selectors:
        try:
            locator = page.locator(sel).first
            locator.click(timeout=timeout)
            log(f"{label}: клик через {sel}")
            return True
        except Exception:
            pass
    return False


def set_date_range(page, period):
    # сначала пробуем явные селекторы
    from_ok = fill_first_visible(
        page,
        [
            'input[name="dateFrom"]',
            'input[name="from"]',
            'input[placeholder*="2026-03-16"]',
            'input[placeholder*="YYYY-MM-DD"]',
        ],
        period["date_start"],
        "date_from",
    )

    to_ok = fill_first_visible(
        page,
        [
            'input[name="dateTo"]',
            'input[name="to"]',
            'input[placeholder*="2026-03-27"]',
            'input[placeholder*="YYYY-MM-DD"]',
        ],
        period["date_end"],
        "date_to",
    )

    if from_ok and to_ok:
        return

    # fallback: ищем первые два видимых input
    try:
        inputs = page.locator("input")
        visible_inputs = []
        for i in range(inputs.count()):
            inp = inputs.nth(i)
            try:
                if inp.is_visible():
                    visible_inputs.append(inp)
            except Exception:
                pass

        if len(visible_inputs) >= 2:
            visible_inputs[0].fill(period["date_start"])
            visible_inputs[1].fill(period["date_end"])
            log("date_range: установлен через fallback первые 2 input")
            return
    except Exception:
        pass

    raise RuntimeError("Не удалось заполнить даты периода")


def check_new_players(page):
    # основной вариант
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

    # fallback — клик по тексту
    try:
        page.locator('text=Только новые игроки').first.click(timeout=5000)
        log("checkbox: клик по тексту 'Только новые игроки'")
        return
    except Exception:
        pass

    # fallback — первая доступная checkbox
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


def export_players_report():
    if not PARTNER_LOGIN or not PARTNER_PASSWORD:
        raise RuntimeError("Заполни PARTNER_LOGIN и PARTNER_PASSWORD в .env")

    period = get_half_month_period()
    log("Период:", period)

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

        context = browser.new_context(
            accept_downloads=True,
            locale="ru-RU",
            ignore_https_errors=True,
        )
        context.set_default_timeout(60000)
        context.set_default_navigation_timeout(90000)
        page = context.new_page()

        try:
            # 1. Логин
            log("Открываю страницу логина")
            page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_load_state("networkidle", timeout=90000)

            login_ok = fill_first_visible(
                page,
                [
                    'input[name="login"]',
                    'input[name="username"]',
                    'input[autocomplete="username"]',
                    'input[type="email"]',
                    'input[type="text"]',
                ],
                PARTNER_LOGIN,
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
                PARTNER_PASSWORD,
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

            page.wait_for_load_state("networkidle", timeout=90000)
            page.wait_for_timeout(3000)

            # 2. Отчет по игрокам
            log("Открываю отчет по игрокам")
            page.goto(PLAYERS_REPORT_URL, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_load_state("networkidle", timeout=90000)
            page.wait_for_timeout(3000)

            # 3. Даты
            set_date_range(page, period)

            # 4. Только новые игроки
            check_new_players(page)

            # 5. Сгенерировать отчет
            generated = click_first(
                page,
                [
                    'button:has-text("СГЕНЕРИРОВАТЬ ОТЧЕТ")',
                    'button:has-text("Сгенерировать отчет")',
                    'text=СГЕНЕРИРОВАТЬ ОТЧЕТ',
                ],
                "generate_report",
                timeout=15000,
            )
            if not generated:
                raise RuntimeError("Не удалось нажать 'СГЕНЕРИРОВАТЬ ОТЧЕТ'")

            page.wait_for_load_state("networkidle", timeout=90000)
            page.wait_for_timeout(5000)

            # 6. Экспорт CSV
            file_path = None
            try:
                with page.expect_download(timeout=60000) as download_info:
                    try:
                        # сначала пробуем напрямую CSV
                        page.locator('text=CSV').first.click(timeout=10000)
                    except Exception:
                        # если не получилось — открываем экспорт и потом CSV
                        opened = click_first(
                            page,
                            [
                                'button:has-text("ЭКСПОРТ")',
                                'text=ЭКСПОРТ',
                            ],
                            "export_menu",
                            timeout=10000,
                        )
                        if not opened:
                            raise RuntimeError("Не удалось открыть меню 'ЭКСПОРТ'")

                        page.wait_for_timeout(1500)

                        clicked_csv = click_first(
                            page,
                            [
                                'text=CSV',
                                'a:has-text("CSV")',
                                'button:has-text("CSV")',
                            ],
                            "csv_button",
                            timeout=10000,
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

            return file_path, period

        finally:
            browser.close()


def upload_to_crm(file_path: Path, period: dict):
    headers = {}
    if CRM_IMPORT_API_KEY:
        headers["X-API-Key"] = CRM_IMPORT_API_KEY

    log("Отправка в CRM:", file_path)

    with open(file_path, "rb") as f:
        response = requests.post(
            CRM_IMPORT_URL,
            headers=headers,
            data={
                "source_name": "1xbet_players",
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


def run_parser():
    exported_file, current_period = export_players_report()
    result = upload_to_crm(exported_file, current_period)
    return result


if __name__ == "__main__":
    result = run_parser()
    print("crm_result:", result)
