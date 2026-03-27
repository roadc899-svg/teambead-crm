import os
import calendar
from pathlib import Path
from datetime import date, datetime

import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

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


def export_players_report():
    if not PARTNER_LOGIN or not PARTNER_PASSWORD:
        raise RuntimeError("Заполни PARTNER_LOGIN и PARTNER_PASSWORD в .env")

    period = get_half_month_period()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
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

        # 1. Логин
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_load_state("networkidle", timeout=90000)

        login_filled = False
        for sel in [
            'input[name="login"]',
            'input[name="username"]',
            'input[type="email"]',
            'input[type="text"]',
            'input[autocomplete="username"]',
        ]:
            try:
                locator = page.locator(sel).first
                locator.wait_for(state="visible", timeout=10000)
                locator.fill(PARTNER_LOGIN)
                login_filled = True
                break
            except Exception:
                pass

        if not login_filled:
            browser.close()
            raise RuntimeError("Не удалось найти поле логина на странице входа")

        password_filled = False
        for sel in [
            'input[name="password"]',
            'input[type="password"]',
            'input[autocomplete="current-password"]',
        ]:
            try:
                locator = page.locator(sel).first
                locator.wait_for(state="visible", timeout=10000)
                locator.fill(PARTNER_PASSWORD)
                password_filled = True
                break
            except Exception:
                pass

        if not password_filled:
            browser.close()
            raise RuntimeError("Не удалось найти поле пароля на странице входа")

        clicked = False
        for sel in [
            'button[type="submit"]',
            'button:has-text("Войти")',
            'button:has-text("Login")',
            'button:has-text("Sign in")',
            'text=Войти',
        ]:
            try:
                page.locator(sel).first.click(timeout=10000)
                clicked = True
                break
            except Exception:
                pass

        if not clicked:
            browser.close()
            raise RuntimeError("Не удалось найти кнопку входа")

        page.wait_for_load_state("networkidle", timeout=90000)

        # 2. Переход в отчет по игрокам
        page.goto(PLAYERS_REPORT_URL, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_load_state("networkidle", timeout=90000)

        # 3. Выставляем даты
        date_inputs = page.locator("input")
        filled_from = False
        filled_to = False

        for i in range(date_inputs.count()):
            try:
                inp = date_inputs.nth(i)
                placeholder = (inp.get_attribute("placeholder") or "").strip()
                value = ""
                try:
                    value = inp.input_value().strip()
                except Exception:
                    value = ""

                if not filled_from and ("2026-03-16" in placeholder or "2026-03-16" in value or placeholder == "" or "-" in value):
                    inp.fill(period["date_start"])
                    filled_from = True
                    continue

                if filled_from and not filled_to:
                    inp.fill(period["date_end"])
                    filled_to = True
                    break
            except Exception:
                pass

        if not filled_from:
            for sel in ['input[name="dateFrom"]', 'input[name="from"]']:
                try:
                    page.locator(sel).first.fill(period["date_start"])
                    filled_from = True
                    break
                except Exception:
                    pass

        if not filled_to:
            for sel in ['input[name="dateTo"]', 'input[name="to"]']:
                try:
                    page.locator(sel).first.fill(period["date_end"])
                    filled_to = True
                    break
                except Exception:
                    pass

        # 4. Галочка "Только новые игроки"
        checked_new_players = False
        try:
            label = page.locator('label:has-text("Только новые игроки")')
            if label.count() > 0:
                checkbox = label.locator('input[type="checkbox"]').first
                if not checkbox.is_checked():
                    checkbox.check()
                checked_new_players = True
        except Exception:
            pass

        if not checked_new_players:
            try:
                all_checkboxes = page.locator('input[type="checkbox"]')
                if all_checkboxes.count() > 0:
                    cb = all_checkboxes.nth(0)
                    if not cb.is_checked():
                        cb.check()
            except Exception:
                pass

        # 5. Нажимаем "СГЕНЕРИРОВАТЬ ОТЧЕТ"
        generated = False
        for sel in [
            'button:has-text("СГЕНЕРИРОВАТЬ ОТЧЕТ")',
            'button:has-text("Сгенерировать отчет")',
            'text=СГЕНЕРИРОВАТЬ ОТЧЕТ',
        ]:
            try:
                page.locator(sel).first.click(timeout=10000)
                page.wait_for_load_state("networkidle", timeout=90000)
                generated = True
                break
            except Exception:
                pass

        if not generated:
            browser.close()
            raise RuntimeError("Не удалось нажать кнопку 'СГЕНЕРИРОВАТЬ ОТЧЕТ'")

        # 6. Открываем меню "ЭКСПОРТ"
        export_opened = False
        for sel in [
            'button:has-text("ЭКСПОРТ")',
            'text=ЭКСПОРТ',
        ]:
            try:
                page.locator(sel).first.click(timeout=10000)
                export_opened = True
                break
            except Exception:
                pass

        if not export_opened:
            browser.close()
            raise RuntimeError("Не удалось открыть меню 'ЭКСПОРТ'")

        # 7. Качаем CSV
        file_path = None
        for sel in [
            'text=CSV',
            'a:has-text("CSV")',
            'button:has-text("CSV")',
        ]:
            try:
                with page.expect_download(timeout=30000) as d:
                    page.locator(sel).first.click(timeout=10000)
                download = d.value
                filename = download.suggested_filename or f"1xbet_players_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                file_path = DOWNLOAD_DIR / filename
                download.save_as(str(file_path))
                break
            except Exception:
                pass

        browser.close()

        if not file_path:
            raise RuntimeError("Не удалось скачать CSV из меню 'ЭКСПОРТ'")

    return file_path, period


def upload_to_crm(file_path: Path, period: dict):
    headers = {}
    if CRM_IMPORT_API_KEY:
        headers["X-API-Key"] = CRM_IMPORT_API_KEY

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
    return response.json()


if __name__ == "__main__":
    exported_file, current_period = export_players_report()
    result = upload_to_crm(exported_file, current_period)
    print("period:", current_period)
    print("crm_result:", result)
