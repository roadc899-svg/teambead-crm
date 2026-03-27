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
        "--single-process",
    ],
)
        context = browser.new_context(accept_downloads=True, locale="ru-RU")
        page = context.new_page()

        page.goto(LOGIN_URL, wait_until="domcontentloaded")
        page.fill('input[type="text"]', PARTNER_LOGIN)
        page.fill('input[type="password"]', PARTNER_PASSWORD)
        page.click('button[type="submit"]')
        page.wait_for_load_state("networkidle")

        page.goto(PLAYERS_REPORT_URL, wait_until="networkidle")

        possible_from = [
            'input[name="dateFrom"]',
            'input[name="from"]',
            'input[placeholder*="От"]',
            'input[placeholder*="С"]',
        ]
        possible_to = [
            'input[name="dateTo"]',
            'input[name="to"]',
            'input[placeholder*="До"]',
            'input[placeholder*="По"]',
        ]

        for sel in possible_from:
            try:
                page.locator(sel).first.fill(period["date_start"])
                break
            except Exception:
                pass

        for sel in possible_to:
            try:
                page.locator(sel).first.fill(period["date_end"])
                break
            except Exception:
                pass

        for sel in [
            'button:has-text("Показать")',
            'button:has-text("Применить")',
            'button:has-text("Сформировать")',
            'button:has-text("Show")',
            'button:has-text("Apply")',
        ]:
            try:
                page.locator(sel).first.click()
                page.wait_for_load_state("networkidle")
                break
            except Exception:
                pass

        file_path = None
        for sel in [
            'button:has-text("Экспорт")',
            'button:has-text("Export")',
            'a:has-text("Экспорт")',
            'a:has-text("Export")',
            '[download]',
        ]:
            try:
                with page.expect_download(timeout=15000) as d:
                    page.locator(sel).first.click()
                download = d.value
                filename = download.suggested_filename or f"1xbet_players_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                file_path = DOWNLOAD_DIR / filename
                download.save_as(str(file_path))
                break
            except Exception:
                pass

        browser.close()

        if not file_path:
            raise RuntimeError("Не удалось скачать отчет по игрокам")

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
