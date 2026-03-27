import os
import time
import requests
from datetime import datetime, date
from playwright.sync_api import sync_playwright

PARTNER_LOGIN = os.getenv("PARTNER_LOGIN")
PARTNER_PASSWORD = os.getenv("PARTNER_PASSWORD")

CRM_IMPORT_URL = os.getenv("CRM_IMPORT_URL")
CRM_IMPORT_API_KEY = os.getenv("CRM_IMPORT_API_KEY")


def get_period():
    today = date.today()

    if today.day <= 15:
        start = today.replace(day=1)
        end = today.replace(day=15)
    else:
        start = today.replace(day=16)
        end = today

    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def export_players_report():
    date_start, date_end = get_period()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()

        page.goto("https://partners.1xbet.com")

        # логин
        page.fill('input[name="login"]', PARTNER_LOGIN)
        page.fill('input[name="password"]', PARTNER_PASSWORD)
        page.click('button[type="submit"]')

        page.wait_for_timeout(5000)

        # идем в отчеты
        page.goto("https://partners.1xbet.com/players")

        page.wait_for_timeout(3000)

        # выбираем даты
        page.fill('input[name="date_from"]', date_start)
        page.fill('input[name="date_to"]', date_end)

        # только новые игроки
        page.check('input[name="new_players"]')

        # жмем экспорт
        with page.expect_download() as download_info:
            page.click("text=CSV")

        download = download_info.value

        file_path = f"/tmp/report_{int(time.time())}.csv"
        download.save_as(file_path)

        browser.close()

    return file_path, f"{date_start}_{date_end}"


def upload_to_crm(file_path, period):
    with open(file_path, "rb") as f:
        response = requests.post(
            CRM_IMPORT_URL,
            headers={
                "X-API-KEY": CRM_IMPORT_API_KEY
            },
            files={
                "file": f
            },
            data={
                "date_start": period.split("_")[0],
                "date_end": period.split("_")[1]
            }
        )

    return response.json()


def run_parser():
    file_path, period = export_players_report()
    return upload_to_crm(file_path, period)
