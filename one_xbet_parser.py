import json
import os
import time
from typing import Callable, Optional

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "uploaded_data", "onexbet_parser_config.json")
DEFAULT_PLAYERS_REPORT_URL = "https://1xpartners.com/ru/partner/reports/players"


def load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        return {}
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


class OneXBetParser:
    def __init__(self, headless: bool = True):
        self.driver = None
        self.headless = headless
        self.setup_driver()

    def setup_driver(self) -> None:
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")

        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

    def restart_without_headless(self) -> None:
        current_url = self.driver.current_url if self.driver else ""
        self.close()
        self.headless = False
        self.setup_driver()
        if current_url:
            self.driver.get(current_url)

    def find_any(self, selectors) -> bool:
        for by, selector in selectors:
            try:
                elements = self.driver.find_elements(by, selector)
                if elements:
                    return True
            except Exception:
                continue
        return False

    def find_first_visible(self, selectors, timeout: int = 10):
        end_time = time.time() + timeout
        while time.time() < end_time:
            for by, selector in selectors:
                try:
                    elements = self.driver.find_elements(by, selector)
                    for element in elements:
                        if element.is_displayed():
                            return element
                except Exception:
                    continue
            time.sleep(0.4)
        return None

    def visible_elements(self, selectors):
        result = []
        for by, selector in selectors:
            try:
                for element in self.driver.find_elements(by, selector):
                    if element.is_displayed():
                        result.append(element)
            except Exception:
                continue
        return result

    def click_element(self, element) -> bool:
        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
            element.click()
            return True
        except Exception:
            try:
                ActionChains(self.driver).move_to_element(element).click().perform()
                return True
            except Exception:
                try:
                    self.driver.execute_script("arguments[0].click();", element)
                    return True
                except Exception:
                    return False

    def set_input_value(self, element, value: str) -> bool:
        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
            element.clear()
        except Exception:
            pass
        try:
            element.send_keys(value)
            return True
        except Exception:
            try:
                self.driver.execute_script(
                    """
                    arguments[0].value = arguments[1];
                    arguments[0].dispatchEvent(new Event('input', {bubbles: true}));
                    arguments[0].dispatchEvent(new Event('change', {bubbles: true}));
                    arguments[0].dispatchEvent(new Event('blur', {bubbles: true}));
                    """,
                    element,
                    value,
                )
                return True
            except Exception:
                return False

    def find_clickable_text(self, texts, timeout: int = 8):
        end_time = time.time() + timeout
        lowered_texts = [text.lower() for text in texts if text]
        xpath = "//*[(self::a or self::button or self::label or self::span or self::div)]"
        while time.time() < end_time:
            try:
                elements = self.driver.find_elements(By.XPATH, xpath)
            except Exception:
                elements = []
            for element in elements:
                try:
                    if not element.is_displayed():
                        continue
                    content = (element.text or "").strip().lower()
                    if content and any(text in content for text in lowered_texts):
                        return element
                except Exception:
                    continue
            time.sleep(0.4)
        return None

    def check_recaptcha_v2(self) -> bool:
        selectors = [
            (By.CSS_SELECTOR, "iframe[src*='recaptcha']"),
            (By.CSS_SELECTOR, "div.g-recaptcha"),
            (By.CSS_SELECTOR, "iframe[title*='recaptcha']"),
            (By.CSS_SELECTOR, "div.recaptcha-checkbox-border"),
            (By.XPATH, "//div[contains(@class, 'recaptcha')]"),
        ]
        return self.find_any(selectors)

    def check_recaptcha_v3(self) -> bool:
        try:
            scripts = self.driver.find_elements(By.TAG_NAME, "script")
            for script in scripts:
                content = script.get_attribute("src") or script.get_attribute("innerHTML") or ""
                lowered = content.lower()
                if "recaptcha" in lowered and "api.js" in lowered:
                    if "render=explicit" not in lowered and "render=onload" not in lowered:
                        return True
        except Exception:
            return False
        return False

    def check_recaptcha_challenge(self) -> bool:
        selectors = [
            (By.CSS_SELECTOR, "iframe[src*='bframe']"),
            (By.CSS_SELECTOR, "div.rc-imageselect-desc"),
            (By.CSS_SELECTOR, "div.rc-imageselect-target"),
            (By.XPATH, "//div[contains(text(), 'Select all images')]"),
            (By.XPATH, "//div[contains(text(), 'Please select all')]"),
        ]
        for by, selector in selectors:
            try:
                element = self.driver.find_element(by, selector)
                if element.is_displayed():
                    return True
            except Exception:
                continue
        return False

    def maybe_open_recaptcha(self) -> None:
        try:
            recaptcha_iframe = self.driver.find_element(
                By.CSS_SELECTOR,
                "iframe[src*='recaptcha'], iframe[title*='recaptcha']",
            )
            self.driver.switch_to.frame(recaptcha_iframe)
            checkbox = self.driver.find_element(
                By.CSS_SELECTOR,
                ".recaptcha-checkbox-border, #recaptcha-anchor",
            )
            checkbox.click()
        except Exception:
            pass
        finally:
            try:
                self.driver.switch_to.default_content()
            except Exception:
                pass

    def wait_for_manual_captcha_solution(self, timeout: int = 300) -> bool:
        print("\n" + "=" * 50)
        print("reCAPTCHA detected.")
        print("Solve it manually in the browser window, then the parser will continue.")
        print(f"Timeout: {timeout // 60} minutes.")
        print("=" * 50 + "\n")

        if self.headless:
            print("Switching to visible browser mode for captcha solving...")
            self.restart_without_headless()

        start_time = time.time()
        last_reported = -30
        while time.time() - start_time < timeout:
            if not self.check_recaptcha_challenge() and not self.check_recaptcha_v2():
                print("Captcha is no longer visible. Resuming parser...")
                return True

            elapsed = int(time.time() - start_time)
            if elapsed - last_reported >= 30:
                last_reported = elapsed
                remaining = timeout - elapsed
                print(f"Waiting for manual captcha solution... {remaining} seconds left")
            time.sleep(1)

        print("Timed out while waiting for captcha solution.")
        return False

    def wait_until_page_ready(self, timeout: int = 20) -> None:
        WebDriverWait(self.driver, timeout).until(
            lambda driver: driver.execute_script("return document.readyState") == "complete"
        )

    def open_page(self, url: str) -> None:
        print(f"Loading page: {url}")
        self.driver.get(url)
        self.wait_until_page_ready()

    def handle_possible_captcha(self) -> bool:
        if not (self.check_recaptcha_v2() or self.check_recaptcha_v3()):
            return True

        print("Captcha was detected on the page.")
        if not self.check_recaptcha_challenge():
            time.sleep(2)
            self.maybe_open_recaptcha()

        if self.check_recaptcha_challenge() or self.check_recaptcha_v2():
            return self.wait_for_manual_captcha_solution()
        return True

    def login_if_needed(
        self,
        login: str = "",
        password: str = "",
        timeout: int = 15,
    ) -> bool:
        if not login or not password:
            print("Login/password are not set. Skipping automatic login step.")
            return False

        username_selectors = [
            (By.CSS_SELECTOR, "input[name='login']"),
            (By.CSS_SELECTOR, "input[name='username']"),
            (By.CSS_SELECTOR, "input[name='email']"),
            (By.CSS_SELECTOR, "input[type='email']"),
            (By.CSS_SELECTOR, "input[type='text']"),
        ]
        password_selectors = [
            (By.CSS_SELECTOR, "input[name='password']"),
            (By.CSS_SELECTOR, "input[type='password']"),
        ]
        submit_selectors = [
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.CSS_SELECTOR, "input[type='submit']"),
            (By.XPATH, "//button[contains(., 'Log in')]"),
            (By.XPATH, "//button[contains(., 'Login')]"),
            (By.XPATH, "//button[contains(., 'Sign in')]"),
            (By.XPATH, "//button[contains(., 'Войти')]"),
        ]

        try:
            username_input = self.find_first_visible(username_selectors, timeout=timeout)
            password_input = self.find_first_visible(password_selectors, timeout=timeout)
            submit_button = self.find_first_visible(submit_selectors, timeout=timeout)

            if not username_input or not password_input:
                print("Visible login form fields were not found. Assuming session may already be active.")
                return False

            username_input.clear()
            username_input.send_keys(login)
            password_input.clear()
            password_input.send_keys(password)

            if submit_button:
                submit_button.click()
            else:
                password_input.submit()

            self.wait_until_page_ready(timeout=timeout)
            print("Login form submitted.")
            return True
        except (TimeoutException, NoSuchElementException) as exc:
            print(f"Login form was not found or changed: {exc}")
            return False

    def ensure_logged_in(self, login: str = "", password: str = "", timeout: int = 15) -> None:
        did_submit = self.login_if_needed(login=login, password=password, timeout=timeout)
        if did_submit:
            time.sleep(2)
            self.handle_possible_captcha()

    def navigate_to_report(self, report_url: str = "") -> None:
        target_url = (report_url or "").strip() or DEFAULT_PLAYERS_REPORT_URL
        self.open_page(target_url)
        self.handle_possible_captcha()

    def open_players_report(self, report_url: str = "") -> None:
        self.navigate_to_report(report_url)

        current_url = (self.driver.current_url or "").lower()
        if "/reports/players" in current_url:
            print("Players report page opened directly.")
            return

        players_link = self.find_clickable_text(
            ["players report", "players", "player report", "отчет по игрокам", "игроки"],
            timeout=6,
        )
        if players_link:
            if self.click_element(players_link):
                print("Players report link opened.")
                time.sleep(2)
                self.wait_until_page_ready()
                self.handle_possible_captcha()

    def set_custom_period_mode(self) -> None:
        period_trigger = self.find_first_visible(
            [
                (By.CSS_SELECTOR, "div[role='combobox']"),
                (By.CSS_SELECTOR, "input[placeholder*='Произволь']"),
                (By.XPATH, "//*[contains(text(), 'Произволь')]"),
                (By.XPATH, "//*[contains(text(), 'Период')]"),
            ],
            timeout=4,
        )
        if period_trigger and self.click_element(period_trigger):
            time.sleep(1)
            custom_option = self.find_clickable_text(
                ["произвольный", "custom"],
                timeout=4,
            )
            if custom_option and self.click_element(custom_option):
                print("Custom period mode selected.")

    def set_date_range(self, date_from: str, date_to: str) -> None:
        if not date_from and not date_to:
            return

        self.set_custom_period_mode()

        from_candidates = [
            (By.CSS_SELECTOR, "input[type='date']"),
            (By.CSS_SELECTOR, "input[name*='from']"),
            (By.CSS_SELECTOR, "input[name*='start']"),
            (By.CSS_SELECTOR, "input[placeholder*='From']"),
            (By.CSS_SELECTOR, "input[placeholder*='Start']"),
            (By.CSS_SELECTOR, "input[placeholder*='Дата с']"),
            (By.CSS_SELECTOR, "input[placeholder*='с']"),
        ]
        to_candidates = [
            (By.CSS_SELECTOR, "input[type='date']"),
            (By.CSS_SELECTOR, "input[name*='to']"),
            (By.CSS_SELECTOR, "input[name*='end']"),
            (By.CSS_SELECTOR, "input[placeholder*='To']"),
            (By.CSS_SELECTOR, "input[placeholder*='End']"),
            (By.CSS_SELECTOR, "input[placeholder*='Дата по']"),
            (By.CSS_SELECTOR, "input[placeholder*='по']"),
        ]
        all_date_inputs = self.visible_elements([(By.CSS_SELECTOR, "input")])
        date_like_inputs = []
        for item in all_date_inputs:
            try:
                joined = " ".join(
                    filter(
                        None,
                        [
                            item.get_attribute("type"),
                            item.get_attribute("name"),
                            item.get_attribute("placeholder"),
                            item.get_attribute("class"),
                        ],
                    )
                ).lower()
                if any(token in joined for token in ["date", "from", "to", "start", "end", "period", "с", "по", "calendar"]):
                    date_like_inputs.append(item)
            except Exception:
                continue

        unique_inputs = []
        seen_ids = set()
        for item in date_like_inputs:
            try:
                marker = item.id
            except Exception:
                marker = id(item)
            if marker not in seen_ids:
                seen_ids.add(marker)
                unique_inputs.append(item)

        from_input = self.find_first_visible(from_candidates, timeout=4)
        to_input = self.find_first_visible(to_candidates, timeout=4)

        if from_input and to_input and from_input == to_input and len(unique_inputs) > 1:
            from_input = unique_inputs[0]
            to_input = unique_inputs[1]
        elif not from_input and unique_inputs:
            from_input = unique_inputs[0]
        elif not from_input and date_like_inputs:
            from_input = date_like_inputs[0]

        if not to_input and len(unique_inputs) > 1:
            to_input = unique_inputs[1]
        elif not to_input and len(date_like_inputs) > 1:
            to_input = date_like_inputs[1]

        if from_input and date_from:
            self.set_input_value(from_input, date_from)
            print(f"Date from set to {date_from}")
        if to_input and date_to:
            self.set_input_value(to_input, date_to)
            print(f"Date to set to {date_to}")

    def set_new_players_only(self, enabled: bool) -> None:
        if not enabled:
            return

        checkbox_selectors = [
            (By.CSS_SELECTOR, "input[type='checkbox'][name*='new']"),
            (By.CSS_SELECTOR, "input[type='checkbox'][id*='new']"),
            (By.CSS_SELECTOR, "input[type='checkbox'][value*='new']"),
        ]
        checkbox = self.find_first_visible(checkbox_selectors, timeout=3)
        if checkbox:
            try:
                if not checkbox.is_selected():
                    self.click_element(checkbox)
                print("New players checkbox enabled.")
                return
            except Exception:
                pass

        label = self.find_clickable_text(
            ["new players", "only new players", "новые игроки", "только новые игроки"],
            timeout=5,
        )
        if label and self.click_element(label):
            print("New players filter selected.")

    def apply_filters(self, date_from: str = "", date_to: str = "", new_players_only: bool = False) -> None:
        self.set_date_range(date_from, date_to)
        self.set_new_players_only(new_players_only)

        apply_button = self.find_clickable_text(
            ["сгенерировать отчет", "generate report", "apply", "show", "filter", "search", "показать", "применить", "найти"],
            timeout=4,
        )
        if apply_button and self.click_element(apply_button):
            print("Report filters applied.")
            time.sleep(2)
            self.wait_until_page_ready()
            self.handle_possible_captcha()

    def run_default_flow(self, config: dict, parse_function: Optional[Callable] = None):
        start_url = config.get("start_url") or "https://partners.1xbet.com/"
        report_url = config.get("report_url") or DEFAULT_PLAYERS_REPORT_URL
        login = config.get("login") or ""
        password = config.get("password") or ""
        date_from = config.get("date_from") or ""
        date_to = config.get("date_to") or ""
        new_players_only = bool(config.get("new_players_only"))

        self.open_page(start_url)
        if not self.handle_possible_captcha():
            return None

        self.ensure_logged_in(login=login, password=password)

        self.open_players_report(report_url=report_url)
        self.apply_filters(
            date_from=date_from,
            date_to=date_to,
            new_players_only=new_players_only,
        )

        if parse_function:
            return parse_function(self.driver)
        return {"title": self.driver.title, "url": self.driver.current_url}

    def parse_with_captcha_handling(
        self,
        url: str,
        parse_function: Optional[Callable] = None,
    ):
        try:
            self.open_page(url)
            if not self.handle_possible_captcha():
                return None

            if parse_function:
                return parse_function(self.driver)
            return self.driver.title
        except Exception as exc:
            print(f"Parser failed: {exc}")
            return None

    def close(self) -> None:
        if self.driver:
            self.driver.quit()
            self.driver = None


def example_parse_function(driver):
    print(f"Current page title: {driver.title}")
    return {"title": driver.title, "url": driver.current_url}


def main():
    config = load_config()
    parser = OneXBetParser(headless=bool(config.get("headless", True)))
    try:
        result = parser.run_default_flow(config, example_parse_function)
        if result:
            print(f"Parsing result: {result}")
        else:
            print("No result returned.")
    finally:
        parser.close()


if __name__ == "__main__":
    main()
