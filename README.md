# teambead-crm

## 1xBet parser setup

```bash
python3 -m pip install -r requirements.txt
python3 -m playwright install chromium
cp .env.example .env
```

Заполни в `.env`:

- `PARTNER_LOGIN`
- `PARTNER_PASSWORD`
- `TEAMBEAD_PARTNER_IMPORT_KEY` если импорт в CRM защищен ключом
- `HEADLESS=false` для первого запуска, если 1xPartners просит captcha
- после успешного ручного входа можно вернуть `HEADLESS=true`, парсер попробует использовать сохраненную сессию из `playwright_state_1xbet.json`

После этого парсер можно запускать из интерфейса CRM или напрямую:

```bash
python3 parser_1xbet_halfmonth.py
```

В CRM для 1xBet есть 2 сценария:

- `Open 1xBet Login` открывает браузер для ручного входа и сохраняет сессию
- `Run Parser` запускает фоновый импорт и пишет live log на странице
- список кабинетов хранится в `uploaded_data/onexbet_runtime/accounts.json`
- у каждого кабинета своя сессия в `uploaded_data/onexbet_runtime/sessions/`
