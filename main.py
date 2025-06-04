import json
import os
import time
import logging
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from apscheduler.schedulers.blocking import BlockingScheduler
from requests.exceptions import HTTPError, RequestException
from dotenv import load_dotenv

load_dotenv()

# ==== Настройки ====
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
PER_PAGE = 250            # Запрашиваем по 200 монет за запрос
TOTAL_COINS = 2000       # «Фризим» первые 2000
PAGES = TOTAL_COINS // PER_PAGE  # =20 страниц
CURRENCY = "usd"

SERVICE_ACCOUNT_FILE = "credentials.json"
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
WORKSHEET_NAME = "Цена"
FROZEN_FILE = "frozen_coins.json"  # локальный кеш списка id

# ==== Логирование ====
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)
logger = logging.getLogger("price_updater")

# ==== Авторизация Google Sheets ====
def get_gsheet_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)
    return client

# ==== Чтение/запись локального кеша «зажатых» монет ====
def load_frozen_coins():
    if os.path.exists(FROZEN_FILE):
        try:
            with open(FROZEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list) and len(data) >= TOTAL_COINS:
                    logger.info("Загружены %d «фризнутых» монет из локального кеша.", len(data))
                    return data[:TOTAL_COINS]
        except Exception as e:
            logger.warning("Не удалось прочитать %s: %s", FROZEN_FILE, e)
    return None

def save_frozen_coins(ids):
    try:
        with open(FROZEN_FILE, "w", encoding="utf-8") as f:
            json.dump(ids, f, ensure_ascii=False)
        logger.info("Локальный кеш сохранён (%s) с %d ID.", FROZEN_FILE, len(ids))
    except Exception as e:
        logger.error("Ошибка сохранения локального кеша: %s", e)

# ==== Сбор топ-2000 (id, symbol, price) с бесконечным retry при 429 ====
def fetch_top_coins_with_price():
    """
    Делает 20 запросов по 100 монет.
    При HTTP 429 ждёт 60 сек и повторяет (без ограничения числа попыток).
    Между удачными запросами делает паузу 10 сек.
    Возвращает список из {"id","symbol","price"} длиной ровно TOTAL_COINS.
    """
    items = []
    for page in range(1, PAGES + 1):
        while True:  # цикл до успешного получения 100 монет
            params = {
                "vs_currency": CURRENCY,
                "order": "market_cap_desc",
                "per_page": PER_PAGE,
                "page": page,
                "sparkline": False
            }
            try:
                resp = requests.get(COINGECKO_URL, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, list) or len(data) < PER_PAGE:
                    # Если пришло меньше 100, возможно неполный ответ → считаем ошибкой и повторяем
                    raise ValueError(f"Получено {len(data)} записей вместо {PER_PAGE}")
                break
            except HTTPError as he:
                code = he.response.status_code
                if code == 429:
                    logger.warning("HTTP 429 на странице %d: ждём 60 сек...", page)
                    time.sleep(60)
                    continue
                else:
                    logger.error("HTTPError %d на странице %d: %s", code, page, he)
                    return items
            except (RequestException, ValueError) as e:
                logger.error("Ошибка на странице %d: %s (будем повторять через 60 сек)", page, e)
                time.sleep(60)
                continue

        # Успешно получили ровно 100 монет
        for coin in data:
            items.append({
                "id": coin["id"],
                "symbol": coin["symbol"].upper(),
                "price": coin["current_price"]
            })
        logger.info("Страница %d: получено 100 монет (итого %d).", page, len(items))
        time.sleep(20)  # пауза 10 сек между запросами

    return items[:TOTAL_COINS]

# ==== Проверка и создание листа в Google Sheets ====
def ensure_worksheet(client):
    sheet = client.open_by_key(SPREADSHEET_ID)
    try:
        return sheet.worksheet(WORKSHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        wks = sheet.add_worksheet(
            title=WORKSHEET_NAME,
            rows=str(TOTAL_COINS + 1),
            cols="3"
        )
        logger.info("Создан новый лист «%s» с %d строк и 3 колонками.", WORKSHEET_NAME, TOTAL_COINS + 1)
        return wks

# ==== Полная запись (ID, Ticker, Price) ====
def write_full_table(wks, coins):
    """
    coins: список словарей [{id, symbol, price}, ...] длиной TOTAL_COINS.
    Записывает диапазон A1:C{TOTAL_COINS+1}.
    """
    values = [["ID", "Ticker", "Price (USD)"]]
    for coin in coins:
        values.append([coin["id"], coin["symbol"], coin["price"]])

    last_row = len(values)  # должно быть TOTAL_COINS + 1
    range_name = f"A1:C{last_row}"
    wks.update(values, range_name)
    logger.info("Записано %d строк в диапазон %s.", last_row - 1, range_name)

# ==== Обновление только цен (столбец C2:C?) ====
def update_prices_only(wks, frozen_ids):
    """
    frozen_ids: список из TOTAL_COINS id, «жёстко» зафиксированных.
    Берёт цены пачками по PER_PAGE, при 429 – ждёт 60 сек и пробует снова.
    """
    total = len(frozen_ids)
    prices = []
    for i in range(0, total, PER_PAGE):
        chunk = frozen_ids[i:i + PER_PAGE]
        while True:
            params = {
                "vs_currency": CURRENCY,
                "ids": ",".join(chunk),
                "order": "market_cap_desc",
                "per_page": PER_PAGE,
                "page": 1,
                "sparkline": False
            }
            try:
                resp = requests.get(COINGECKO_URL, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                break
            except HTTPError as he:
                code = he.response.status_code
                if code == 429:
                    logger.warning("HTTP 429 при обновлении batch %d: ждём 60 сек...", i // PER_PAGE + 1)
                    time.sleep(60)
                    continue
                else:
                    logger.error("HTTPError %d при обновлении batch %d: %s", code, i // PER_PAGE + 1, he)
                    data = []
                    break
            except RequestException as e:
                logger.error("Ошибка при запросе batch %d: %s", i // PER_PAGE + 1, e)
                time.sleep(60)
                continue

        price_map = {c["id"]: c["current_price"] for c in data}
        for coin_id in chunk:
            prices.append([price_map.get(coin_id, "")])
        logger.info("Batch %d: добавлено %d цен.", i // PER_PAGE + 1, len(chunk))
        time.sleep(10)  # пауза 10 сек между пакетами

    last_row = total + 1
    range_name = f"C2:C{last_row}"
    wks.update(prices, range_name)
    logger.info("Обновлено %d цен (диапазон %s).", total, range_name)

# ==== Основная синхронизация ====
def sync_to_sheet():
    client = get_gsheet_client()
    wks = ensure_worksheet(client)

    try:
        header = wks.acell("A1").value
    except Exception:
        header = None

    if header != "ID":
        # Первый раз: скачиваем топ-2000 и сохраняем локально «фризнутые» id
        logger.info("Первичная инициализация: скачиваем топ-2000 монет…")
        coins = fetch_top_coins_with_price()
        if not coins:
            logger.error("Не удалось получить ни одной монеты. Выходим.")
            return
        write_full_table(wks, coins)
        frozen_ids = [c["id"] for c in coins]
        save_frozen_coins(frozen_ids)
    else:
        # Последующие запуски: обновляем только колонки C
        frozen_ids = load_frozen_coins()
        if not frozen_ids:
            logger.info("Локальный кеш не найден, читаем ID из столбца A…")
            frozen_ids = wks.col_values(1)[1:TOTAL_COINS + 1]
        update_prices_only(wks, frozen_ids)

# ==== Запуск APScheduler каждые 15 минут ====
if __name__ == "__main__":
    logger.info("=== Старт скрипта ===")
    scheduler = BlockingScheduler(timezone="UTC")
    # Первая синхронизация сразу
    sync_to_sheet()
    # Далее каждые 15 минут
    scheduler.add_job(sync_to_sheet, "interval", minutes=15)
    logger.info("Scheduler запущен. Обновление каждые 15 минут.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler остановлен пользователем.")
