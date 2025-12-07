import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон.

    Args:
        last_id (str): Чтобы загружать товары по частям. Пустая строка - начать с начала, иначе - продолжить с этого места.
        client_id (str): Идентификатор клиента Ozon API.
        seller_token (str): Токен продавца для аутентификации в Ozon API.

    Returns:
        dict: Возвращает словарь, содержащий список товаров из магазина на Ozon.

    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса к API Ozon.
        requests.exceptions.RequestException: При проблемах с сетью или таймаутом.

    Examples:
        >>> get_product_list("", "client123", "token456")
        {'items': [...], 'total': 100, 'last_id': 'next_page_token'}

        >>> get_product_list("invalid", "wrong_client", "wrong_token")
        # Вызовет HTTPError из-за неверных учетных данных
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон.

    Args:
        client_id (str): Идентификатор клиента Ozon API.
        seller_token (str): Токен продавца для аутентификации в Ozon API.

    Returns:
        list: Возвращает список артикулов в магазине Ozon.

    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса к API Ozon.
        requests.exceptions.RequestException: При проблемах с сетью или таймаутом.

    Examples:
        >>> get_offer_ids("valid_client_id", "valid_token")
        ['артикул1', 'артикул2', 'артикул3']

        >>> get_offer_ids("", "")
        # Вызовет HTTPError из-за неверных учетных данных
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров на маркетплейсе Ozon.

    Args:
        prices (list): Список словарей с данными для обновления цен.
                       Каждый словарь должен содержать ключи: offer_id, price, 
                       currency_code, old_price, auto_action_enabled.
        client_id (str): Идентификатор клиента Ozon API.
        seller_token (str): Токен продавца для аутентификации в Ozon API.

    Returns:
        dict: Ответ от API Ozon с результатом операции обновления цен.

    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса к API Ozon.
        requests.exceptions.RequestException: При проблемах с сетью или таймаутом.

    Examples:
        >>> update_price([{"offer_id": "123", "price": "5990", 
        ...                "currency_code": "RUB", "old_price": "0",
        ...                "auto_action_enabled": "UNKNOWN"}], 
        ...               "client123", "token456")
        {'result': [...]}

        >>> update_price([{"offer_id": "invalid", "price": "abc"}], 
        ...               "", "")
        # Вызовет HTTPError из-за неверных данных или учетных данных
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки.

    Args:
        stocks (list): Список словарей с данными об остатках товаров.
                        Каждый словарь должен содержать ключи: offer_id (артикул товара)
                        и stock (количество товара на складе).
        client_id (str): Идентификатор клиента Ozon API.
        seller_token (str): Токен продавца для аутентификации в Ozon API.

    Returns:
        dict : Ответ от API Ozon с результатом операции обновления остатков.

    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса к API Ozon.
        requests.exceptions.RequestException: При проблемах с сетью или таймаутом.

    Examples:
        >>> update_stocks([{"offer_id": "123", "stock": 10}], 
        ...                "client123", "token456")
        {'result': [...]}

        >>> update_stocks([{"offer_id": "invalid", "stock": "abc"}], 
        ...                "", "")
        # Вызовет HTTPError из-за неверных данных или учетных данных
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio.

    Returns:
        list: Список словарей с данными об остатках часов.
              Каждый словарь содержит информацию о товаре: код, количество, цену и др.

    Raises:
        requests.exceptions.RequestException: При проблемах с сетью или недоступности сайта.
        zipfile.BadZipFile: Если скачанный файл не является корректным ZIP-архивом.
        Exception: При ошибках чтения Excel-файла или проблемах с файловой системой.

    Examples:
        >>> download_stock()
        [{'Код': 'ABC123', 'Количество': '5', 'Цена': "5'990.00 руб.", ...}, ...]

        >>> # Если сайт timeworld.ru недоступен:
        # Вызовет requests.exceptions.ConnectionError
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать список остатков товаров для обновления в Ozon.

    Args:
        watch_remnants (list): Список словарей с данными о товарах из файла остатков.
                               Каждый словарь содержит поля: "Код", "Количество", "Цена".
        offer_ids (list): Список артикулов товаров, которые есть в магазине Ozon.

    Returns:
        list: Список словарей формата {"offer_id": артикул, "stock": количество}
              для обновления остатков в Ozon.

    Raises:
        ValueError: Если не удается преобразовать количество товара в число.
        KeyError: Если в данных товара отсутствуют необходимые ключи.

    Examples:
        >>> create_stocks([{'Код': '123', 'Количество': '5'}], ['123', '456'])
        [{'offer_id': '123', 'stock': 5}, {'offer_id': '456', 'stock': 0}]

        >>> create_stocks([{'Код': '999', 'Количество': 'нет'}], ['999'])
        # Вызовет ValueError при попытке преобразования 'нет' в число
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен для обновления в Ozon.

    Args:
        watch_remnants (list): Список словарей с данными о товарах из файла остатков.
                               Каждый словарь содержит поля: "Код", "Количество", "Цена".
        offer_ids (list): Список артикулов товаров, которые есть в магазине Ozon.

    Returns:
        list: Создает список словарей для отправки в API Ozon. Каждый словарь содержит поля: 
                               - auto_action_enabled: статус автоматических акций (всегда "UNKNOWN")
                               - currency_code: валюта (всегда "RUB")
                               - offer_id: артикул
                               - old_price: старая цена (всегда "0")
                               - price: преобразованная в нужный формат цена

    Raises:
        KeyError: Если в данных товара отсутствуют необходимые ключи ("Код" или "Цена").
        TypeError: Если аргументы имеют неверный тип (например, watch_remnants не список).

    Examples:
        >>> create_prices([{'Код': '123', 'Цена': "5'990.00 руб."}], ['123', '456'])
        [{
            'auto_action_enabled': 'UNKNOWN',
            'currency_code': 'RUB', 
            'offer_id': '123',
            'old_price': '0',
            'price': '5990'
        }]

        >>> create_prices([{'Код': '999', 'Цена': 'неизвестно'}], ['999'])
        # price_conversion('неизвестно') вернет '', цена будет пустой строкой
        [{
            'auto_action_enabled': 'UNKNOWN',
            'currency_code': 'RUB',
            'offer_id': '999',
            'old_price': '0',
            'price': ''
        }]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует цену в нужный формат.

    Args:
        price (str): Подгружается в виде строки с ценой в формате X'XXX.XX руб.

    Returns:
        str: Функция возвращает преобразованную в нужный формат цены, целое число без разделителей, пробелов и обозначения валюты.

    Raises:
        Функция не вызывает исключений.

    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'

        >>> price_conversion("")
        ''

    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список на части заданного размера.

    Args:
        lst (list): Исходный список для разделения.
        n (int): Размер каждой части (количество элементов в части).

    Returns:
        generator: Генератор, который последовательно возвращает части списка.
                   Каждая часть - это список из n элементов (или меньше для последней части).

    Raises:
        TypeError: Если lst не является списком или n не является целым числом.
        ValueError: Если n <= 0.

    Examples:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]

        >>> list(divide([1, 2, 3], 5))
        [[1, 2, 3]]

        >>> list(divide([], 2))
        []

        >>> list(divide([1, 2, 3], 0))
        # Вызовет ValueError: n должно быть больше 0
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция для запуска процесса синхронизации остатков и цен.

    Функция выполняет следующие шаги:
    1. Загружает учетные данные из переменных окружения
    2. Получает список артикулов товаров с Ozon
    3. Скачивает актуальные остатки и цены с сайта поставщика
    4. Обновляет остатки товаров на Ozon
    5. Обновляет цены товаров на Ozon

    Raises:
        requests.exceptions.ReadTimeout: При превышении времени ожидания ответа от API.
        requests.exceptions.ConnectionError: При проблемах с интернет-соединением.
        Exception: При любых других ошибках выполнения.

    Examples:
        >>> main()
        # Запускает процесс синхронизации, выводит сообщения об ошибках при их возникновении

        >>> # Если отсутствуют переменные окружения SELLER_TOKEN или CLIENT_ID:
        # Вызовет исключение при попытке получить значения из env
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
