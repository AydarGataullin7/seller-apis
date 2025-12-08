import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
       """Получить список товаров магазина на Яндекс.Маркете.

    Args:
        page (str): Чтобы загружать товары частями. "" - начать с начала, иначе - продолжить с этого места.
        campaign_id (str): Уникальный идентификатор магазина (кампании) на Яндекс.Маркете.
        access_token (str): Токен доступа для аутентификации в API Яндекс.Маркета.

    Returns:

        dict: Ответ от Яндекс.Маркета с товарами и информацией для продолжения загрузки.

    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса (неверный токен, не найден магазин и т.д.).
        requests.exceptions.RequestException: При проблемах с сетью, таймаутах.

    Examples:
        >>> get_product_list("", "campaign_123", "token_abc")
        {
            "offerMappingEntries": [...],
            "paging": {"nextPageToken": "next_page_token"}
        }

        >>> get_product_list("invalid_token", "wrong_campaign", "bad_token")
        # Вызовет HTTPError с кодом 401 или 404
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Выполняет обновление остатков на Яндекс.Маркет.

    Args:
        stocks (list): Список остатков для обновления. Каждый элемент должен содержать:
                        - sku: артикул товара
                        - warehouseId: идентификатор склада на Яндекс.Маркете
                        - items: список с одним элементом:
                            {
                                "count": количество товара,
                                "type": "FIT" (готов к отгрузке),
                                "updatedAt": дата обновления
                            }
        campaign_id (str): Уникальный идентификатор магазина на Яндекс.Маркете.
        access_token (str): Токен доступа для аутентификации в API Яндекс.Маркета.

    Returns:
        dict: Ответ от API Яндекс.Маркет с результатом операции обновления остатков.

    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса.
        requests.exceptions.RequestException: При проблемах с сетью или таймаутах.

    Examples:
        >>> update_stocks([{
        ...     "sku": "12345",
        ...     "warehouseId": "warehouse_1",
        ...     "items": [{"count": 10, "type": "FIT", "updatedAt": "2024-01-15T10:30:00Z"}]
        ... }], "campaign_123", "token_abc")
        {'status': 'OK'}

        >>> update_stocks([], "invalid_id", "bad_token")
        # Вызовет HTTPError с кодом 401 (неавторизован) или 404 (не найден)
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Выполняет обновление цен на Яндекс.Маркет.

    Args:
        prices (list): Список цен для обновления. Каждый элемент должен содержать:
                        - id: артикул товара (строка)
                        - price: словарь с данными о цене:
                            {
                                "value": числовое значение цены (целое число),
                                "currencyId": валюта (всегда "RUR")
                            }
        campaign_id (str): Уникальный идентификатор магазина на Яндекс.Маркете.
        access_token (str): Токен доступа для аутентификации в API Яндекс.Маркета.

    Returns:
        dict: Ответ от API Яндекс.Маркет с результатом операции обновления цен.

    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса.
        requests.exceptions.RequestException: При проблемах с сетью или таймаутах.

    Examples:
        >>> update_price([{
        ...     "id": "12345",
        ...     "price": {
        ...         "value": 5990,
        ...         "currencyId": "RUR"
        ...     }
        ... }], "campaign_123", "token_abc")
        {'status': 'OK'}

        >>> update_price([{
        ...     "id": "неверный_артикул",
        ...     "price": {
        ...         "value": "не_число",
        ...         "currencyId": "WRONG"
        ...     }
        ... }], "invalid", "bad_token")
        # Вызовет HTTPError из-за неверного формата данных
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета.

    Args:
        campaign_id (str): Уникальный идентификатор магазина на Яндекс.Маркете.
        market_token (str): Уникальный ключ доступа для API.

    Returns:
        list: Список артикулов (shopSku) всех товаров магазина на Яндекс.Маркете.

    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса.
        requests.exceptions.RequestException: При проблемах с сетью или таймаутах.

    Examples:
        >>> get_offer_ids("campaign_123", "token_abc")
        ['12345', '67890', '54321']

        >>> get_offer_ids("invalid", "bad_token")
        # Вызовет HTTPError с кодом 401 (неавторизован) или 404 (магазин не найден)
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):    
    """Создание списка с данными об остатках.

    Args:
        watch_remnants (list): Список остатков на сайте поставщика.
        offer_ids (list): Список артикулов (shopSku) всех товаров магазина на Яндекс.Маркете.
        warehouse_id (str): ID склада на Яндекс.Маркете.

    Returns:
        list: Список словарей с данными об остатках. Каждый словарь содержит:
              - sku (str): артикул товара
              - warehouseId (str): ID склада
              - items (list): список с одним словарем:
                  {
                      "count" (int): количество товара,
                      "type" (str): тип товара (всегда "FIT" - готов к отгрузке),
                      "updatedAt" (str): дата в формате ISO (например "2024-01-15T10:30:00Z")
                  }

    Raises:
        ValueError: Если не удается преобразовать количество товара в число.
        KeyError: Если в данных товара отсутствуют ключи "Код" или "Количество".

    Examples:
        >>> create_stocks([{'Код': '123', 'Количество': '5'}], ['123', '456'], "warehouse_fbs")
        [{
            "sku": "123",
            "warehouseId": "warehouse_fbs",
            "items": [{"count": 5, "type": "FIT", "updatedAt": "2024-01-15T10:30:00Z"}]
        },
        {
            "sku": "456",
            "warehouseId": "warehouse_fbs",
            "items": [{"count": 0, "type": "FIT", "updatedAt": "2024-01-15T10:30:00Z"}]
        }]

        >>> create_stocks([{'Код': '999', 'Количество': 'не число'}], ['999'], "warehouse_fbs")
        # Вызовет ValueError при попытке преобразовать 'не число' в int
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список с данными о ценах.

    Args:
        watch_remnants (list): Список остатков на сайте поставщика.
        offer_ids (list): Список артикулов (shopSku) всех товаров магазина на Яндекс.Маркете.

    Returns:
        list: Список словарей с данными о ценах. Каждый словарь содержит:
              - id (str): артикул товара
              - price (dict): словарь с данными о цене:
                  {
                      "value" (int): цена товара (целое число),
                      "currencyId" (str): валюта (всегда "RUR")
                  }

    Raises:
        ValueError: Если не удается преобразовать цену товара в число.
        KeyError: Если в данных товара отсутствуют ключи "Код" или "Цена".

    Examples:
        >>> create_prices([{'Код': '123', 'Цена': "5'990.00 руб."}], ['123'])
        [{
            "id": "123",
            "price": {
                "value": 5990,
                "currencyId": "RUR"
            }
        }]

        >>> create_prices([{'Код': '999', 'Цена': 'некорректная'}], ['999'])
        # price_conversion('некорректная') вернет '',
        # int('') вызовет ValueError
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Основная функция для синхронизации остатков и цен с Яндекс.Маркетом.

    Функция выполняет полный цикл обновления данных для двух схем работы:
    1. FBS (Fulfillment By Seller) - продавец хранит и отправляет товары
    2. DBS (Delivery By Seller) - продавец хранит, Яндекс доставляет

    Для каждой схемы выполняются следующие шаги:
    - Получение списка артикулов товаров магазина
    - Создание данных об остатках с учетом склада
    - Обновление остатков на Яндекс.Маркете
    - Обновление цен на Яндекс.Маркете

    Все настройки загружаются из переменных окружения:
    - MARKET_TOKEN: токен доступа к API Яндекс.Маркета
    - FBS_ID: идентификатор магазина FBS
    - DBS_ID: идентификатор магазина DBS
    - WAREHOUSE_FBS_ID: идентификатор склада FBS
    - WAREHOUSE_DBS_ID: идентификатор склада DBS

    Raises:
        requests.exceptions.ReadTimeout: При превышении времени ожидания ответа от API.
        requests.exceptions.ConnectionError: При проблемах с интернет-соединением.
        Exception: При любых других ошибках выполнения.

    Examples:
        >>> main()
        # Запускает процесс синхронизации для FBS и DBS магазинов
        
        >>> # Если отсутствуют необходимые переменные окружения:
        # Вызовет исключение при попытке получить значения из env
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
