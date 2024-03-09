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
    """Получить список товаров магазина озон

    Args:
        last_id (str): Идентификатор последнего полученного товара.
        client_id (str): Идентификатор клиента.
        seller_token (str): API - токен продавца.

    Returns:
        list: Список товаров.

    Examples:
        >>> get_product_list("last123", "client123", "token123")
         {"result": {"items": [{
                "product_id": 223681945,
                "offer_id": "136748"
                }],
            "total": 1, "last_id": "bnVсbA=="}}

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.
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
    """Получить артикулы товаров магазина озон

    Args:
        client_id (str): Идентификатор клиента.
        seller_token (str): API - токен продавца.

    Returns:
        offer_ids(list): Список товаров.

    Examples:
        >>> get_offer_ids("client123", "token123")
        {"items": [{"product_id": 223681945, "offer_id": "136748"}],
                  [{"product_id": 223681946, "offer_id": "136749"}],
        }

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.
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
    """Обновить цены товаров
    Args:
        prices(list) - Список цен товаров
        client_id (str): Идентификатор клиента.
        seller_token (str): API - токен продавца.

    Returns:
        dict: Результат оперции.

    Examples:
        >>>update_price(prices, "client123", "token123")
        {"result":[{"product_id": 1386,
            "offer_id": "PH8865",
            "updated": true,
            "errors": [ ]
            }]
        }

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.
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
    """Обновить остатки

    Args:
        stocks(list) - Список цен товаров
        client_id (str): Идентификатор клиента.
        seller_token (str): API - токен продавца.

    Returns:
        dict: Результат оперции.

    Examples:
        >>>update_stocks(stocks, "client123", "token123"))
        {"result": [{
            "product_id": 55946,
            "offer_id": "PG-2404С1",
            "updated": true,
            "errors": []
            }]
        }

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.
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
    """Скачать файл ostatki с сайта casio

    Returns:
        watch_remnants(list): Список остатков часов.

    Examples:
        >>> download_stock()
        [{'item': 'watch1', 'quantity': 10},
        {'item': 'watch2', 'quantity': 20}]
        
    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.
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
    """ Создать список оставшихся товаров
    Args:
        watch_remnants (list): Список остатков товаров.
        offer_ids (list): Список артикулов товаров.

    Returns:
        stoks(list): Список остатков товаров.

    Examples:
        >>> create_stocks(watch_remnants, offer_ids)
        {"stocks": [{
            "offer_id": "PH11042",
            "product_id": 313455276,
            "stock": 100,
            "warehouse_id": 22142605386000
            }]
        }

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
    """Создать список цен товаров

    Args:
        watch_remnants (list): Список остатков товаров.
        offer_ids (list): Список артикулов товаров.

    Returns:
        prices(list): Список остатков товаров.

    Examples:
        >>> create_prices(watch_remnants, offer_ids)
        {"prices": [{
            "auto_action_enabled": "UNKNOWN",
            "currency_code": "RUB",
            "min_price": "800",
            "offer_id": "",
            "old_price": "0",
            "price": "1448",
            "price_strategy_enabled": "UNKNOWN",
            "product_id": 1386
            }]
        }

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
    """Преобразовать цену. Пример: 5'990.00 руб. -> 5990

    Args:
        price (str): цена в формате строки

    Returns:
        str: значение в виде строки юез знака "руб." и разделителя апострофа.

    Examples:
        >>> price_conversion("5'990.00 руб.")
        "5990"

    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов
    Args:
        lst (list): Список элементов.
        n (int): Размер части списка.

    Returns:
        list: Список элементов, разделенный на части.

    Examples:
    >>> list(divide([1, 2, 3, 4, 5], 2)
    [[1, 2], [3, 4], [5]]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загрузить цену товаров на маркетплейсе
    Args:
        watch_remnants (list): Список остатков часов.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Returns:
        list: Список цен товаров с указанием валюты,
        загруженных на маркетплейс.

    Examples:
        >>> upload_prices(watch_remnants, client_id, seller_token)
        [{"auto_action_enabled": "UNKNOWN",
        "currency_code": "RUB",
        "min_price": "800",
        "offer_id": "",
        "old_price": "0",
        "price": "1448",
        "price_strategy_enabled": "UNKNOWN",
        "product_id": 1386
        }]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загрузить остатки товаров на маркетплейс.

    Args:
        watch_remnants (list): Список остатков товаров.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Returns:
        tuple: not_empty(не пустые остатки) и stocks(все остатки).

    Examples:
        >>> upload_stocks(watch_remnants, client_id, seller_token)
        [{"offer_id": "PH11042",
          "product_id": 313455276,
          "stock": 100,
          "warehouse_id": 22142605386000
        }]

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
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
