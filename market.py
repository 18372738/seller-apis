import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров магазина на Яндекс.Маркет.

    Args:
        page(str) - Идентификатор страницы c результатами.
        campaign_id(str) - Идентификатор компании.
        access_token (str) - Токен доступа.

    Returns:
        list: Список товаров компании.

    Examples:
        >>> get_product_list("page", "campaign", "access_token")
        {
            "offerIds": ["string"],
            "cardStatuses": ["HAS_CARD_CAN_NOT_UPDATE"],
            "categoryIds": [0],
            "vendorNames": ["string"],
            "tags": ["string"],
            "archived": false
        }

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки во время запроса.
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
    """Обновить остатки товаров на Яндекс.Маркет.

    Args:
        stocks(list) - Список информации об остатках товаров.
        campaign_id(str) - Идентификатор компании.
        access_token(str) - Токен доступа.

    Returns:
        dict: Результат операции.

    Examples:
        >>> update_stocks("stoks", "campaign", "access_token")
        [{
            "sku": "string",
            "warehouseId": 0,
            "items": [{
                "count": 0,
                "type": "FIT",
                "updatedAt": "2022-12-29T18:02:01Z"
                }]
        }]

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.
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
    """Обновить цены товаров на Яндекс.Маркет.

    Args:
        prices(list) - Список цен на товары.
        campaign_id(str) - Идентификатор компании.
        access_token(str) - Токен доступа.

    Returns:
        dict: Результат операции.

    Examples:
        >>> update_price(prices, "campaign123", "access_token123")
        {"status": "OK"}

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.
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
    """Получить артикулы товаров Яндекс маркета

    Args:
        campaign_id(str) - Идентификатор компании.
        market_token (str) - Токен доступа.

    Returns:
        offer_ids(list): Список артикулов товаров.

    Examples:
        >>> get_offer_ids("campaign123", "market_token123")
        ['offer1', 'offer2']

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.
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
    """Создать список остатков товаров на Яндекс.Маркете.

    Args:
        watch_remnants(list) - Список остатков товаров.
        offer_ids(list) - Список артикулов товаров.
        warehouse_id(str) - Идентификатор склада.

    Returns:
        stoks(list): Список информации об остатках товаров.

    Examples:
        >>> create_stocks(watch_remnants, offer_ids, 'warehouse_id')
        {"warehouseId": 0, "skus": ["string"]}
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
    """Создать список цен на товары на Яндекс.Маркете.

    Args:
        watch_remnants(list) - Список остатков товаров.
        offer_ids(list) - Список артикулов товаров.

    Returns:
        prices(list): Список цен на товары.

    Examples:
        >>> create_prices((watch_remnants, offer_ids))
        [{
            "offerId": "string",
            "price": {
                "value": 500,
                "currencyId": "RUR",
                "discountBase": 600
                }
        }]
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
    """Загрузить цены на товары на Яндекс.Маркет.

    Args:
        watch_remnants(list[dict]) - Список остатков товаров с информацией о ценах.
        campaign_id (str - Идентификатор кампании.
        market_token (str) - Токен доступа.

    Returns:
        list: Список цен на товары с указанием валюты,
        загруженных на Яндекс.Маркет.

    Examples:
        >>> upload_prices(watch_remnants, 'campaign_id', 'market_token')
        [{
            "offerId": "string",
            "price": {
                "value": 500,
                "currencyId": "RUR",
                "discountBase": 600
                }
        }]
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загрузить информацию об остатках товаров на Яндекс.Маркет.

    Args:
        watch_remnants(list) - Список остатков товаров.
        campaign_id(str) - Идентификатор кампании.
        market_token(str) - Токен доступа.
        warehouse_id(str) - Идентификатор склада.

    Returns:
        tuple: not_empty(не пустые остатки) и stocks(все остатки)

    Examples:
        >>> upload_stocks(watch_remnants, "campaign_id", "market_token", "warehouse_id")
        [{
            "sku": "string",
            "warehouseId": 0,
            "items": [{
                "count": 0,
                "type": "FIT",
                "updatedAt": "2022-12-29T18:02:01Z"
                }]
        }]
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
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
