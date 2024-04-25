"""

Get offer info from data file
Insert into offer table

"""

import json
import uuid
from xml.etree.ElementTree import iterparse

import psycopg2

from config import HOST, USER, PASSWORD, DATABASE
from src.create_seller_table import insert_or_get_seller_id
from src.create_vendor_table import insert_or_get_vendor_id
from src.utils import parse_categories

offer_info = {
    "uuid": None,
    "marketplace_id": None,
    "product_id": None,
    "title": None,
    "description": None,
    "brand": None,
    "seller_id": None,
    "seller_name": None,
    "first_image_url": None,
    "category_id": None,
    "category_lvl_1": None,
    "category_lvl_2": None,
    "category_lvl_3": None,
    "category_remaining": None,
    "features": dict(),
    "rating_count": None,
    "rating_value": None,
    "price_before_discounts": None,
    "discount": None,
    "price_after_discounts": None,
    "bonuses": None,
    "sales": None,
    "inserted_at": None,
    "updated_at": None,
    "currency": None,
    "barcode": None,
}


def generate_offers(filename: str) -> None:
    try:
        conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)

        cur = conn.cursor()

        cur.execute(
            """CREATE TABLE IF NOT EXISTS public.sku ( uuid UUID, marketplace_id INTEGER, product_id BIGINT,
            title TEXT, description TEXT, brand INTEGER, seller_id INTEGER, seller_name TEXT, first_image_url TEXT,
            category_id INTEGER, category_lvl_1 TEXT, category_lvl_2 TEXT, category_lvl_3 TEXT, category_remaining
            TEXT, features JSON, rating_count INTEGER, rating_value DOUBLE PRECISION, price_before_discounts REAL,
            discount DOUBLE PRECISION, price_after_discounts REAL, bonuses INTEGER, sales INTEGER, inserted_at
            TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW(), currency TEXT, barcode BIGINT[] ) ; comment
            on column public.sku.uuid is 'id товара в нашей бд'; comment on column public.sku.marketplace_id is 'id
            маркетплейса'; comment on column public.sku.product_id is 'id товара в маркетплейсе'; comment on column
            public.sku.title is 'название товара'; comment on column public.sku.description is 'описание товара';
            comment on column public.sku.category_lvl_1 is 'Первая часть категории товара. Например, для товара,
            находящегося по пути Детям/Электроника/Детская электроника/Игровая консоль/Игровые консоли и игры/Игровые
            консоли, в это поле запишется "Детям".'; comment on column public.sku.category_lvl_2 is 'Вторая часть
            категории товара. Например, для товара, находящегося по пути Детям/Электроника/Детская
            электроника/Игровая консоль/Игровые консоли и игры/Игровые консоли, в это поле запишется "Электроника".';
            comment on column public.sku.category_lvl_3 is 'Третья часть категории товара. Например, для товара,
            находящегося по пути Детям/Электроника/Детская электроника/Игровая консоль/Игровые консоли и игры/Игровые
            консоли, в это поле запишется "Детская электроника".'; comment on column public.sku.category_remaining is
            'Остаток категории товара. Например, для товара, находящегося по пути Детям/Электроника/Детская
            электроника/Игровая консоль/Игровые консоли и игры/Игровые консоли, в это поле запишется "Игровая
            консоль/Игровые консоли и игры/Игровые консоли".'; comment on column public.sku.features is
            'Характеристики товара'; comment on column public.sku.rating_count is 'Кол-во отзывов о товаре'; comment
            on column public.sku.rating_value is 'Рейтинг товара (0-5)'; comment on column public.sku.barcode is
            'Штрихкод'; create index sku_brand_index on public.sku (brand); create unique index
            sku_marketplace_id_sku_id_uindex on public.sku (marketplace_id, product_id); create unique index
            sku_uuid_uindex on public.sku (uuid);"""
        )
        counter = 0
        dct = offer_info
        seller_name = None
        seller_id = None
        for _, elem in iterparse(source=filename, events=("end",)):
            if elem.tag == "company":
                seller_name = elem.text  # it is a single value, Yandex
                seller_id = insert_or_get_seller_id(elem.text)
            elif elem.tag == "offer":
                offer_id = elem.get("id")
                dct["product_id"] = offer_id
            elif elem.tag == "barcode" and dct.get("product_id"):
                barcode = [int(i) for i in elem.itertext() if i]
                dct["barcode"] = barcode
            elif elem.tag == "description" and dct.get("product_id"):
                dct["description"] = elem.text
            elif elem.tag == "categoryId" and dct.get("product_id"):
                dct["category_id"] = int(elem.text)
                other_categories = parse_categories(dct["category_id"])
                dct.update(other_categories)
            elif elem.tag == "currencyId" and dct.get("product_id"):
                dct["currency"] = elem.text
            elif elem.tag == "name" and dct.get("product_id"):
                dct["title"] = elem.text
            elif elem.tag == "price" and dct.get("product_id"):
                dct["price_after_discounts"] = float(elem.text)
            elif elem.tag == "oldprice" and dct.get("product_id"):
                dct["price_before_discounts"] = float(elem.text)
            elif elem.tag == "param" and dct.get("product_id"):
                dct["features"] = {elem.get("name"): elem.text}
            elif elem.tag == "picture" and dct.get("product_id"):
                dct["first_image_url"] = elem.text
            elif elem.tag == "vendor" and dct.get("product_id"):
                dct["brand"] = insert_or_get_vendor_id(elem.text)
                dct["discount"] = round(dct.get("price_before_discounts") - dct.get("price_after_discounts"), 2)
                dct["uuid"] = uuid.uuid4().hex
                dct["features"] = json.dumps(dct.get("features", {}), ensure_ascii=False)
                dct["seller_name"] = seller_name
                dct["seller_id"] = seller_id
                insert_query = """
                        INSERT INTO public.sku (
                            uuid, marketplace_id, product_id, title, description, brand, seller_id, seller_name,
                            first_image_url, category_id, category_lvl_1, category_lvl_2, category_lvl_3,
                            category_remaining, features, rating_count, rating_value, price_before_discounts,
                            discount, price_after_discounts, bonuses, sales, currency, barcode
                        ) VALUES (
                            %(uuid)s, %(marketplace_id)s, %(product_id)s, %(title)s, %(description)s, %(brand)s,
                            %(seller_id)s, %(seller_name)s, %(first_image_url)s, %(category_id)s, %(category_lvl_1)s,
                            %(category_lvl_2)s, %(category_lvl_3)s, %(category_remaining)s, %(features)s,
                            %(rating_count)s, %(rating_value)s, %(price_before_discounts)s, %(discount)s,
                            %(price_after_discounts)s, %(bonuses)s, %(sales)s, %(currency)s, %(barcode)s)"""
                cur.execute(insert_query, dct)
                counter += 1
                if counter == 10:
                    conn.commit()
                    counter = 0
                dct = offer_info
            else:
                continue
            elem.clear()
        conn.commit()
        print("Offer data inserted successfully!")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()
