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
            """
            DROP TABLE IF EXISTS public.sku;
            CREATE TABLE IF NOT EXISTS public.sku (
                uuid UUID,
                marketplace_id INTEGER,
                product_id BIGINT,
                title VARCHAR(1000),
                description VARCHAR(10000),
                brand INTEGER,
                seller_id INTEGER,
                seller_name VARCHAR(100),
                first_image_url VARCHAR(1000),
                category_id INTEGER,
                category_lvl_1 VARCHAR(100),
                category_lvl_2 VARCHAR(100),
                category_lvl_3 VARCHAR(100),
                category_remaining VARCHAR(100),
                features JSON,
                rating_count INTEGER,
                rating_value DOUBLE PRECISION,
                price_before_discounts REAL,
                discount DOUBLE PRECISION,
                price_after_discounts REAL,
                bonuses INTEGER,
                sales INTEGER,
                inserted_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                currency VARCHAR(10),
                barcode BIGINT[]
            );
            CREATE INDEX IF NOT EXISTS sku_brand_index ON public.sku (brand);
            CREATE UNIQUE INDEX IF NOT EXISTS sku_marketplace_id_sku_id_uindex
            ON public.sku (marketplace_id, product_id);
            CREATE UNIQUE INDEX IF NOT EXISTS sku_uuid_uindex ON public.sku (uuid);""".strip()
        )

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
                barcode = [int(i) if len(i) < 15 else None for i in elem.itertext()]
                dct["barcode"] = barcode
            elif elem.tag == "description" and dct.get("product_id"):
                dct["description"] = elem.text[:10000] if elem.text else None
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
                dct["discount"] = (
                    round(dct.get("price_before_discounts") - dct.get("price_after_discounts"), 2)
                    if (dct.get("price_before_discounts") and dct.get("price_after_discounts"))
                    else None
                )
                dct["uuid"] = uuid.uuid4().hex
                dct["features"] = json.dumps(dct.get("features", {}), ensure_ascii=False)
                dct["features"] = None if len(dct["features"]) > 1000 else dct["features"]
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
                conn.commit()
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
