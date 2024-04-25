import json
import uuid
from xml.etree.ElementTree import iterparse
import pandas as pd

offers_data = []
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
    "features": [],
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

dct = offer_info
all_tags = set()
for _, elem in iterparse(source="temp_data.xml", events=("end",)):
    offer_elem = elem
    if elem.tag == "offer":
        offer_id = elem.get("id")
        dct["product_id"] = offer_id
    elif elem.tag == "barcode" and dct.get("product_id"):
        barcode = [int(i) for i in offer_elem.itertext() if i]
        dct["barcode"] = barcode
    elif elem.tag == "categoryId" and dct.get("product_id"):
        dct["category_id"] = int(offer_elem.text)
        # other_categories = parse_categories(dct['category_id'])
        # dct.update(other_categories)
    elif elem.tag == "currencyId" and dct.get("product_id"):
        dct["currency"] = offer_elem.text
    # elif elem.tag == 'group_id' and dct.get('product_id'):
    #     dct['group_id'] = [int(i) for i in offer_elem.itertext() if i]
    # elif elem.tag == 'modified_time' and dct.get('product_id'):
    #     dct['modified_time'] = offer_elem.text
    elif elem.tag == "name" and dct.get("product_id"):
        dct["title"] = offer_elem.text
    elif elem.tag == "price" and dct.get("product_id"):
        dct["price_after_discounts"] = float(offer_elem.text)  # Assuming price is numeric
    elif elem.tag == "oldprice" and dct.get("product_id"):
        dct["price_before_discounts"] = float(offer_elem.text)  # Assuming price is numeric
    # elif elem.tag == 'offers' and dct.get('product_id'):
    #     dct['offers'] = offer_elem.text
    elif elem.tag == "param" and dct.get("product_id"):
        dct["features"] = dct["features"].append([{offer_elem.get("name"): offer_elem.text}])
    elif elem.tag == "picture" and dct.get("product_id"):
        dct["first_image_url"] = offer_elem.text
    elif elem.tag == "vendor" and dct.get("product_id"):
        # dct['brand'] = offer_elem.text
        dct["discount"] = round(dct.get("price_before_discounts") - dct.get("price_after_discounts"), 2)
        dct["uuid"] = uuid.uuid4().hex
        dct["features"] = json.dumps(dct.get("features", {}), ensure_ascii=False)
        print(dct["features"])
        # insert(dct)
        # Append the dictionary to the list
        # offers_data.append(dct)
    else:
        continue
    elem.clear()
print(offers_data)
df = pd.DataFrame.from_records(offers_data)
df.to_csv("parsed_data.csv")
print(df, all_tags)
# print(all_tags)
