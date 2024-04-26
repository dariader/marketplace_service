# Marketplace service

Data:  http://export.admitad.com/ru/webmaster/websites/777011/products/export_adv_products/?user=bloggers_style&code=uzztv9z1ss&feed_id=21908&format=xml 

Size: 5Gb

**How to run:**

Download repository

Run `sudo docker-compose up --build`

This will setup postgres and docker container to insert data.
Data will also be downloaded. 


Schema: 
```
create table public.sku
(
    uuid                   uuid,
    marketplace_id         integer
    product_id             bigint,
    title                  text,
    description            text,
    brand                  integer,
    seller_id              integer,
    seller_name            text,
    first_image_url        text,
    category_id            integer,
    category_lvl_1         text,
    category_lvl_2         text,
    category_lvl_3         text,
    category_remaining     text,
    features               json,
    rating_count           integer,
    rating_value           double precision,
    price_before_discounts real,
    discount               double precision,
    price_after_discounts  real,
    bonuses                integer,
    sales                  integer,
    inserted_at            timestamp default now(),
    updated_at             timestamp default now(),
    currency               text,
    barcode                bigint
);
```

### Mappings: 

Category levels are retrieved from mapping table with schema: 
```
create table public.category
( child_id INTEGER,
 parent_id INTEGER,
 child_annotation TEXT )
```

Vendors ids are generated by insertion into vendor table with schema (so we do not have to iterate over all 
data multiple times)

```
create table public.vendor
( vendor_id SERIAL PRIMARY KEY,
vendor_name TEXT UNIQUE ) 
```

Notes regarding fields: 
1. To be fixed: `features` field must be valid json. During iterations on the dataset it should be updated with `.update` dict method. Troubles with records like "{\"Цвет\": \"черный\"}"
2. Could not find `rating_count`, `rating_value` information in the table
3. `discount` value can be measured in absolute values or in percents. Some values are negative (price_after_discount is larger that price_before_discount). Bug?
4. Could not find `bonuses` and `sales` fields also. Should they be derived from description?
5. Field with barcode is different from what is set in the schema. In some cases it may contain a set of values, meaning the type of field is bigint[] not bigint. There's no solutions which will preserve both schema and data, so I decided to change bigint to bigint[]. Motivation - it is not clear which relationship this field holds with product id, and it is not clear how this filed is parsed in the consequents steps.

Notes regarding SQL expressions:
1. To be fixed: Expression for generation of id of venfor and seller must utilise RETURNING statement.

Notes regarding code
1. SQL expressions should be organised in separate directory, imo
2. configurations are stored in config.py, which I guess is ok for a project
3. some tests will be useful ( I left small dataset - temp_data.xml - for the purpose of testing)
4. linters could be stricter :-)

QC procedures:
1. `barcode` length < 15 numbers (AFAIK typical barcode is 12-13 chars long)
2. `features` field is restricted to 1000 characters, else {"error": "Parsing error"}
3. changed text type to varchar to control the size of the data