"""

Get category info from data file
Retrieve everything that relates to category

"""

from xml.etree.ElementTree import iterparse

import psycopg2

from config import HOST, USER, PASSWORD, DATABASE


def generate_mapping(filename: str) -> None:
    try:
        conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)

        cur = conn.cursor()

        cur.execute(
            """
               CREATE TABLE IF NOT EXISTS public.category (
                   child_id INTEGER,
                   parent_id INTEGER,
                   child_annotation TEXT
               )
           """
        )

        for _, elem in iterparse(source=filename, events=("end",)):
            if elem.tag == "category":
                child_id = int(elem.attrib.get("id"))
                parent_id = int(elem.attrib.get("parentId", 0))
                child_annotation = elem.text
                cur.execute(
                    """INSERT INTO public.category
                       VALUES (%(child_id)s, %(parent_id)s, %(child_annotation)s)""",
                    {"child_id": child_id, "parent_id": parent_id, "child_annotation": child_annotation},
                )

        conn.commit()
        print("Category data inserted successfully!")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()
