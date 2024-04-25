"""

Get vendor info from data file
Retrieve everything that relates to category

"""

import psycopg2

from config import HOST, USER, PASSWORD, DATABASE


def insert_or_get_vendor_id(vendor_name: str) -> int:
    """
    Todo: rewrite with RETURNING
    :param vendor_name:
    :param conn:
    :return:
    """
    try:
        conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
        cur = conn.cursor()
        cur.execute(
            """
                      CREATE TABLE IF NOT EXISTS public.vendor (
                          vendor_id SERIAL PRIMARY KEY,
                          vendor_name TEXT UNIQUE
                      )
                  """
        )
        cur = conn.cursor()
        # Check if vendor exists
        cur.execute(
            """
            SELECT vendor_id FROM public.vendor WHERE vendor_name = %s
        """,
            (vendor_name,),
        )
        existing_vendor = cur.fetchone()

        if existing_vendor:
            vendor_id = existing_vendor[0]
            print(f"Vendor '{vendor_name}' already exists with ID: {vendor_id}")
            return vendor_id
        else:
            cur.execute(
                """
                INSERT INTO public.vendor (vendor_name)
                VALUES (%s)
                RETURNING vendor_id
            """,
                (vendor_name,),
            )
            conn.commit()
            new_vendor_id = cur.fetchone()[0]
            return new_vendor_id

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()
