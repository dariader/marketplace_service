"""

Get vendor info from data file
Retrieve everything that relates to category

"""

import psycopg2

from config import HOST, USER, PASSWORD, DATABASE


def insert_or_get_seller_id(seller_name: str) -> int:
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
                      CREATE TABLE IF NOT EXISTS public.seller (
                          seller_id SERIAL PRIMARY KEY,
                          seller_name TEXT UNIQUE
                      )
                  """
        )
        cur = conn.cursor()
        # Check if vendor exists
        cur.execute(
            """
            SELECT seller_id FROM public.seller WHERE seller_name = %s
        """,
            (seller_name,),
        )
        existing_seller = cur.fetchone()

        if existing_seller:
            seller_id = existing_seller[0]
            return seller_id
        else:
            cur.execute(
                """
                INSERT INTO public.seller (seller_name)
                VALUES (%s)
                RETURNING seller_id
            """,
                (seller_name,),
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
