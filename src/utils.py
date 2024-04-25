from typing import List

import psycopg2
from config import HOST, USER, PASSWORD, DATABASE


def get_categories(child_id: int) -> List[str]:
    try:
        conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)

        cur = conn.cursor()

        def get_parents(child_id: int, annotation=None) -> List[str]:
            if annotation is None:
                annotation = []

            cur.execute(
                """
                SELECT parent_id, child_annotation FROM public.category
                WHERE child_id = %s """,
                (child_id,),
            )
            fetch_res = cur.fetchall()

            if fetch_res:
                result_list = fetch_res[0]
                parent_id = result_list[0]
                child_annotation = result_list[1]

                if parent_id == 0:
                    annotation.append(child_annotation)
                    return annotation
                else:
                    annotation.append(child_annotation)
                    return get_parents(parent_id, annotation)
            else:
                return annotation

        return get_parents(child_id)

    except Exception as e:
        print(f"Error: {e}")
        conn.close()

    finally:
        cur.close()
        conn.close()


def parse_categories(category_id: int) -> dict:
    categories = get_categories(category_id)[::-1]
    total_categories = len(categories)
    categories_levels = {
        "category_lvl_1": categories[0] if total_categories else None,
        "category_lvl_2": categories[1] if total_categories > 2 else None,
        "category_lvl_3": categories[3] if total_categories > 3 else None,
        "category_remaining": "/".join(categories[4:]) if total_categories > 4 else None,
    }
    return categories_levels


# print(get_mapping(91499))
