import os
from src.create_category_tables import generate_mapping
from src.create_offer_tables import generate_offers


def process_data(file: str) -> None:
    generate_mapping(filename=file)
    generate_offers(filename=file)


if __name__ == "__main__":
    file = os.path.join("data", "data.xml")
    process_data(file=file)
