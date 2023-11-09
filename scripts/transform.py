import os
import csv

from common.tables import PprRawAll
from common.base import session
from datetime import datetime
from sqlalchemy import text

# Settings
BASE_PATH = os.path.abspath(__file__ + "/../../")

# Get the current date (in the format YYYY-MM-DD)
CURRENT_DATE = datetime.now().date()

# Raw path where we want to extract the new CSV data
RAW_PATH = f"{BASE_PATH}/data/raw/downloaded_at={CURRENT_DATE}/ppr-all.csv"


def transform_to_lowercase(input_string):
    """
    Lowercase string fields
    """
    return input_string.lower()


def update_date_of_sale(date_input):
    """
    Update date format from DD/MM/YYYY to YYYY-MM-DD
    """
    current_format = datetime.strptime(date_input, "%d/%m/%Y")
    new_format = current_format.strftime("%Y-%m-%d")
    return new_format


def update_description(description_input):
    """
    Simplify the description field for potentialy future analysis, just return:
    - "new" if string contains "new" or "nua" substring
    - "second-hand" if string contains "second-hand" or "atháime" substring
    """
    description_input = transform_to_lowercase(description_input)
    if "new" or "nua" in description_input:
        return "new"
    elif "second-hand" or "atháimhe" in description_input:
        return "second-hand"
    return description_input


def update_price(price_input):
    """
    Return price as integer by removing:
    - "€" symbol
    - "," comma
    """
    price_input = price_input.replace("€", "")
    price_input = price_input.replace(",", "")
    price_input = float(price_input)
    return int(price_input)


def truncate_table():
    """
    Ensure that "ppr_raw_all" table is always in empty state before running any transformations.
    And primary key (id) restarts from 1.
    """
    print("[Transform] Truncating ppr_raw_all table and restarting the sequence")

    try:
        session.execute(
            text("TRUNCATE TABLE ppr_raw_all;ALTER SEQUENCE ppr_raw_all_id_seq RESTART;")
        )
        session.commit()
        print("[Transform] Table truncated and sequence restarted successfully.")
    except Exception as e:
        print(f"[Transform] Error truncating table or restarting the sequence: {str(e)}")


def transform_new_data():
    """
    Apply transformations to each row in the .csv file before saving it into the database.

    This function reads a CSV file containing new data, applies a series of transformations
    to each row, and saves the transformed data as PprRawAll objects in the database.

    The transformations include updating the date of sale, converting the address, postal code,
    and county to lowercase, updating the price, and updating the property description.
    """
    
    print("[Transform] Transform new data available in ppr_raw_all table")

    with open(RAW_PATH, mode="r", encoding="windows-1252") as csv_file:
        # Read the new CSV snapshot ready to be processed
        reader = csv.DictReader(csv_file)
        # Initialize an empty list for our PprRawAll objects
        ppr_raw_objects = []
        for row in reader:
            # Apply transformations and save as PprRawAll object
            ppr_raw_objects.append(
                PprRawAll(
                    date_of_sale=update_date_of_sale(row["date_of_sale"]),
                    address=transform_to_lowercase(row["address"]),
                    postal_code=transform_to_lowercase(row["postal_code"]),
                    county=transform_to_lowercase(row["county"]),
                    price=update_price(row["price"]),
                    description=update_description(row["description"]),
                )
            )
        # Save all new processed objects and commit
        try:
            print("[Transform] Bulk saving new data available in ppr_raw_all table")
            session.bulk_save_objects(ppr_raw_objects)
            session.commit()
            print("[Transform] Data saved successfully.")
        except Exception as e:
            print(f"[Transform] Error saving data to the database: {str(e)}")


def main():
    print("[Transform] Start")
    truncate_table()
    transform_new_data()
    print("[Transform] End")
