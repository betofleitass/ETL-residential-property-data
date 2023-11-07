import datetime
import csv
import os
import tempfile
import requests
from zipfile import ZipFile

# Settings
base_path = os.path.abspath(__file__ + "/../../")

# External website file url
source_url = "https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip"

# Get the current date (in the format YYYY-MM-DD)
current_date = datetime.datetime.now().date()

# Source path where we want to save the .zip file downloaded from the website
source_path = f"{base_path}/data/source/downloaded_at={current_date}/PPR-ALL.zip"

# Raw path where we want to extract the new .csv data
raw_path = f"{base_path}/data/raw/downloaded_at={current_date}/ppr-all.csv"


def create_folder_if_not_exists(path):
    """
    Create a new folder if it doesn't exists
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)


def download_snapshot():
    """
    Download the new dataset from the source only if it does not exist
    """
    if not os.path.exists(source_path):
        print("[Extract] Downloading snapshot")
        create_folder_if_not_exists(source_path)
        with open(source_path, "wb") as source_ppr:
            response = requests.get(source_url, verify=False)
            source_ppr.write(response.content)
    else:
        print("[Extract] File already exists. Skipping the download.")


def save_new_raw_data():
    """
    Save new raw data from the source
    """

    if not os.path.exists(raw_path):
        print(f"[Extract] Saving data from '{source_path}' to '{raw_path}'")

        create_folder_if_not_exists(raw_path)
    
        with tempfile.TemporaryDirectory() as dirpath:
            with ZipFile(
                source_path,
                "r",
            ) as zipfile:
                names_list = zipfile.namelist()
                csv_file_path = zipfile.extract(names_list[0], path=dirpath)
                # Open the CSV file in read mode
                with open(csv_file_path, mode="r", encoding="windows-1252") as csv_file:
                    reader = csv.DictReader(csv_file)

                    row = next(reader)  # Get first row from reader
                    print("[Extract] First row example:", row)

                    # Open the CSV file in write mode
                    with open(raw_path, mode="w", encoding="windows-1252") as csv_file:
                        # Rename field names so they're ready for the next step
                        fieldnames = {
                            "Date of Sale (dd/mm/yyyy)": "date_of_sale",
                            "Address": "address",
                            "Postal Code": "postal_code",
                            "County": "county",
                            "Eircode": "eircode",
                            "Price (€)": "price",
                            "Not Full Market Price": "not_full_market_price",
                            "VAT Exclusive": "vat_exclusive",
                            "Description of Property": "description",
                            "Property Size Description": "property_size_description",
                        }
                        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                        # Write headers as first line
                        writer.writerow(fieldnames)
                        for row in reader:
                            # Write all rows in file
                            writer.writerow(row)

    else:
        print(f"[Extract] File already exists. Skipping saving raw data")

# Main function called inside the execute.py script
def main():
    print("[Extract] Start")
    download_snapshot()
    save_new_raw_data()
    print(f"[Extract] End")
