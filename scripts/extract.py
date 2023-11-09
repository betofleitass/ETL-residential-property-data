import csv
import datetime
import os
import requests
import tempfile
from zipfile import ZipFile

# Settings
BASE_PATH = os.path.abspath(__file__ + "/../../")

# External website file url
SOURCE_URL = "https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip"

# Get the current date (in the format YYYY-MM-DD)
CURRENT_DATE = datetime.datetime.now().date()

# Source path where we want to save the .zip file downloaded from the website
SOURCE_ZIP_PATH = f"{BASE_PATH}/data/source/downloaded_at={CURRENT_DATE}/PPR-ALL.zip"

# Raw path where we want to extract the new .csv data
RAW_CSV_PATH = f"{BASE_PATH}/data/raw/downloaded_at={CURRENT_DATE}/ppr-all.csv"


def create_folder_if_not_exists(path):
    """
    Create a folder at the specified path if it doesn't already exist.

    Parameters:
    - path (str): The path of the folder to be created.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)



def download_snapshot():
    """
    Download a snapshot from a specified URL and save it to the designated path.

    This function checks if the snapshot file already exists. If not, it creates
    the necessary folders and downloads the file. If an error occurs during the
    download, an exception is raised, and the script exits.
    """

    if not os.path.exists(SOURCE_ZIP_PATH):
        print("[Extract] Downloading snapshot")
        create_folder_if_not_exists(SOURCE_ZIP_PATH)
        try:
            with open(SOURCE_ZIP_PATH, "wb") as source_ppr:
                response = requests.get(SOURCE_URL, verify=False)
                response.raise_for_status()
                source_ppr.write(response.content)
        except Exception as e:
            print(f"[Extract] Error: {str(e)}")
            raise Exception("Failed to download snapshot. Script will exit.")
    else:
        print("[Extract] Snapshot file already exists. Skipping download.")


def extract_zip_file(zip_file_path, target_dir):
    """
    Extract the contents of a ZIP file to a specified target directory.

    Parameters:
    - zip_file_path (str): The path to the ZIP file to be extracted.
    - target_dir (str): The directory where the contents of the ZIP file will be extracted.

    Returns:
    str: The path to the extracted CSV file.

    """
    with ZipFile(zip_file_path, "r") as zipfile:
        csv_file_path = zipfile.extract(zipfile.namelist()[0], path=target_dir)
    return csv_file_path


def process_csv_data(csv_file_path, output_path, fieldnames, row_limit):
    """
    Process CSV data, filter selected columns, and write to an output CSV file.

    Parameters:
    - csv_file_path (str): The path to the input CSV file.
    - output_path (str): The path to the output CSV file.
    - fieldnames (dict): A dictionary mapping original column names to new column names.
    - row_limit (int): The maximum number of rows to process.

    Note:
    The function assumes that the input CSV file uses the 'windows-1252' encoding.
    It reads the CSV file, prints the first row as an example, filters selected columns,
    and writes the processed data to a new CSV file.
    """
    with open(csv_file_path, mode="r", encoding="windows-1252") as csv_file:
        reader = csv.DictReader(csv_file)
        row = next(reader)
        print("[Extract] First row example:", row)

        with open(output_path, mode="w", encoding="windows-1252") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            counter = 1
            writer.writerow(fieldnames)
            for row in reader:
                filtered_rowdict = {
                    key: value for key, value in row.items() if key in fieldnames
                }
                if counter <= row_limit:
                    writer.writerow(filtered_rowdict)
                    counter += 1
                else:
                    break


def save_new_raw_data():
    """
    Save new raw data from a source ZIP file to a specified output CSV file.

    This function checks if the output CSV file already exists. If not, it creates
    the necessary folders, extracts data from a source ZIP file, processes the CSV data,
    and saves the processed data to the output CSV file.
    """

    if not os.path.exists(RAW_CSV_PATH):
        print(f"[Extract] Saving data from '{SOURCE_ZIP_PATH}' to '{RAW_CSV_PATH}'")
        create_folder_if_not_exists(RAW_CSV_PATH)

        with tempfile.TemporaryDirectory() as dirpath:
            csv_file_path = extract_zip_file(SOURCE_ZIP_PATH, dirpath)
            fieldnames = {
                "Date of Sale (dd/mm/yyyy)": "date_of_sale",
                "Address": "address",
                "Postal Code": "postal_code",
                "County": "county",
                "Price (€)": "price",
                "Description of Property": "description",
            }
            process_csv_data(csv_file_path, RAW_CSV_PATH, fieldnames, row_limit=100000)
    else:
        print(f"[Extract] File already exists. Skipping saving raw data")


def main():
    print("[Extract] Start")
    download_snapshot()
    save_new_raw_data()
    print(f"[Extract] End")