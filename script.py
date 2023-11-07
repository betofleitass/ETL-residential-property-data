# Import the required libraries
import requests
import datetime
import zipfile
import os
from zipfile import ZipFile
import csv
from pprint import pprint

# Path of the zip file
zip_file_path = "https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip"

# Get the current date (in the format YYYY-MM-DD)
current_date = datetime.datetime.now().date()

# Directory where you want to save the file
save_directory = f"tmp/data/source/downloaded_at={current_date}"

# Create the directory if it doesn't exist
os.makedirs(save_directory, exist_ok=True)

# Check if the file already exists
local_path = os.path.join(save_directory, "PPR-ALL.zip")

if not os.path.exists(local_path):
    # The file does not exist, so download it
    response = requests.get(zip_file_path)

    # Print the status code
    print(response.status_code)

    # Save the file locally
    with open(local_path, "wb") as f:
        f.write(response.content)
else:
    print("File already exists. Skipping the download.")

# Explore the Zip file
with ZipFile(local_path, mode="r") as f:
    # Get the list of files and print it
    file_names = f.namelist()
    print(file_names)

# Add the current date to the extracted folder
extracted_folder = f"tmp/data/csv/{current_date}"

# Check if the folder for the current date already exists
if not os.path.exists(extracted_folder):
    # Create the extracted folder if it doesn't exist
    os.makedirs(extracted_folder, exist_ok=True)

    # Extract the contents of the zip file
    with ZipFile(local_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_folder)
    print("Zip file has been successfully extracted to:", extracted_folder)
else:
    print(f"Folder for {current_date} already exists. Skipping the extraction.")

# Path of the CSV file
path = os.path.join(extracted_folder, "PPR-ALL.csv")

# Open the csv file in read mode
with open(path, mode="r", encoding="windows-1252") as csv_file:
    # Open csv_file so that each row is a dictionary
    reader = csv.DictReader(csv_file)
    
    # Print the first row
    row = next(reader)
    print(type(row))
    pprint(row)

fieldnames = {
    "Date of Sale (dd/mm/yyyy)": "date_of_sale",
    "Address": "address",
    "Postal Code": "postal_code",
    "County": "county",
    "Price (€)": "price",
    "Description of Property": "description",
}
