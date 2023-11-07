# Import the required libraries
import requests
import datetime
import os

# Path of the zip file
path = "https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip"

# Get the zip file
response = requests.get(path)

# Print the status code
print(response.status_code)

# Get the current date (in the format YYYY-MM-DD)
current_date = datetime.datetime.now().date()

# Define the directory where you want to save the file
save_directory = f"tmp/data/source/downloaded_at={current_date}"

# Create the directory if it doesn't exist
os.makedirs(save_directory, exist_ok=True)

# Save the file locally
local_path = os.path.join(save_directory, "PPR-ALL.zip")
with open(local_path, "wb") as f:
    f.write(response.content)
