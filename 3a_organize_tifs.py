#%%
import os
import re
import shutil
from helpers import get_quarter_dates

# Define the source directory where your files are currently stored
source_directory = r"/CCAS/groups/engstromgrp/mike/spfeas_outputs/tifs"


# Define the target directory where you want the files to be organized
target_directory = r"/CCAS/groups/engstromgrp/mike/spfeas_outputs/tifs_organized"


# List of features for easy access
features = [
    "fourier",
    "gabor",
    "hog",
    "lac",
    "lbpm",
    "mean",
    "ndvi",
    "orb",
    "pantex",
    "sfs",
]
 
# Make sure target directory exists
os.makedirs(target_directory, exist_ok=True)

# Get all files in the source directory
files = [
    f
    for f in os.listdir(source_directory)
    if os.path.isfile(os.path.join(source_directory, f))
]

# Process each file
for file in files:
    # Extract information from the filename
    match = re.match(r"S2_SR_(\d{4}_Q\d{2})_(north|south)_([a-z]+)_.*\.tif", file)
    if match:
        quarter = match.group(1)
        direction = match.group(2)
        feature = match.group(3)
        start,end = get_quarter_dates(quarter)
        # Check if the feature is in the list to process
        if feature in features:
            # Create the directory structure if it doesn't exist
            dir_path = os.path.join(target_directory, direction, f"{feature}_{start}_{end}")
            os.makedirs(dir_path, exist_ok=True)

            # Define the new file path
            new_file_path = os.path.join(dir_path, file)

            # Move the file to the new location
            shutil.copy2(os.path.join(source_directory, file), new_file_path)
            print(f"Moved {file} to {new_file_path}")
        else:
            print(
                f"Feature {feature} in file {file} is not recognized or not in the list."
            )
    else:
        print(f"Filename {file} does not match the expected pattern.")
