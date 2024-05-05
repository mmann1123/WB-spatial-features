# author: Michael Mann mmann1123@gwu.edu
# copies files from a source directory to a target directory based on the filename
# input: folder of spfeas tifs
# output: organized folder of tifs as follows:
# north-south/feature_quarter_start_end/feature_name_band.tif
# south/lbpm_2020_01_01_2020_03_31/lbpm_sc5_kurtosis.tif
# ---------------------------------lbpm_sc7_kurtosis.tif


import os
import re
import shutil

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

        # Check if the feature is in the list to process
        if feature in features:
            # Create the directory structure if it doesn't exist
            dir_path = os.path.join(target_directory, direction, f"{feature}_{quarter}")
            os.makedirs(dir_path, exist_ok=True)

            # Define the new file path
            new_file_path = os.path.join(dir_path, file)

            # Move the file to the new location
            shutil.move(os.path.join(source_directory, file), new_file_path)
            print(f"Moved {file} to {new_file_path}")
        else:
            print(
                f"Feature {feature} in file {file} is not recognized or not in the list."
            )
    else:
        print(
            f"Filename {file} does not match the expected pattern. Update re.match('')"
        )
