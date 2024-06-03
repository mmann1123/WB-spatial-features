# %% run with sbatch 5a_organize_tifs.sh
# Description: Organize the tifs into folders based on the feature and quarter
# Author: Michael Mann GWU mmann1123@gwu.edu

# expected file structure:
# spfeas_outputs
# ├── tifs
# │   ├── S2_SR_2020_Q01_north_hog_SC1_0000000000.tif
# │   ├── S2_SR_2020_Q01_north_hog_SC1_0000000001.tif

# output file structure:
# spfeas_outputs2
# ├── tifs_organized
# ├── south
# |   ├── lbpm_2020_01_01_2020_03_31
# |   |   ├── lbpm_sc7_kurtosis.tif
# |   |   ├── lbpm_sc7_mean.tif

import os
import re
import shutil

import logging
from multiprocessing import Pool
from helpers import get_quarter_dates
from tqdm import tqdm

# Initialize logging
logging.basicConfig(
    filename="file_transfer_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Define the source and target directories
source_directory = r"/CCAS/groups/engstromgrp/mike/spfeas_outputs_2020/tifs"
target_directory = r"/CCAS/groups/engstromgrp/mike/spfeas_outputs2/tifs_organized"

# Ensure the target directory exists
os.makedirs(target_directory, exist_ok=True)

# List of features to process
features = [
    # "fourier",
    # "gabor",
    # "hog",
    "lac",
    "lbpm",
    "mean",
    "ndvi",
    "orb",
    "pantex",
    "sfs",
]

# Get all files in the source directory
files = [
    f
    for f in os.listdir(source_directory)
    if os.path.isfile(os.path.join(source_directory, f))
]

print("Files founds #:", len(files))
if not files:
    raise ValueError("No files found in the folder")
if len(files) < 6:
    print("Example", files[0])
else:
    print("Example", files[:5])


# Function to handle file processing
def process_file(file):
    match = re.match(
        r"S2_SR_(\d{4}_Q\d{2})_(north|south)_([a-z]+)_SC(\d+)_([a-zA-Z0-9_]+)\.tif",
        file,
    )
    if match:
        quarter, direction, feature, sc_number, descriptor = match.groups()
        start, end = get_quarter_dates(quarter)
        if feature in features:
            dir_path = os.path.join(
                target_directory, direction, f"{feature}_{start}_{end}"
            )
            os.makedirs(dir_path, exist_ok=True)
            new_file_path = os.path.join(
                dir_path, f"{feature}_sc{sc_number}_{descriptor}.tif"
            )
            shutil.copy2(os.path.join(source_directory, file), new_file_path)
            return "success", file, new_file_path
        else:
            return "error", file, "Feature not recognized"
    else:
        return "error", file, "Filename pattern mismatch"


# Run processing with multiprocessing
if __name__ == "__main__":
    success_count = 0
    error_count = 0

    with Pool() as pool:
        # Wrap 'files' with tqdm for a progress bar
        results = pool.imap_unordered(process_file, tqdm(files, total=len(files)))
        for status, src, message in results:
            if status == "success":
                logging.info(f"Copied {src} to {message}")
                success_count += 1
            else:
                logging.error(f"Error with {src}: {message}")
                error_count += 1

    # Print summary of the processing
    logging.info(f"Total files processed: {len(files)}")
    logging.info(f"Successful transfers: {success_count}")
    logging.info(f"Failed transfers: {error_count}")
    print(f"Total files processed: {len(files)}")
    print(f"Successful transfers: {success_count}")
    print(f"Failed transfers: {error_count}")

# %% copy back
# from glob import glob
# import os
# target_directory = r"/CCAS/groups/engstromgrp/mike/spfeas_outputs/tifs_organized"
# os.chdir(target_directory)

# glob('*/*')
