# %% execute the following code to convert the VRT files to TIF files
# it will create a series of bash scripts that will be executed in parallel
# in a folder called batch_scripts_vrt_to_tif in the parent directory of the feature VRT files
# author: Michael Mann GWU
# from terminal:
# python 3_features_to_tifs.py
# Import modules

# %%
import os
from glob import glob
from functions import *
from multiprocessing import Pool
import subprocess

################################################
# NEED TO EDIT THIS LINES
# path to folder containing outputs from spfeas (e.g. folders ending in _mean _gabor etc),
# should be  {imagery_folder}/features from 2_run_spfeas.py

feature_vrt_output_directory = r"/CCAS/groups/engstromgrp/mike/spfeas_outputs/features"

partition = "short"  # partition for slurm
time_request = "00-23:55:00"  # time request for slurm DD-HH:MM:SS
email = "mmann1123@gwu.edu"  # email for slurm notifications


################ Don't edit below this line ################

# check for errors in slurm partition and time request
check_partition_time(partition, time_request)


# create folder for feature tifs
feature_tif_output_directory = os.path.join(
    os.path.dirname(feature_vrt_output_directory), "tifs"
)
os.makedirs(feature_tif_output_directory, exist_ok=True)

# create folder for batch scripts
batch_script_path = os.path.join(
    os.path.dirname(feature_vrt_output_directory), "batch_scripts_vrt_to_tif"
)
os.makedirs(batch_script_path, exist_ok=True)

# write the batch script to run all the other batch scripts
with open(
    f"{os.path.dirname(feature_vrt_output_directory)}/run_all_vrt_to_tif_batch_files.sh",
    "w",
) as f:

    f.write(
        f"""#!/bin/bash

# Set the folder containing the .sh files
folder={batch_script_path}

# Loop through each .sh file in the folder
for file in "$folder"/*.sh; do
    # Submit the .sh file to Slurm
    sbatch "$file"
done
"""
    )


# Get all VRT paths
vrt_paths = glob(os.path.join(feature_vrt_output_directory, "*/*.vrt"))
if not vrt_paths:
    raise ValueError("No vrts found in the folder")
print("vrt files found: ", vrt_paths)


# for each vrt file get its scales and feature name, and region based on the vrt path


def process_vrt(vrt):
    # get all scales
    scales = get_scales(vrt)
    # get feature name
    feature = get_feature_name(vrt)
    # get folder name
    folder = get_vrt_folder_name(vrt)

    print(
        "Processing feature: ",
        feature,
        " with scales: ",
        scales,
        " in folder: ",
        folder,
    )

    # bashscript output file name
    bash_script_name = f"{folder}_vrt_to_tif.sh"

    # create bash script path
    path_to_bash_script = os.path.join(batch_script_path, bash_script_name)

    # delete the file if it exists
    if os.path.exists(path_to_bash_script):
        os.remove(path_to_bash_script)
    print("Writing bash file to: ", path_to_bash_script)

    # Write SBATCH header
    with open(os.path.join(path_to_bash_script), "a+") as file:
        file.write(
            f"""#!/bin/bash
#SBATCH -p {partition}
#SBATCH -J {folder}_vrt2tif
#SBATCH --export=NONE
#SBATCH -t {time_request}
#SBATCH --mail-type=ALL
#SBATCH --mail-user={email}
#SBATCH -e {folder}_vrt2tif.err
#SBATCH -o {folder}_vrt2tif.out 

export PATH="/usr/bin:$PATH"
source activate Ryan_CondaEnvP3.6
               
# This script takes the VRT files (the output of spfeas) and extracts the VRT
# band-by-band, assigning each band according to its output name. The order of
# outputs is determined from the spfeas package.

               
"""
        )

    band_count = 0

    # Open file and add comment indicating the scale and the output
    with open(os.path.join(path_to_bash_script), "a+") as file:
        file.write(f"###################### \n #{folder.upper()}\n\n")

    for scale in scales:
        for output in feature_bands_table[feature]:

            # Open file and add comment indicating the scale and the output
            with open(os.path.join(path_to_bash_script), "a+") as file:
                file.write(f"# scale: {scale}, output: {output}\n")

            # Increase band count by 1
            band_count += 1

            # Create the relative path for the output tif that will be
            output_tif = os.path.join(
                feature_tif_output_directory,
                folder + "_SC" + scale + "_" + output + ".tif",
            )

            # Open file and write the gdal command to extract the bands and write to tif
            with open(os.path.join(path_to_bash_script), "a+") as file:
                file.write(
                    f'gdal_translate -b {band_count} -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" {vrt} {output_tif}\n\n'
                )


# Create a pool of workers
with Pool() as p:
    p.map(process_vrt, vrt_paths)


print(
    f"""\n\n############# IMPORANT ################## 
#############################################
All batch scripts will be written to folder: {batch_script_path}

Execute all batch scripts using:  {os.path.dirname(feature_vrt_output_directory)}/run_all_vrt_to_tif_batch_files.sh

All output tifs will be written to folder: {feature_tif_output_directory}
#############################################
#############################################

    """
)


# Ask the user if they want to execute the bash script
execute_script = input(
    f"Execute all the batch scripts and convert spfeas vrts to tifs? (yes/no): "
)


# If the user answers 'yes', execute the bash script
if execute_script.lower() == "yes":
    subprocess.run(["bash", batch_script_path])
# %%
# %%
