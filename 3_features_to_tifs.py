# %%
# Import modules
import os
from glob import glob
from functions import *

################################################
# NEED TO EDIT THIS LINES
# path to folder containing outputs from spfeas
feature_vrt_output_directory = r"/CCAS/groups/engstromgrp/mike/spfeas_outputs/features"  # "/home/mmann1123/Dropbox/wb_malawi/test"  # "/mnt/bigdrive/Dropbox/wb_malawi/test"  ##
os.chdir(feature_vrt_output_directory)

# bashscript output file name
bash_script_name = "vrt_to_tif.sh"

################################################

# create folder for feature tifs
feature_tif_output_directory = os.path.join(
    os.path.dirname(feature_vrt_output_directory), "tifs"
)


path_to_bash_script = os.path.join(
    os.path.dirname(feature_vrt_output_directory), bash_script_name
)  # "/mnt/bigdrive/Dropbox/wb_malawi/test"  ##r"/CCAS/groups/engstromgrp/mike/spfeas_outputs/features"
# delete the file if it exists
if os.path.exists(path_to_bash_script):
    os.remove(path_to_bash_script)
print("Writing bash file to: ", path_to_bash_script)

# Get all VRT paths
vrt_paths = glob(os.path.join(feature_vrt_output_directory, "*/*.vrt"))
print("vrt files found: ", vrt_paths)

# Write SBATCH header
with open(os.path.join(path_to_bash_script), "a+") as file:
    file.write(
        f"""#!/bin/bash
#SBATCH -p defq
#SBATCH -J spfeas_vrt_to_tif_run
#SBATCH --export=NONE
#SBATCH -t 5-00:00:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=mmann1123@gwu.edu
#SBATCH -e vrt_to_tiff.err
#SBATCH -o vrt_to_tiff.out 

export PATH="/usr/bin:$PATH"
source activate Ryan_CondaEnvP3.6
               
# This script takes the VRT files (the output of spfeas) and extracts the VRT
# band-by-band, assigning each band according to its output name. The order of
# outputs is determined from the spfeas package.

               
"""
    )

for vrt in vrt_paths:
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
            # extracted from the VRT band-by-band *
            output_tif = os.path.join(
                feature_tif_output_directory,
                folder + "_SC" + scale + "_" + output + ".tif",
            )

            # Open file and write the code *
            with open(os.path.join(path_to_bash_script), "a+") as file:
                file.write(
                    f'gdal_translate -b {band_count} -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" {vrt} {output_tif}\n\n'
                )


# %%
# %%
