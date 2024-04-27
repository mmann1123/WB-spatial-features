# %% This takes imagery, runs spfeas, and then converts the output VRT files
# author: Michael Mann GWU
# run from terminal as
# python 2_run_spfeas.py
# then submit all the batch scripts using
# bash run_all_spfeas_batch_files.sh
import os
from glob import glob
from functions import *  # import helper functions
import subprocess

############### EDIT THE FOLLOWING ################
imagery_folder = "/CCAS/groups/engstromgrp/mike/mosaic"  # path to folder containing images ending in .tif

band_order = "bgrn"  # band order for spfeas
partition = "defq"  # partition for slurm
time_request = "04-12:35:00"  # time request for slurm DD-HH:MM:SS
email = "mmann1123@gwu.edu"  # email for slurm notifications

################ Don't edit below this line ################

# check for errors in slurm partition and time request
check_partition_time(partition, time_request)


# throw error if band order is not correct
if band_order not in ["bgrn", "rgbn", "rgb", "bgr"]:
    raise ValueError("band order must be one of 'bgrn', 'rgbn', 'rgb', 'bgr'")

# base path
base_path = os.path.dirname(imagery_folder)

# find all input images to process
images = glob(f"{imagery_folder}/*.tif")
print("Number of images found:", len(images))
if len(images) < 6:
    print("Example", images[0])
else:
    print("Example", images[:5])

if not images:
    raise ValueError("No images found in the folder")

# set output folder
output_folder = os.path.join(base_path, "spfeas_outputs")
os.makedirs(output_folder, exist_ok=True)
os.makedirs(os.path.join(output_folder, "features"), exist_ok=True)


# make folder to hold batch scripts
batch_script_path = os.path.join(output_folder, "spfeas_batch_scripts")
os.makedirs(batch_script_path, exist_ok=True)

# write the batch script to run all the other batch scripts
with open(f"{output_folder}/run_all_spfeas_batch_files.sh", "w") as f:

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

# for each image write a batch script
for image in images:
    for feature in [
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
    ]:

        # get file name without extension
        image_name = os.path.splitext(os.path.basename(image))[0]

        # write the batch script
        with open(f"{batch_script_path}/{image_name}.sh", "w") as f:
            f.write(
                f"""#!/bin/bash
#SBATCH -p {partition}
#SBATCH -J spfeas_{image_name}_run
#SBATCH --export=NONE
#SBATCH -t {time_request}
#SBATCH --mail-type=ALL
#SBATCH --mail-user={email} 
#SBATCH -e {batch_script_path}/{image_name}.err
#SBATCH -o {batch_script_path}/{image_name}.out


export PATH="/groups/engstromgrp/anaconda3/bin:$PATH"
source activate Ryan_CondaEnvP2.7

# output folders will be created automatically
# scales 3, 5, 7
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_mean')} --block 1 --scales 3 5 7 --tr mean --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_gabor')} --vis-order {band_order} --block 1 --scales 3 5 7 --tr gabor --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_hog')} --vis-order {band_order} --block 1 --scales 3 5 7 --tr hog --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_lac')} --vis-order {band_order} --block 1 --scales 3 5 7 --tr lac --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_lbpm')} --vis-order {band_order} --block 1 --scales 3 5 7 --tr lbpm --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_ndvi')} --vis-order {band_order} --block 1 --scales 3 5 7 --tr ndvi --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_pantex')} --vis-order {band_order} --block 1 --scales 3 5 7 --tr pantex --overwrite

# scales 31, 51, 71
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_sfs')} --vis-order {band_order} --block 1 --scales 31 51 71 --tr sfs --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_fourier')} --vis-order {band_order} --block 1 --scales 31 51 71 --tr fourier --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_orb')} --vis-order {band_order} --block 1 --scales 31 51 71 --tr orb --overwrite
"""
            )


print(
    f"""\n\n############# IMPORANT ################## 
#############################################
All batch scripts will be written to folder: {batch_script_path}

Execute all batch scripts using:  {output_folder}/run_all_spfeas_batch_files.sh

All output feature vrts and images will be writen to: {output_folder}/features
#############################################
#############################################

    """
)

# Ask the user if they want to execute the bash script
execute_script = input(
    f"Execute all the batch scripts and create all spfeas features? (yes/no): "
)


# If the user answers 'yes', execute the bash script
if execute_script.lower() == "yes":
    subprocess.run(["bash", f"{batch_script_path}/{image_name}.sh"])

# %%
