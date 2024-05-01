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
from tqdm import tqdm
from multiprocessing import Pool

############### EDIT THE FOLLOWING ################
imagery_folder = "/CCAS/groups/engstromgrp/mike/mosaic_2020"  # path to folder containing images ending in .tif

output_folder_name = (
    "spfeas_outputs_2020"  # name of folder to hold spfeas outputs not path
)

band_order = "bgrn"  # band order for spfeas
partition = "defq"  # partition for slurm
time_request = "04-12:35:00"  # time request for slurm DD-HH:MM:SS
email = "mmann1123@gwu.edu"  # email for slurm notifications

# features to run and what scales
feature_scale_dict = {
    "gabor": [3, 5, 7],
    "hog": [3, 5, 7],
    "lac": [3, 5, 7],
    "lbpm": [3, 5, 7],
    "mean": [3, 5, 7],
    "ndvi": [3, 5, 7],
    "pantex": [3, 5, 7],
    "sfs": [31, 51, 71],
    "fourier": [31, 51, 71],
    "orb": [31, 51, 71],
}

image_name_subset = "*"  # subset of images to process, use '*' for all images

################ Don't edit below this line ################

# check for errors in slurm partition and time request
check_partition_time(partition, time_request)


# throw error if band order is not correct
if band_order not in ["bgrn", "rgbn", "rgb", "bgr"]:
    raise ValueError("band order must be one of 'bgrn', 'rgbn', 'rgb', 'bgr'")

# base path
base_path = os.path.dirname(imagery_folder)

# find all input images to process
images = glob(f"{imagery_folder}/{image_name_subset}.tif")

print("Number of images found:", len(images))
if len(images) < 6:
    print("Example", images[0])
else:
    print("Example", images[:5])

if not images:
    raise ValueError("No images found in the folder")

# set output folder
output_folder = os.path.join(base_path, output_folder_name)
os.makedirs(output_folder, exist_ok=True)
os.makedirs(os.path.join(output_folder, "features"), exist_ok=True)


# make folder to hold batch scripts
batch_script_path = os.path.join(output_folder, "spfeas_batch_scripts")
os.makedirs(batch_script_path, exist_ok=True)
# remove all files in the folder
files = glob(os.path.join(batch_script_path, "*.sh"))
with Pool() as p:
    # Use tqdm for progress bar
    for _ in tqdm(
        p.imap_unordered(remove_file, files),
        total=len(files),
        desc="erasing existing batch scripts",
    ):
        pass

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
for image in tqdm(images, desc="writing new batch scripts"):
    for feature, scales in feature_scale_dict.items():

        # get file name without extension
        image_name = os.path.splitext(os.path.basename(image))[0]

        # unpack scales as space separated string
        scale_text = " ".join([str(scale) for scale in scales])

        # write the batch script
        with open(f"{batch_script_path}/{image_name}_{feature}.sh", "w") as f:
            f.write(
                f"""#!/bin/bash
#SBATCH -p {partition}
#SBATCH -J sp_{feature}_{image_name}_run
#SBATCH --export=NONE
#SBATCH -t {time_request}
#SBATCH --mail-type=ALL
#SBATCH --mail-user={email} 
#SBATCH -e {batch_script_path}/{image_name}_{feature}.err
#SBATCH -o {batch_script_path}/{image_name}_{feature}.out


export PATH="/groups/engstromgrp/anaconda3/bin:$PATH"
source activate Ryan_CondaEnvP2.7

# output folders will be created automatically
# scales 3, 5, 7
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_'+feature)} --block 1 --scales {scale_text} --tr {feature} --overwrite

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
    subprocess.run(["bash", f"{output_folder}/run_all_spfeas_batch_files.sh"])

# %%
