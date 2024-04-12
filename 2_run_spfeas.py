# %%
# run from terminal as
# python run_spfeas.py
# then submit all the batch scripts using
# bash run_all_spfeas_batch_files.sh
import os
from glob import glob

# base path
base_path = "/CCAS/groups/engstromgrp/mike"  # "/mnt/bigdrive/Dropbox/wb_malawi"
os.chdir(base_path)

# make folder to hold batch scripts
batch_script_path = os.path.join(base_path, "batch_scripts")
os.makedirs(batch_script_path, exist_ok=True)

# find all input images to process
imagery_folder = os.path.join(base_path, "mosaic")
images = glob(f"{imagery_folder}/*.tif")
print("processing images:", images)

# set output folder
output_folder = os.path.join(base_path, "spfeas_outputs")
os.makedirs(output_folder, exist_ok=True)
os.makedirs(os.path.join(output_folder, "features"), exist_ok=True)


with open(f"{base_path}/run_all_spfeas_batch_files.sh", "w") as f:

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

        with open(f"{batch_script_path}/{image_name}.sh", "w") as f:

            f.write(
                f"""#!/bin/bash
#SBATCH -p defq
#SBATCH -J spfeas_{image_name}_run
#SBATCH --export=NONE
#SBATCH -t 5-00:00:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=mmann1123@gwu.edu
#SBATCH -e {batch_script_path}/{image_name}.err
#SBATCH -o {batch_script_path}/{image_name}.out


export PATH="/groups/engstromgrp/anaconda3/bin:$PATH"
source activate Ryan_CondaEnvP2.7

# output folders will be created automatically
# scales 3, 5, 7
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_mean')} --block 1 --scales 3 5 7 --tr mean --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_gabor')} --vis-order bgrn --block 1 --scales 3 5 7 --tr gabor --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_hog')} --vis-order bgrn --block 1 --scales 3 5 7 --tr hog --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_lac')} --vis-order bgrn --block 1 --scales 3 5 7 --tr lac --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_lbpm')} --vis-order bgrn --block 1 --scales 3 5 7 --tr lbpm --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_ndvi')} --vis-order bgrn --block 1 --scales 3 5 7 --tr ndvi --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_pantex')} --vis-order bgrn --block 1 --scales 3 5 7 --tr pantex --overwrite

# scales 31, 51, 71
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_sfs')} --vis-order bgrn --block 1 --scales 31 51 71 --tr sfs --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_fourier')} --vis-order bgrn --block 1 --scales 31 51 71 --tr fourier --overwrite
spfeas -i {image} -o {os.path.join(output_folder, "features", image_name+'_orb')} --vis-order bgrn --block 1 --scales 31 51 71 --tr orb --overwrite
"""
            )

# jobs were submited subset at time


# defq           up 14-00:00:0      1  down* gpu041
# defq           up 14-00:00:0      4  drain cpu[088,096,111],gpu044
# defq           up 14-00:00:0    137  alloc cpu[020-087,089-095,097-103,109-110,112-126,131-158,161-163],gpu[042-043,045-049]
# short          up 1-00:00:00      3  drain cpu[088,096,111]
# short          up 1-00:00:00    130  alloc cpu[020-087,089-095,097-103,109-110,112-126,131-158,161-163]
# short-384gb    up 1-00:00:00      1  drain cpu111
# short-384gb    up 1-00:00:00     10  alloc cpu[109-110,112-119]
# tiny           up    4:00:00      3  drain cpu[088,096,111]
# tiny           up    4:00:00    130  alloc cpu[020-087,089-095,097-103,109-110,112-126,131-158,161-163]
# nano           up      30:00      1  down* cpu006
# nano           up      30:00     15   idle cpu[004-005,007-019]
# 384gb          up 14-00:00:0      1  drain cpu111
# 384gb          up 14-00:00:0     47  alloc cpu[109-110,112-126,131-158,161-162]
# highMem        up 14-00:00:0      4  alloc hmm[001-004]
# highThru       up 7-00:00:00      6  alloc hth[001-006]
# graphical      up    4:00:00      3  alloc cpu[001-002],gpu014
# debug          up    4:00:00      3  alloc cpu[001-002],gpu013
# debug-cpu      up    4:00:00      2  alloc cpu[001-002]
# debug-gpu      up    4:00:00      1  alloc gpu013
# ultra-gpu      up 7-00:00:00      1  alloc gpu050
# large-gpu      up 7-00:00:00     21  alloc gpu[002-012,021-023,032-038]
# med-gpu        up 7-00:00:00      1  drain gpu039
# med-gpu        up 7-00:00:00      8  alloc gpu[018-020,028-031,040]
# small-gpu      up 7-00:00:00      1  down* gpu041
# small-gpu      up 7-00:00:00      1  drain gpu044
# small-gpu      up 7-00:00:00     16  alloc gpu[013-017,024-027,042-043,045-049]
# awscpu         up   infinite     10  idle~ awscpu-cpunode-[0-9]
# awsgpu*        up   infinite      2  down% awsgpu-gpunode-[8-9]
# awsgpu*        up   infinite      8  idle~ awsgpu-gpunode-[0-7]

# %%
