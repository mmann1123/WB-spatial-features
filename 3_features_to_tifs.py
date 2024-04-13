# %%
# Import modules
import os
from glob import glob
import re


def get_scales(vrt_path):
    matches = re.search(r"_SC(\d+(?:-\d+)*)_[^/]*TR", vrt_path)

    if matches:
        # Extract the numbers
        scales = matches.group(1).split("-")
        return scales
    else:
        raise ValueError("No scales found")


def get_feature_name(vrt_path):
    matches = re.search(r"TR(.*?)\.vrt", vrt_path)
    if matches:
        feature_name = matches.group(1)
        print(feature_name)
        return feature_name
    else:
        raise ValueError("No feature_name found")


def get_vrt_folder_name(vrt_path):
    parent_folder = os.path.dirname(vrt_path)
    parent_folder_name = os.path.basename(parent_folder)
    return parent_folder_name


# Create dictionary of outputs for each feature
# The number of bands differs based on contextual feature; more information
# can be found at https://github.com/jgrss/spfeas.
feature_bands_table = {
    "fourier": ["mean", "variance"],
    "gabor": [
        "mean",
        "variance",
        "filter_1",
        "filter_2",
        "filter_3",
        "filter_4",
        "filter_5",
        "filter_6",
        "filter_7",
        "filter_8",
        "filter_9",
        "filter_10",
        "filter_11",
        "filter_12",
        "filter_13",
        "filter_14",
    ],
    "hog": ["max", "mean", "variance", "skew", "kurtosis"],
    "lac": ["lac"],
    "lbpm": ["max", "mean", "variance", "skew", "kurtosis"],
    "mean": ["mean", "variance"],
    "ndvi": ["mean", "variance"],
    "orb": ["max", "mean", "variance", "skew", "kurtosis"],
    "pantex": ["min"],
    "sfs": [
        "max_line_length",
        "min_line_length",
        "mean",
        "w_mean",
        "std",
        "max_ratio_of_orthogonal_angles",
    ],
}

# NEED TO EDIT THIS LINE
feature_vrt_output_directory = "/home/mmann1123/Dropbox/wb_malawi/test"  # "/mnt/bigdrive/Dropbox/wb_malawi/test"  ##r"/CCAS/groups/engstromgrp/mike/spfeas_outputs/features"
os.chdir(feature_vrt_output_directory)

# create folder for feature tifs
feature_tif_output_directory = os.path.join(
    os.path.dirname(feature_vrt_output_directory), "tifs"
)


path_to_bash_script = "/home/mmann1123/Dropbox/wb_malawi/vrt_to_tif.sh"  # "/mnt/bigdrive/Dropbox/wb_malawi/test"  ##r"/CCAS/groups/engstromgrp/mike/spfeas_outputs/features"
# delete the file if it exists
if os.path.exists(path_to_bash_script):
    os.remove(path_to_bash_script)

# Get all VRT paths
vrt_paths = glob(os.path.join(feature_vrt_output_directory, "*/*.vrt"))
print(vrt_paths)

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
        print(scale)

        for output in feature_bands_table[feature]:
            print(output)

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

# # Create dictionary of outputs for each feature
# # The number of bands differs based on contextual feature; more information
# # can be found at https://github.com/jgrss/spfeas.
# feature_bands_table = {
#     "fourier": ["mean", "variance"],
#     "gabor": [
#         "mean",
#         "variance",
#         "filter_1",
#         "filter_2",
#         "filter_3",
#         "filter_4",
#         "filter_5",
#         "filter_6",
#         "filter_7",
#         "filter_8",
#         "filter_9",
#         "filter_10",
#         "filter_11",
#         "filter_12",
#         "filter_13",
#         "filter_14",
#     ],
#     "hog": ["max", "mean", "variance", "skew", "kurtosis"],
#     "lac": ["lac"],
#     "lbpm": ["max", "mean", "variance", "skew", "kurtosis"],
#     "mean": ["mean", "variance"],
#     "ndvi": ["mean", "variance"],
#     "orb": ["max", "mean", "variance", "skew", "kurtosis"],
#     "pantex": ["min"],
#     "sfs": [
#         "max_line_length",
#         "min_line_length",
#         "mean",
#         "w_mean",
#         "std",
#         "max_ratio_of_orthogonal_angles",
#     ],
# }
# # %%
# for vrt in vrt_paths:

# # Function writes the outputs to a text file in the format necessary to run .sh
# # Need to extract the VRT band-by-band and assign each band to its output name
# def write_outputs(scales):

#     # Set band count to 0
#     # The band count will help associate a band in the VRT to its output (i.e., variance)
#     # based on the spfeas package
#     band_count = 0

#     # Iterate through each scale, which contains multiple outputs
#     for scale in scales:

#         # Iterate through each output of the feature
#         for output in outputs[feature]:

#             # Open file and add comment indicating the scale and the output
#             with open(os.path.join(output_directory, text_file), "a+") as file:
#                 file.write("# scale: {}, output: {}\n".format(scale, output))

#             # Increase band count by 1
#             band_count = band_count + 1

#             # Create the relative path for the output tif that will be
#             # extracted from the VRT band-by-band *
#             output_tif = os.path.join(
#                 r"../outputs/band",
#                 feature + "_2021_09_01_2021_12_01",
#                 feature + "_sc" + str(scale) + "_" + output + ".tif",
#             )

#             # Open file and write the code *
#             with open(os.path.join(output_directory, text_file), "a+") as file:
#                 file.write(
#                     'gdal_translate -b {} -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" {} {}\n\n'.format(
#                         band_count, vrt, output_tif
#                     )
#                 )


# # %%
# # Set text file name
# # NEED TO EDIT THIS LINE
# text_file = "vrt_to_bands.sh"

# # Group contextual features by scale *
# group_a = [
#     "gabor",
#     "hog",
#     "lac",
#     "lbpm",
#     "mean",
#     "ndvi",
#     "pantex",
# ]  # Scales of 30 m, 50 m, 70 m
# group_b = ["fourier", "orb", "sfs"]  # Scales of 31 m, 51 m, 71 m

# # Create list of contextual features
# spfeas = sorted(group_a + group_b)

# # Create dictionary of outputs for each feature
# # The number of bands differs based on contextual feature; more information
# # can be found at https://github.com/jgrss/spfeas.
# outputs = {
#     "fourier": ["mean", "variance"],
#     "gabor": [
#         "mean",
#         "variance",
#         "filter_1",
#         "filter_2",
#         "filter_3",
#         "filter_4",
#         "filter_5",
#         "filter_6",
#         "filter_7",
#         "filter_8",
#         "filter_9",
#         "filter_10",
#         "filter_11",
#         "filter_12",
#         "filter_13",
#         "filter_14",
#     ],
#     "hog": ["max", "mean", "variance", "skew", "kurtosis"],
#     "lac": ["lac"],
#     "lbpm": ["max", "mean", "variance", "skew", "kurtosis"],
#     "mean": ["mean", "variance"],
#     "ndvi": ["mean", "variance"],
#     "orb": ["max", "mean", "variance", "skew", "kurtosis"],
#     "pantex": ["min"],
#     "sfs": [
#         "max_line_length",
#         "min_line_length",
#         "mean",
#         "w_mean",
#         "std",
#         "max_ratio_of_orthogonal_angles",
#     ],
# }


# # Function writes the outputs to a text file in the format necessary to run .sh
# # Need to extract the VRT band-by-band and assign each band to its output name
# def write_outputs(scales):

#     # Set band count to 0
#     # The band count will help associate a band in the VRT to its output (i.e., variance)
#     # based on the spfeas package
#     band_count = 0

#     # Iterate through each scale, which contains multiple outputs
#     for scale in scales:

#         # Iterate through each output of the feature
#         for output in outputs[feature]:

#             # Open file and add comment indicating the scale and the output
#             with open(os.path.join(output_directory, text_file), "a+") as file:
#                 file.write("# scale: {}, output: {}\n".format(scale, output))

#             # Increase band count by 1
#             band_count = band_count + 1

#             # Create the relative path for the output tif that will be
#             # extracted from the VRT band-by-band *
#             output_tif = os.path.join(
#                 r"../outputs/band",
#                 feature + "_2021_09_01_2021_12_01",
#                 feature + "_sc" + str(scale) + "_" + output + ".tif",
#             )

#             # Open file and write the code *
#             with open(os.path.join(output_directory, text_file), "a+") as file:
#                 file.write(
#                     'gdal_translate -b {} -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" {} {}\n\n'.format(
#                         band_count, vrt, output_tif
#                     )
#                 )


# # Create a new file
# with open(os.path.join(output_directory, text_file), "w+") as file:
#     file.write("SH FILE TO CONVERT VRT INTO BANDS")


# # Iterate through each feature
# for feature in spfeas:
#     print("\n\nWorking on {}".format(feature))

#     # Add feature name as comment to file
#     with open(os.path.join(output_directory, text_file), "a+") as file:
#         file.write(
#             "\n\n\n############################\n\n\n# {}\n".format(feature.upper())
#         )

#     # Get folder where feature is located *
#     # NEED TO EDIT THIS LINE
#     folder = os.path.join(
#         output_directory, "features", """malawi_2021_09_01_2021_12_01_""" + feature
#     )

#     # Get VRT file
#     vrt = glob.glob(os.path.join(folder, "*.vrt*"))[0]  # Last asterisk not necessary

#     # Replace absolute path with relative path *
#     # vrt is global variable used above
#     vrt = vrt.replace(output_directory, r"../outputs/")

#     # Check which group feature is in and assign the appropriate scales to the
#     # write_output function
#     if feature in group_a:
#         write_outputs(scales=[3, 5, 7])
#     elif feature in group_b:
#         write_outputs(scales=[31, 51, 71])

#     # If feature is not assigned, let user know
#     else:
#         print("Feature not in a group!")

# print(
#     "\n\nOutput file located at: {}".format(os.path.join(output_directory, text_file))
# )
# print("\n\nDone.")
