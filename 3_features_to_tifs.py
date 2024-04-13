# %%
# Import modules
import os
from glob import glob


# need to generate the following code
# # FOURIER
# # scale: 31, output: mean
# gdal_translate -b 1 -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" ../outputs//features/malawi_2021_09_01_2021_12_01_fourier/south_2021_09_01_2021_12_01__BD1_BK1_SC31-51-71_TRfourier.vrt ../outputs/band/fourier_2021_09_01_2021_12_01/fourier_sc31_mean.tif

# # scale: 31, output: variance
# gdal_translate -b 2 -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" ../outputs//features/malawi_2021_09_01_2021_12_01_fourier/south_2021_09_01_2021_12_01__BD1_BK1_SC31-51-71_TRfourier.vrt ../outputs/band/fourier_2021_09_01_2021_12_01/fourier_sc31_variance.tif

# # scale: 51, output: mean
# gdal_translate -b 3 -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" ../outputs//features/malawi_2021_09_01_2021_12_01_fourier/south_2021_09_01_2021_12_01__BD1_BK1_SC31-51-71_TRfourier.vrt ../outputs/band/fourier_2021_09_01_2021_12_01/fourier_sc51_mean.tif

# # scale: 51, output: variance
# gdal_translate -b 4 -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" ../outputs//features/malawi_2021_09_01_2021_12_01_fourier/south_2021_09_01_2021_12_01__BD1_BK1_SC31-51-71_TRfourier.vrt ../outputs/band/fourier_2021_09_01_2021_12_01/fourier_sc51_variance.tif

# # scale: 71, output: mean
# gdal_translate -b 5 -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" ../outputs//features/malawi_2021_09_01_2021_12_01_fourier/south_2021_09_01_2021_12_01__BD1_BK1_SC31-51-71_TRfourier.vrt ../outputs/band/fourier_2021_09_01_2021_12_01/fourier_sc71_mean.tif

# # scale: 71, output: variance
# gdal_translate -b 6 -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" ../outputs//features/malawi_2021_09_01_2021_12_01_fourier/south_2021_09_01_2021_12_01__BD1_BK1_SC31-51-71_TRfourier.vrt ../outputs/band/fourier_2021_09_01_2021_12_01/fourier_sc71_variance.tif


# Set directory that contains all the feature VRTs (the parent output folder) *
# Make sure there is end slash
# NEED TO EDIT THIS LINE
output_directory = "/mnt/bigdrive/Dropbox/wb_malawi/test"  ##r"/CCAS/groups/engstromgrp/mike/spfeas_outputs/features"

os.chdir(output_directory)


# Get all VRT paths
vrt_paths = glob(os.path.join(output_directory, "*/*.vrt"))
print(vrt_paths)


# %%
# Set text file name
# NEED TO EDIT THIS LINE
text_file = "vrt_to_bands.sh"

# Group contextual features by scale *
group_a = [
    "gabor",
    "hog",
    "lac",
    "lbpm",
    "mean",
    "ndvi",
    "pantex",
]  # Scales of 30 m, 50 m, 70 m
group_b = ["fourier", "orb", "sfs"]  # Scales of 31 m, 51 m, 71 m

# Create list of contextual features
spfeas = sorted(group_a + group_b)

# Create dictionary of outputs for each feature
# The number of bands differs based on contextual feature; more information
# can be found at https://github.com/jgrss/spfeas.
outputs = {
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


# Function writes the outputs to a text file in the format necessary to run .sh
# Need to extract the VRT band-by-band and assign each band to its output name
def write_outputs(scales):

    # Set band count to 0
    # The band count will help associate a band in the VRT to its output (i.e., variance)
    # based on the spfeas package
    band_count = 0

    # Iterate through each scale, which contains multiple outputs
    for scale in scales:

        # Iterate through each output of the feature
        for output in outputs[feature]:

            # Open file and add comment indicating the scale and the output
            with open(os.path.join(output_directory, text_file), "a+") as file:
                file.write("# scale: {}, output: {}\n".format(scale, output))

            # Increase band count by 1
            band_count = band_count + 1

            # Create the relative path for the output tif that will be
            # extracted from the VRT band-by-band *
            output_tif = os.path.join(
                r"../outputs/band",
                feature + "_2021_09_01_2021_12_01",
                feature + "_sc" + str(scale) + "_" + output + ".tif",
            )

            # Open file and write the code *
            with open(os.path.join(output_directory, text_file), "a+") as file:
                file.write(
                    'gdal_translate -b {} -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" {} {}\n\n'.format(
                        band_count, vrt, output_tif
                    )
                )


# Create a new file
with open(os.path.join(output_directory, text_file), "w+") as file:
    file.write("SH FILE TO CONVERT VRT INTO BANDS")


# Iterate through each feature
for feature in spfeas:
    print("\n\nWorking on {}".format(feature))

    # Add feature name as comment to file
    with open(os.path.join(output_directory, text_file), "a+") as file:
        file.write(
            "\n\n\n############################\n\n\n# {}\n".format(feature.upper())
        )

    # Get folder where feature is located *
    # NEED TO EDIT THIS LINE
    folder = os.path.join(
        output_directory, "features", """malawi_2021_09_01_2021_12_01_""" + feature
    )

    # Get VRT file
    vrt = glob.glob(os.path.join(folder, "*.vrt*"))[0]  # Last asterisk not necessary

    # Replace absolute path with relative path *
    # vrt is global variable used above
    vrt = vrt.replace(output_directory, r"../outputs/")

    # Check which group feature is in and assign the appropriate scales to the
    # write_output function
    if feature in group_a:
        write_outputs(scales=[3, 5, 7])
    elif feature in group_b:
        write_outputs(scales=[31, 51, 71])

    # If feature is not assigned, let user know
    else:
        print("Feature not in a group!")

print(
    "\n\nOutput file located at: {}".format(os.path.join(output_directory, text_file))
)
print("\n\nDone.")
