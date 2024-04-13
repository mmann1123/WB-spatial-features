import re
import os
from glob import glob


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
