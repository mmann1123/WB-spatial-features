import re
import os
from glob import glob


def remove_file(file):
    os.remove(file)


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

# %%
partitions = {
    "defq": "14-00:00:0",
    "short": "1-00:00:00",
    "short-384gb": "1-00:00:00",
    "tiny": "4:00:00",
    "nano": "30:00",
    "384gb": "14-00:00:0",
    "highMem": "14-00:00:0",
    "highThru": "7-00:00:00",
    "graphical": "4:00:00",
    "debug": "4:00:00",
    "debug-cpu": "4:00:00",
    "debug-gpu": "4:00:00",
    "ultra-gpu": "7-00:00:00",
    "large-gpu": "7-00:00:00",
    "med-gpu": "7-00:00:00",
    "small-gpu": "7-00:00:00",
    "awscpu": "infinite",
    "awsgpu*": "infinite",
}


# create function that checks partition name and time limit against the partitions dictionary
# where the time limit is coverted to seconds and checked if it is less than the time limit
def check_partition_time(partition, time_limit_requested):
    if len(time_limit_requested) < 11:
        raise ValueError(
            "Time limit must be in the format DD-HH:MM:SS for zero days use 00-HH:MM:SS"
        )

    if partition in partitions:
        time_limit_seconds = convert_time_to_seconds(partitions[partition])
        if time_limit_seconds > convert_time_to_seconds(time_limit_requested):
            print("Passed slurm checks")
            pass
        else:
            raise ValueError(f"Allocation time is too long: {partitions}")
    else:
        raise ValueError(
            f"Partition not found in partitions dictionary: {partitions.keys()}"
        )


# write a function that converts 7-01:05:00 to seconds
def convert_time_to_seconds(time):
    days, rest = time.split("-")
    hours, minutes, seconds = rest.split(":")
    days = int(days)
    hours = int(hours)
    minutes = int(minutes)
    seconds = int(seconds)
    total_seconds = days * 24 * 60 * 60 + hours * 60 * 60 + minutes * 60 + seconds
    return total_seconds


# %%
