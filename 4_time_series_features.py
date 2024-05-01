# %% xr_fresh env
# before this run 4a_stack_2_single_band.py
import geowombat as gw
import os, sys

sys.path.append("/home/mmann1123/Documents/github/xr_fresh/")
from glob import glob
from datetime import datetime

from xr_fresh.feature_calculator_series import *
from xr_fresh.feature_calculator_series import function_mapping
import numpy as np
import re
import logging
from pathlib import Path
from helpers import get_quarter_dates

zones = ["north", "south"]
output_path = "../time_features"

complete_times_series_list = {
    "abs_energy": [{}],
    "absolute_sum_of_changes": [{}],
    "autocorr": [{"lag": 1}, {"lag": 2}, {"lag": 3}],
    "kurtosis": [{}],
    "large_standard_deviation": [{}],
    "maximum": [{}],
    "mean_abs_change": [{}],
    "mean_change": [{}],
    "mean_second_derivative_central": [{}],
    "median": [{}],
    "minimum": [{}],
    "quantile": [{"q": 0.05}, {"q": 0.95}],
    "ratio_beyond_r_sigma": [{"r": 1}, {"r": 2}],
    "skewness": [{}],
    "standard_deviation": [{}],
    "ts_complexity_cid_ce": [{}],
    "variance": [{}],
}

os.chdir("/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/single_band_mosaics/")
images = sorted(glob("*.tif"))
print(images)
# %%

# # add data notes
try:
    Path(f"{output_path}").mkdir(parents=True)
except FileExistsError:
    print(f"The interpolation directory already exists. Skipping.")

with open(f"{output_path}/0_notes.txt", "a") as the_file:
    the_file.write(
        "Gererated by  github/YM_TZ_crop_classifier/2_xr_fresh_extraction.py \t"
    )
    the_file.write(str(datetime.now()))
# Set up logging
logging.basicConfig(
    filename=f"{output_path}/error_log.log",
    level=logging.ERROR,
    format="%(asctime)s:%(levelname)s:%(message)s",
)


for zone in zones:
    a_zone = sorted([f for f in images if zone in f])

    for band_name in ["B12", "B11", "B8"]:

        # isolate the grid
        a_band = sorted([f for f in a_zone if band_name in f])
        # print(a_band)

        # loop through moving window of 4 lagged images in a_band
        for i in range(0, len(a_band) - 4, 1):
            # estimating for rolling periods of:
            a_period = a_band[i : i + 5]
            print([os.path.basename(x) for x in a_period])

            # get end period name
            end_period = re.search(r"_(\d{4}_Q\d{2})", a_period[-1]).group(1)

            # Extract information from the filename
            match = re.match(
                r"B(\d+)_S2_SR_(\d{4}_Q\d{2})_(north|south)\.tif",
                os.path.basename(a_period[-1]),
            )
            if match:
                band = match.group(1)
                quarter = match.group(2)
                direction = match.group(3)
                start, end = get_quarter_dates(quarter)

            print(f"working on {band_name} {a_period}")
            with gw.series(
                a_period,
                window_size=[512, 512],  # transfer_lib="numpy"
                nodata=np.nan,
            ) as src:
                # iterate across functions
                for func_name, param_list in complete_times_series_list.items():
                    for params in param_list:
                        # instantiate function
                        func_class = function_mapping.get(func_name)
                        if func_class:
                            func_instance = func_class(
                                **params
                            )  # Instantiate with parameters
                            if len(params) > 0:
                                print(f"Instantiated {func_name} with  {params}")
                            else:
                                print(f"Instantiated {func_name} ")

                        # create output directories file name
                        if len(list(params.keys())) > 0:
                            key_names = list(params.keys())[0]
                            value_names = list(params.values())[0]

                            dir_path = os.path.join(
                                output_path,
                                zone,
                                band_name,
                                f"{band_name}_{func_name}-{key_names}-{value_names}_{start}_{end}",
                            )
                            os.makedirs(dir_path, exist_ok=True)

                            outfile = f"{dir_path}/{band_name}_{func_name}_{key_names}_{value_names}.tif"

                        else:
                            dir_path = os.path.join(
                                output_path,
                                zone,
                                band_name,
                                f"{func_name}_{start}_{end}",
                            )
                            os.makedirs(dir_path, exist_ok=True)
                            outfile = f"{dir_path}/{band_name}_{func_name}.tif"
                        # extract features
                        try:
                            src.apply(
                                func=func_instance,
                                outfile=outfile,
                                num_workers=3,
                                processes=False,
                                bands=1,
                                kwargs={"BIGTIFF": "YES", "compress": "LZW"},
                            )
                        except Exception as e:
                            logging.error(
                                f"Error extracting features from {band_name} {func_name} {a_period[-1]}: {e}"
                            )
                            continue
# %%
