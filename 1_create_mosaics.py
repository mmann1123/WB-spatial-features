# %% Interpolate missing values in the time series
#
# expected file structure:
# malawi_imagery_new (set current directory here)
# ├── B11
# │   ├── S2_SR_B11_2016_Q1_south-0000000000-0000000000.tif
# │   ├── S2_SR_B11_2016_Q1_south-0000000000-0000000001.tif
# │   ├── etc
# ├── B12
# │   ├── S2_SR_B12_2016_Q1_south-0000000000-0000000000.tif
# │   ├── S2_SR_B12_2016_Q1_south-0000000000-0000000001.tif
# │   ├── etc
# ├── B2
# │   ├── ....


from xr_fresh.interpolate_series import interpolate_nan
from numpy import nan
import os
from glob import glob
import re
import geowombat as gw

# interpolate missing values in the time series
missing_data = nan

bands = [
    "B2",
    "B3",
    "B4",
    "B8",
    "B11",
    "B12",
]

os.chdir(
    "/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new"
)  # "/mnt/bigdrive/Dropbox/wb_malawi")
output_dir = "interpolated"
os.makedirs(os.path.join(os.getcwd(), output_dir), exist_ok=True)

# set interpolation method
interp_type = "linear"

south_tiles = sorted(glob("./**/*S2_SR_*_south*.tif"))
south_tiles

# Get unique grid codes

pattern = r"((north|south)-\d+-\d+)(?=\.tif)"
unique_grids = list(
    set(
        [
            re.search(pattern, file_path).group()
            for file_path in south_tiles
            if re.search(pattern, file_path)
        ]
    )
)
unique_grids

# get unique year and quarter
pattern = r"\d{4}_Q\d{2}"
unique_quarters = sorted(
    list(
        set(
            [
                re.search(pattern, file_path).group()
                for file_path in south_tiles
                if re.search(pattern, file_path)
            ]
        )
    )
)
unique_quarters

# add north tiles to the unique grids
north_tiles = ["north"]
unique_grids += north_tiles
unique_grids
################################################
# %% interpolate missing values in the time series
for band_name in bands:
    files = f"./{band_name}"
    file_glob = f"{files}/*.tif"

    f_list = sorted(glob(file_glob))
    print(f_list)

    for grid in unique_grids:
        print("working on grid", grid)
        a_grid = sorted([f for f in f_list if grid in f])
        print("files:", a_grid)
        with gw.series(
            a_grid,
            transfer_lib="numpy",
            window_size=[512, 512],
        ) as src:
            src.apply(
                func=interpolate_nan(
                    interp_type=interp_type,
                    missing_value=missing_data,
                    count=len(src.filenames),
                ),
                outfile=os.path.join(
                    os.getcwd(),
                    output_dir,
                    f"{band_name}_S2_SR_linear_interp_{interp_type}_{grid}.tif",
                ),
                num_workers=1,  # src.nchunks,
                bands=1,
                kwargs={"BIGTIFF": "YES"},
            )

######################################
# %% convert multiband images to single band
from glob import glob
import os
import re
import geowombat as gw
from numpy import int16

os.chdir(r"/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/interpolated")
os.makedirs("../interpolated_monthly", exist_ok=True)
bands = [
    "B2",
    "B3",
    "B4",
    "B8",
    "B11",
    "B12",
]

for band_name in bands:
    file_glob = f"{band_name}_S2_SR_linear_interp_{interp_type}*.tif"

    f_list = sorted(glob(file_glob))
    print(f_list)

    # Get unique grid codes
    pattern = r"interp_*_(.+?)\.tif"

    unique_grids = sorted(
        list(
            set(
                [
                    re.search(pattern, file_path).group(1)
                    for file_path in f_list
                    if re.search(pattern, file_path)
                ]
            )
        )
    )
    print("unique_grids:", unique_grids)
    times = unique_quarters

    for stack, grid in zip(f_list, unique_grids):
        print(f"images: {stack}, grid: {grid} - check for consistency")
        with gw.open(stack) as src:
            for i in range(len(src)):
                print(f"working on {times[i]}")
                # display(src[i])
                gw.save(
                    src[i],
                    compress="LZW",
                    filename=f"../interpolated_monthly/{band_name}_S2_SR_{times[i]}-{grid}.tif",
                    num_workers=15,
                )

# %% mosaic souther tiles
import geowombat as gw
from glob import glob
import os
import re


os.chdir("/mnt/bigdrive/Dropbox/wb_malawi")
os.makedirs("mosaic", exist_ok=True)


south_tiles = sorted(glob("./tiles/S2_SR_*_south*.tif"))
south_tiles

# Get unique grid codes
pattern = r"(?<=-)\d+-\d+(?=\.tif)"
unique_grids = list(
    set(
        [
            re.search(pattern, file_path).group()
            for file_path in south_tiles
            if re.search(pattern, file_path)
        ]
    )
)
unique_grids

# get unique year and quarter
pattern = r"\d{4}_Q\d{2}"
unique_quarters = sorted(
    list(
        set(
            [
                re.search(pattern, file_path).group()
                for file_path in south_tiles
                if re.search(pattern, file_path)
            ]
        )
    )
)
unique_quarters

import geopandas as gpd
from numpy import nan
from rasterio.coords import BoundingBox

missing_data = nan

bounds = BoundingBox(*gpd.read_file("./boundaries/south_adm2.geojson").total_bounds)

# Print the unique codes
for quarter in unique_quarters:
    print("working on grid", quarter)

    a_quarter = sorted([f for f in south_tiles if quarter in f])
    print("files:", a_quarter)
    with gw.config.update(ref_bounds=bounds):
        with gw.open(a_quarter, mosaic=True, overlap="max") as src:

            gw.save(
                src,
                filename=f"./mosaic/S2_SR_{quarter}_south.tif",
                nodata=nan,
                overwrite=True,
                num_workers=12,
                compress="lzw",
            )

# %%
