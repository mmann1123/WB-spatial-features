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
        with gw.config.update(bigtiff="yes"):
            with gw.series(
                a_grid,
                transfer_lib="jax",  # use jax takes longer to start but faster
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
                        f"{band_name}_S2_SR_interp_{interp_type}_{grid}.tif",
                    ),
                    num_workers=1,  # src.nchunks,
                    bands=1,
                    kwargs={"BIGTIFF": "YES"},
                )

# switch to geowombat env
# NOTE use 1a_create_mosaics.py next


# # %% calculate number of missing values in each quarter
# import os
# from glob import glob
# import re
# import numpy as np
# import xarray as xr
# import geowombat as gw
# import numpy as np

# os.chdir(r"/home/mmann1123/Desktop/mosaic")

# north = sorted(glob("*north.tif"))
# south = sorted(glob("*south.tif"))
# south
# # %%
# with gw.open(north[0]) as src:
#     display(np.isnan(src.sel(band=1)))
#     # print(np.isnan(src.sel(band=1)).values)
#     print(np.sum(np.isnan(src.sel(band=1)).values))
# %%
