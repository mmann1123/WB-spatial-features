#%% Interpolate missing values in the time series

from xr_fresh.interpolate_series import interpolate_nan
from numpy import nan

missing_data = nan

bands = [
    "B2",
    "B3",
    "B4",
    "B8",
    "B11",
    "B12",
] 

os.chdir("/mnt/bigdrive/Dropbox/wb_malawi")
os.makedirs("interpolated", exist_ok=True)

north_tiles = sorted(glob("./tiles/*S2_SR_*_north*.tif"))

south_tiles = sorted(glob("./tiles/*S2_SR_*_south*.tif"))
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
unique_quarters = sorted(list(
    set(
        [
            re.search(pattern, file_path).group()
            for file_path in south_tiles
            if re.search(pattern, file_path)
        ]
    )
))
unique_quarters

# add north tiles to the unique grids
unique_grids += north_tiles

for band_name in bands:
    files = f"./{band_name}"
    file_glob = f"{files}/*.tif"

    f_list = sorted(glob(file_glob))
    print(f_list)

    for grid in unique_grids:
        print("working on grid", grid)
        a_grid = sorted([f for f in files if grid in f])
        print('files:' , a_grid)
        with gw.series(
            a_grid,
            transfer_lib="numpy",
            window_size=[512, 512],
        ) as src:
            src.apply(
                func=interpolate_nan(
                    interp_type="linear",
                    missing_value=missing_data,
                    count=len(src.filenames),
                ),
                outfile=os.path.join(
                os.getcwd(),
                "interpolated",
                f"{band_name}_S2_SR_linear_interp_{grid}.tif",
            ),
                num_workers=1,  # src.nchunks,
                bands=1,
                kwargs={"BIGTIFF": "YES"},
            )

# %% 
import geowombat as gw
from glob import glob
import os
import re


os.chdir("/mnt/bigdrive/Dropbox/wb_malawi")
os.makedirs("interpolated", exist_ok=True)


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
unique_quarters = sorted(list(
    set(
        [
            re.search(pattern, file_path).group()
            for file_path in south_tiles
            if re.search(pattern, file_path)
        ]
    )
))
unique_quarters

# %%
!mkdir -p mosaic
import geopandas as gpd
from numpy import nan
from rasterio.coords import BoundingBox
missing_data = nan

bounds = BoundingBox(*gpd.read_file('./boundaries/south_adm2.geojson').total_bounds)

# Print the unique codes
for quarter in unique_quarters:
        print("working on grid", quarter)

        a_quarter = sorted([f for f in south_tiles if quarter in f])
        print('files:' , a_quarter)
        with gw.config.update(ref_bounds =bounds):
            with gw.open(a_quarter, mosaic=True,overlap='max' ) as src:

                gw.save(src, filename=f'./mosaic/S2_SR_{quarter}_south.tif',
                                nodata=nan,
                                overwrite=True,
                                num_workers=6, 
                                compress='lzw')
#%%
        


# %%
