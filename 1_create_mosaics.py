# %% Interpolate missing values in the time series
## author: Michael Mann mmann1123@gwu.edu

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

# tifs should be in floating point 32 or 64

from xr_fresh.interpolate_series import interpolate_nan
from numpy import nan
import os
from glob import glob
import re
import geowombat as gw

# interpolate missing values in the time series
missing_data = nan

bands = [
    # "B2",
    # "B3",
    # "B4",
    # "B8",
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

# %% attempt bgrn mosaic from interpolated stacks
import geowombat as gw
import os
import xarray as xr
from helpers import bounds_tiler, list_files_pattern
from numpy import nan
from glob import glob
import re
from geowombat.backends.rasterio_ import get_file_bounds
from matplotlib import pyplot as plt
import numpy as np
import dask.array as da

# location of the interpolated image stacks
os.chdir(r"/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/interpolated")
os.makedirs("../stacks", exist_ok=True)

# output location
output_dir = "../mosaic"
os.makedirs(output_dir, exist_ok=True)

# get image stacks
images = sorted(glob("*.tif"))
images


# get unique year and quarter
pattern = r"\d{4}_Q\d{2}"
unique_quarters = list_files_pattern(glob("../B2/B2*.tif"), pattern)
unique_quarters


# Define the bands in the desired order
band_order = ["B2", "B3", "B4", "B8"]


# for grid in unique_grids:
for grid in ["south", "north"]:
    # get zone for name
    north_south = grid  # re.search(r"(north|south)", grid, re.IGNORECASE).group(0)

    # isolate the grid
    a_grid = sorted([f for f in images if grid in f])

    # pattern = r"linear_*_(.+?)\.tif"
    pattern = r"linear_([^_]+?)\.tif"
    grid_code = list_files_pattern(a_grid, pattern)

    # get list of files for each band in order B2, B3, B4, B8
    bgrn = [sorted([f for f in a_grid if f"{band}" in f]) for band in band_order]
    bounds = get_file_bounds(
        bgrn[0],
        bounds_by="union",
        return_bounds=True,
    )

    for quarter in unique_quarters:
        # skip unnecessary quarters
        if quarter in [
            "2020_Q01",
            "2020_Q02",
            "2020_Q03",
            "2020_Q04",
            "2024_Q01",
            "2024_Q02",
        ]:
            continue
        print("working on quarter:", quarter, "north_south:", north_south)

        if north_south == "south":
            with gw.config.update(bigtiff="YES", ref_bounds=bounds):

                # open each band seperately and interate over quarters
                with gw.open(
                    bgrn[0][0], band_names=unique_quarters, mosaic=True, overlap="max"
                ) as B2a:
                    B2a = B2a.sel(band=quarter)
                    with gw.open(
                        bgrn[0][1],
                        band_names=unique_quarters,
                        mosaic=True,
                        overlap="max",
                    ) as B2b:
                        B2b = B2b.sel(band=quarter)
                        B2 = da.maximum(
                            B2a, B2b
                        )  # mosaic not working w very large files use instead
                        B2 = B2.fillna(B2.mean(skipna=True))

                with gw.open(
                    bgrn[1][0],
                    band_names=unique_quarters,
                    mosaic=True,
                    overlap="max",
                ) as B3a:
                    B3a = B3a.sel(band=quarter)
                    with gw.open(
                        bgrn[1][1],
                        band_names=unique_quarters,
                        mosaic=True,
                        overlap="max",
                    ) as B3b:
                        B3b = B3b.sel(band=quarter)
                        B3 = da.maximum(B3a, B3b)
                        B3 = B3.fillna(B3.mean(skipna=True))

                with gw.open(
                    bgrn[2][0],
                    band_names=unique_quarters,
                    mosaic=True,
                    overlap="max",
                ) as B4a:
                    B4a = B4a.sel(band=quarter)
                    with gw.open(
                        bgrn[2][1],
                        band_names=unique_quarters,
                        mosaic=True,
                        overlap="max",
                    ) as B4b:
                        B4b = B4b.sel(band=quarter)
                        B4 = da.maximum(B4a, B4b)
                        B4 = B4.fillna(B4.mean(skipna=True))

                with gw.open(
                    bgrn[3][0],
                    band_names=unique_quarters,
                    mosaic=True,
                    overlap="max",
                ) as B8a:
                    B8a = B8a.sel(band=quarter)
                    with gw.open(
                        bgrn[3][1],
                        band_names=unique_quarters,
                        mosaic=True,
                        overlap="max",
                    ) as B8b:
                        B8b = B8b.sel(band=quarter)
                        B8 = da.maximum(B8a, B8b)
                        B8 = B8.fillna(B8.mean(skipna=True))

                bands = [B2, B3, B4, B8]
                out = xr.concat(bands, dim="band")
                out.attrs = B2.attrs

                print("files:", bgrn)
                out = out.astype("float32")
                display(out)

                out_name = f"{output_dir}/S2_SR_{quarter}_{north_south}.tif"
                out.gw.save(
                    filename=out_name,
                    nodata=nan,
                    overwrite=True,
                    num_workers=19,
                    compress="lzw",
                )
        else:
            with gw.open(
                bgrn[0][0],
                band_names=unique_quarters,
            ) as B2:

                B2 = B2.sel(band=quarter)
                B2 = B2.fillna(B2.mean(skipna=True))

            with gw.open(
                bgrn[1][0],
                band_names=unique_quarters,
            ) as B3:
                B3 = B3.sel(band=quarter)
                B3 = B3.fillna(B3.mean(skipna=True))

            with gw.open(
                bgrn[2][0],
                band_names=unique_quarters,
            ) as B4:
                B4 = B4.sel(band=quarter)
                B4 = B4.fillna(B4.mean(skipna=True))

            with gw.open(
                bgrn[3][0],
                band_names=unique_quarters,
            ) as B8:
                B8 = B8.sel(band=quarter)
                B8 = B8.fillna(B8.mean(skipna=True))  # stack the bands

            bands = [B2, B3, B4, B8]
            out = xr.concat(bands, dim="band")
            out.attrs = B2.attrs

            print("files:", bgrn)
            out = out.astype("float32")
            display(out)

            out_name = f"{output_dir}/S2_SR_{quarter}_{north_south}.tif"
            out.gw.save(
                filename=out_name,
                nodata=nan,
                overwrite=True,
                num_workers=19,
                compress="lzw",
            )

# %%
# upload to server using rsync
# individual file
# scp -v /local/path/example.txt mmann1123@pegasus.arc.gwu.edu:/path/to/remote/destination
# scp -v /mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/mosaic/S2_SR_2021_Q01_south.tif mmann1123@pegasus.arc.gwu.edu:/CCAS/groups/engstromgrp/mike/mosaic/S2_SR_2021_Q01_south.tif
# scp mmann1123@pegasus.arc.gwu.edu:/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/mosaic/S2_SR_2021_Q01_south.tif /CCAS/groups/engstromgrp/mike/mosaic/S2_SR_2021_Q01_south.tif

# sync a folder
# rsync -avz /path/to/local/folder/ username@remote_server:/path/to/remote/folder/


# %%
# # %% attempt bgrn mosaic from interpolated stacks
# import geowombat as gw
# import os
# import xarray as xr
# from helpers import bounds_tiler, list_files_pattern
# from numpy import nan
# from glob import glob
# import re
# from geowombat.backends.rasterio_ import get_file_bounds

# # location of the interpolated image stacks
# os.chdir(r"/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/interpolated")
# os.makedirs("../stacks", exist_ok=True)

# # output location
# output_dir = "../mosaic"
# os.makedirs(output_dir, exist_ok=True)

# # get image stacks
# images = sorted(glob("*.tif"))
# images


# # get unique year and quarter
# pattern = r"\d{4}_Q\d{2}"
# unique_quarters = list_files_pattern(glob("../B2/B2*.tif"), pattern)
# unique_quarters


# # Define the bands in the desired order
# band_order = ["B2", "B3", "B4", "B8"]


# # for grid in unique_grids:
# for grid in ["south", "north"]:
#     # get zone for name
#     north_south = re.search(r"(north|south)", grid, re.IGNORECASE).group(0)

#     # isolate the grid
#     a_grid = sorted([f for f in images if grid in f])

#     # pattern = r"linear_*_(.+?)\.tif"
#     pattern = r"linear_([^_]+?)\.tif"
#     grid_code = list_files_pattern(a_grid, pattern)

#     # get list of files for each band in order B2, B3, B4, B8
#     bgrn = [sorted([f for f in a_grid if f"{band}" in f]) for band in band_order]
#     bounds = get_file_bounds(
#         bgrn[0],
#         bounds_by="union",
#         return_bounds=True,
#     )

#     for quarter in unique_quarters:
#         # skip unnecessary quarters
#         if quarter in [
#             "2020_Q01",
#             "2020_Q02",
#             "2020_Q03",
#             "2020_Q04",
#             "2024_Q01",
#             "2024_Q02",
#         ]:
#             continue
#         print("working on quarter:", quarter, "north_south:", north_south)

#         with gw.config.update(bigtiff="YES", ref_bounds=bounds):

#             # open each band seperately and interate over quarters
#             with gw.open(
#                 bgrn[0], band_names=unique_quarters, mosaic=True, overlap="max"
#             ) as B2:
#                 B2 = B2.sel(band=quarter)
#                 B2 = B2.fillna(
#                     B2.mean(skipna=True)
#                 )  # fill missing values that remain on edges
#                 with gw.open(
#                     bgrn[1], band_names=unique_quarters, mosaic=True, overlap="max"
#                 ) as B3:
#                     B3 = B3.sel(band=quarter)
#                     B3 = B3.fillna(B3.mean(skipna=True))
#                     with gw.open(
#                         bgrn[2], band_names=unique_quarters, mosaic=True, overlap="max"
#                     ) as B4:
#                         B4 = B4.sel(band=quarter)
#                         B4 = B4.fillna(B4.mean(skipna=True))
#                         with gw.open(
#                             bgrn[3],
#                             band_names=unique_quarters,
#                             mosaic=True,
#                             overlap="max",
#                         ) as B8:
#                             B8 = B8.sel(band=quarter)
#                             B8 = B8.fillna(B8.mean(skipna=True))

#                             # stack the bands
#                             bands = [
#                                 B2,
#                                 B3,
#                                 B4,
#                                 B8,
#                             ]
#                             out = xr.concat(bands, dim="band")
#                             out.attrs = B2.attrs

#                             print("files:", bgrn)
#                             out = out.astype("float32")
#                             display(out)
#                             # src = src.fillna(0.5)

#                             out_name = f"{output_dir}/S2_SR_{quarter}_{north_south}.tif"
#                             out.gw.save(
#                                 filename=out_name,
#                                 nodata=nan,
#                                 overwrite=True,
#                                 num_workers=19,
#                                 compress="lzw",
#                             )
# %%
import dask.array as da
from geowombat.backends.rasterio_ import get_file_bounds
from geowombat.data import (
    l8_224077_20200518_B2,
    l8_224078_20200518_B2,
    l8_224077_20200518_B3,
    l8_224078_20200518_B3,
)

# Get the union of all bounding boxes
# Note that you probably want to pass `crs` in this method.
bounds = get_file_bounds(
    [l8_224077_20200518_B2, l8_224078_20200518_B2],
    bounds_by="union",
    return_bounds=True,
)
fig, ax = plt.subplots(dpi=200)

with gw.config.update(ref_bounds=bounds):
    with gw.open(
        [l8_224077_20200518_B2, l8_224078_20200518_B2],
        band_names=[1],
        mosaic=True,
    ) as src1:
        with gw.open(
            [l8_224077_20200518_B3, l8_224078_20200518_B3],
            band_names=[2],
            mosaic=True,
        ) as src2:
            darray = xr.concat([src1, src2], dim="band")
            darray.sel(band=1).gw.imshow(robust=True, ax=ax)


# ######################################
# %% convert multiband images to single band
# # NEED TO RERUN FOR B11 AND B12

# from glob import glob
# import os
# import re
# import geowombat as gw
# from numpy import int16

# os.chdir(r"/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/interpolated")
# os.makedirs("../interpolated_monthly", exist_ok=True)
# bands = [
#     "B2",
#     "B3",
#     "B4",
#     "B8",
#     "B11",
#     "B12",
# ]
# interp_type = "linear"

# for band_name in bands:
#     file_glob = f"{band_name}_S2_SR_linear_interp_{interp_type}*.tif"

#     f_list = sorted(glob(file_glob))
#     print(f_list)

#     # Get unique grid codes
#     pattern = r"interp_*_(.+?)\.tif"

#     unique_grids = sorted(
#         list(
#             set(
#                 [
#                     re.search(pattern, file_path).group(1)
#                     for file_path in f_list
#                     if re.search(pattern, file_path)
#                 ]
#             )
#         )
#     )
#     print("unique_grids:", unique_grids)
#     times = unique_quarters

#     for stack, grid in zip(f_list, unique_grids):
#         print(f"images: {stack}, grid: {grid} - check for consistency")
#         with gw.config.update(bigtiff="YES"):
#             with gw.open(stack) as src:
#                 for i in range(len(src)):
#                     print(f"working on {times[i]}")
#                     # display(src[i])
#                     src = src.fillna(src.mean())
#                     gw.save(
#                         src[i],
#                         compress="LZW",
#                         filename=f"../interpolated_monthly/{band_name}_S2_SR_{times[i]}-{grid}.tif",
#                         num_workers=15,
#                         overwrite=True,
#                     )


# # %% mosaic bgrn and southern tiles

# import geowombat as gw
# from glob import glob
# import os
# import re
# from numpy import nan
# import os
# from numpy import nan
# from helpers import bounds_tiler, list_files_pattern

# os.environ["GDAL_CACHEMAX"] = "6144"  # Set 512 MB for GDAL cache

# os.chdir(r"/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/interpolated_monthly")
# os.makedirs("../stacks", exist_ok=True)


# images = sorted(glob("*.tif"))
# images

# # Get unique grid codes
# # pattern = r"(?<=-)\d+-\d+(?=\.tif)" #gets just southern codes
# pattern = r"linear_*_(.+?)\.tif"

# unique_grids = list_files_pattern(images, pattern)
# unique_grids

# # get unique year and quarter
# pattern = r"\d{4}_Q\d{2}"
# unique_quarters = list_files_pattern(images, pattern)
# unique_quarters

# missing_data = nan


# # Print the unique codes
# for quarter in unique_quarters[0:1]:
#     print("working on grid", quarter)
#     # subset a quarter
#     a_quarter = sorted([f for f in images if quarter in f])

#     for grid in unique_grids[1:2]:
#         a_zone = sorted([f for f in a_quarter if grid in f])
#         # Define the bands in the desired order
#         band_order = ["B2", "B3", "B4", "B8"]

#         # Filter and sort the list
#         bgrn = sorted(
#             (f for f in a_zone if any(f.startswith(b) for b in band_order)),
#             key=lambda x: band_order.index(re.match(r"B\d+", x).group(0)),
#         )

#         bounds_list = bounds_tiler(bgrn, max_area=2.5e10)
#         for bounds in bounds_list:
#             print("files:", bgrn)
#             with gw.config.update(bigtiff="YES", ref_bounds=bounds):
#                 with gw.open(bgrn, stack_dim="band") as src:
#                     src = src.astype("float32")
#                     display(src)
#                     # src = src.fillna(0.5)

#                     src.gw.save(
#                         filename=f"../stacks/S2_SR_{quarter}_{grid.split('.tif')[0]}_{round(bounds[1],2)}_{round(bounds[3],2)}.tif",
#                         nodata=nan,
#                         overwrite=True,
#                         num_workers=12,
#                         compress="lzw",
#                     )

# %% mosaic bgrn and southern tiles

# import geowombat as gw
# from glob import glob
# import os
# import re
# import geopandas as gpd
# from numpy import nan
# from rasterio.coords import BoundingBox

# os.chdir(r"/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/interpolated_monthly")
# os.makedirs("../mosaic", exist_ok=True)
# bands = [
#     "B2",
#     "B3",
#     "B4",
#     "B8",
#     "B11",
#     "B12",
# ]

# images = sorted(glob("*.tif"))
# images

# # Get unique grid codes
# # pattern = r"(?<=-)\d+-\d+(?=\.tif)" #gets just southern codes
# # pattern = r"linear_*_(.+?)\.tif"

# # unique_grids = list(
# #     set(
# #         [
# #             re.search(pattern, file_path).group()
# #             for file_path in images
# #             if re.search(pattern, file_path)
# #         ]
# #     )
# # )
# # unique_grids

# # get unique year and quarter
# pattern = r"\d{4}_Q\d{2}"
# unique_quarters = sorted(
#     list(
#         set(
#             [
#                 re.search(pattern, file_path).group()
#                 for file_path in images
#                 if re.search(pattern, file_path)
#             ]
#         )
#     )
# )
# unique_quarters

# missing_data = nan

# # bounds = BoundingBox(*gpd.read_file("../../boundaries/south_adm2.geojson").total_bounds)

# # Print the unique codes
# for quarter in unique_quarters:
#     print("working on grid", quarter)
#     # subset a quarter
#     a_quarter = sorted([f for f in images if quarter in f])

#     for zone in ["south", "north"]:
#         a_zone = sorted([f for f in a_quarter if zone in f])
#         # Define the bands in the desired order
#         band_order = ["B2", "B3", "B4", "B8"]

#         # Filter and sort the list
#         bgrn = sorted(
#             (f for f in a_zone if any(f.startswith(b) for b in band_order)),
#             key=lambda x: band_order.index(re.match(r"B\d+", x).group(0)),
#         )
#         print("files:", bgrn)

#         if zone == "north":
#             with gw.open(bgrn, stack_dim="band") as src:
#                 gw.save(
#                     src,
#                     filename=f"../mosaic/S2_SR_{quarter}_north.tif",
#                     nodata=nan,
#                     overwrite=True,
#                     num_workers=12,
#                     compress="lzw",
#                 )
#         if zone == "south":
#             # Splitting the list into two halves with alternating entries
#             part1 = bgrn[0::2]
#             part2 = bgrn[1::2]

#             # with gw.config.update(ref_bounds=bounds):
#             with gw.open(part1, stack_dim="band") as src1:
#                 src1.attrs["crs"] = src1.gw.crs_to_pyproj.to_wkt()
#                 src1.gw.to_vrt("../lat_lon_file1.vrt")
#                 with gw.open(part2, stack_dim="band") as src2:
#                     src1.attrs["crs"] = src1.gw.crs_to_pyproj.to_wkt()
#                     src1.gw.to_vrt("../lat_lon_file2.vrt")

#                     with gw.open(
#                         ["../lat_lon_file1.vrt", "../lat_lon_file2.vrt"],
#                         mosaic=True,
#                         overlap="max",
#                     ) as out:

#                         gw.save(
#                             out,
#                             filename=f"../mosaic/S2_SR_{quarter}_south.tif",
#                             nodata=nan,
#                             overwrite=True,
#                             num_workers=12,
#                             compress="lzw",
#                         )


# %%
