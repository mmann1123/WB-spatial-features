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
    # "B11",
    # "B12",
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

# location of the interpolated image stacks
os.chdir(r"/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/interpolated")
os.makedirs("../stacks", exist_ok=True)

# output location
output_dir = "../mosaic"
os.makedirs(output_dir, exist_ok=True)

# get image stacks
images = sorted(glob("*.tif"))
images

# Get unique grid codes
# pattern = r"(?<=-)\d+-\d+(?=\.tif)" #gets just southern codes
pattern = r"linear_*_(.+?)\.tif"

unique_grids = list_files_pattern(images, pattern)
unique_grids

# get unique year and quarter
pattern = r"\d{4}_Q\d{2}"
unique_quarters = list_files_pattern(glob("../B2/B2*.tif"), pattern)
unique_quarters

# remove years 2020 and 2024 from unique_quarters (only used for interpolation)
unique_quarters = [q for q in unique_quarters if "2020" not in q and "2024" not in q]

# Define the bands in the desired order
band_order = ["B2", "B3", "B4", "B8"]


# for grid in unique_grids:
for grid in unique_grids[1:2]:
    # get zone for name
    north_south = re.search(r"(north|south)", grid, re.IGNORECASE).group(0)

    # isolate the grid
    a_grid = sorted([f for f in images if grid in f])

    # pattern = r"linear_*_(.+?)\.tif"
    pattern = r"linear_([^_]+?)\.tif"
    grid_code = list_files_pattern(a_grid, pattern)

    # Filter and sort the list
    bgrn = sorted(
        (f for f in a_grid if any(f.startswith(b) for b in band_order)),
        key=lambda x: band_order.index(re.match(r"B\d+", x).group(0)),
    )
    # get list of smaller bounds if needed
    bounds_list = bounds_tiler(bgrn, max_area=8e10)
    print(f"Breaking into {len(bounds_list)} bounds boxes:")

    for quarter in unique_quarters:
        for bounds in bounds_list:
            with gw.config.update(bigtiff="YES", ref_bounds=bounds):

                # open each band seperately and interate over quarters
                with gw.open(bgrn[0], band_names=unique_quarters) as B2:
                    B2 = B2.fillna(
                        B2.mean()
                    )  # fill missing values that remain on edges
                    with gw.open(bgrn[1], band_names=unique_quarters) as B3:
                        B3 = B3.fillna(B3.mean())
                        with gw.open(bgrn[2], band_names=unique_quarters) as B4:
                            B4 = B4.fillna(B4.mean())
                            with gw.open(bgrn[3], band_names=unique_quarters) as B8:
                                B8 = B8.fillna(B8.mean())

                                # stack the bands
                                bands = [
                                    B2.sel(band=quarter),
                                    B3.sel(band=quarter),
                                    B4.sel(band=quarter),
                                    B8.sel(band=quarter),
                                ]
                                out = xr.concat(bands, dim="band")
                                out.attrs = B2.attrs

                                print("files:", bgrn)
                                out = out.astype("float32")
                                display(out)
                                # src = src.fillna(0.5)

                                out_name = f"{output_dir}/S2_SR_{quarter}_{north_south}_zone_{str(abs(round(bounds[1],2))).replace('.','-')}_{str(abs(round(bounds[3],2))).replace('.','-')}.tif"
                                out.gw.save(
                                    filename=out_name,
                                    nodata=nan,
                                    overwrite=True,
                                    num_workers=19,
                                    compress="lzw",
                                )

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
