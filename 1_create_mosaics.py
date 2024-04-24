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
# NEED TO RERUN FOR B11 AND B12

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
interp_type = "linear"

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
        with gw.config.update(bigtiff="YES"):
            with gw.open(stack) as src:
                for i in range(len(src)):
                    print(f"working on {times[i]}")
                    # display(src[i])
                    src = src.fillna(src.mean())
                    gw.save(
                        src[i],
                        compress="LZW",
                        filename=f"../interpolated_monthly/{band_name}_S2_SR_{times[i]}-{grid}.tif",
                        num_workers=15,
                        overwrite=True,
                    )


# %% mosaic bgrn and southern tiles

import geowombat as gw
from glob import glob
import os
import re
from numpy import nan
import os
from numpy import nan
from helpers import bounds_tiler, list_files_pattern

os.environ["GDAL_CACHEMAX"] = "6144"  # Set 512 MB for GDAL cache

os.chdir(r"/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/interpolated_monthly")
os.makedirs("../stacks", exist_ok=True)


images = sorted(glob("*.tif"))
images

# Get unique grid codes
# pattern = r"(?<=-)\d+-\d+(?=\.tif)" #gets just southern codes
pattern = r"linear_*_(.+?)\.tif"

unique_grids = list_files_pattern(images, pattern)
unique_grids

# get unique year and quarter
pattern = r"\d{4}_Q\d{2}"
unique_quarters = list_files_pattern(images, pattern)
unique_quarters

missing_data = nan


# Print the unique codes
for quarter in unique_quarters[0:1]:
    print("working on grid", quarter)
    # subset a quarter
    a_quarter = sorted([f for f in images if quarter in f])

    for grid in unique_grids[1:2]:
        a_zone = sorted([f for f in a_quarter if grid in f])
        # Define the bands in the desired order
        band_order = ["B2", "B3", "B4", "B8"]

        # Filter and sort the list
        bgrn = sorted(
            (f for f in a_zone if any(f.startswith(b) for b in band_order)),
            key=lambda x: band_order.index(re.match(r"B\d+", x).group(0)),
        )

        bounds_list = bounds_tiler(bgrn, max_area=2.5e10)
        for bounds in bounds_list:
            print("files:", bgrn)
            with gw.config.update(bigtiff="YES", ref_bounds=bounds):
                with gw.open(bgrn, stack_dim="band") as src:
                    src = src.astype("float32")
                    display(src)
                    # src = src.fillna(0.5)

                    src.gw.save(
                        filename=f"../stacks/S2_SR_{quarter}_{grid.split('.tif')[0]}_{round(bounds[1],2)}_{round(bounds[3],2)}.tif",
                        nodata=nan,
                        overwrite=True,
                        num_workers=12,
                        compress="lzw",
                    )
# %% attempt bgrn mosaic from interpolated stacks
import geowombat as gw
import os
import xarray as xr
from helpers import bounds_tiler
from numpy import nan

os.chdir(r"/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/interpolated")
from geowombat.backends.rasterio_ import get_file_bounds

bgrn = [
    "B2_S2_SR_linear_interp_linear_south-0000000000-0000000000.tif",
    "B3_S2_SR_linear_interp_linear_south-0000000000-0000000000.tif",
    "B4_S2_SR_linear_interp_linear_south-0000000000-0000000000.tif",
    "B8_S2_SR_linear_interp_linear_south-0000000000-0000000000.tif",
]

with gw.open(bgrn[0]) as B2:
    with gw.open(bgrn[1]) as B3:
        with gw.open(bgrn[2]) as B4:
            with gw.open(bgrn[3]) as B8:
                # concatenate the bands
                bands = [B2.sel(band=1), B3.sel(band=1), B4.sel(band=1), B8.sel(band=1)]
                out = xr.concat(bands, dim="band")
                out.attrs = B2.attrs
                with gw.config.update(bigtiff="YES", ref_bounds=[bgrn[2]]):
                    out = out.chunk({"band": -1, "y": 2048, "x": 2048})
                    display(out)

                    out.gw.to_raster(
                        filename=f"test.tif",
                        nodata=nan,
                        overwrite=True,
                        num_workers=20,
                        compress="lzw",
                    )
                # bounds_list = bounds_tiler(bgrn, max_area=2.5e10)
                # for bounds in bounds_list:
                #     print("files:", bgrn)
                #     with gw.config.update(bigtiff="YES", ref_bounds=bounds):
                #         with gw.open(bgrn, stack_dim="band") as src:
                #             src = src.astype("float32")
                #             display(src)
                #             # src = src.fillna(0.5)

                #             src.gw.save(
                #                 filename=f"../stacks/test_{round(bounds[1],2)}_{round(bounds[3],2)}.tif",
                #                 nodata=nan,
                #                 overwrite=True,
                #                 num_workers=12,
                #                 compress="lzw",
                #             )

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
