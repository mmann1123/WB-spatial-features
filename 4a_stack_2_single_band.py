# geoombwat env
# convert multiband images to single band
from glob import glob
import os
import geowombat as gw
import os
from helpers import list_files_pattern
from numpy import nan
from glob import glob
from geowombat.backends.rasterio_ import get_file_bounds
import dask.array as da
import argparse


def main():

    parser = argparse.ArgumentParser(description="stack bgrn bands into a mosaic")

    parser.add_argument("north_or_south", type=str, help="type 'north' or 'south'")
    args = parser.parse_args()

    # location of the interpolated image stacks
    # os.chdir(r"/CCAS/groups/engstromgrp/mike/interpolated/")
    os.chdir(r"/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/interpolated")

    # output location
    output_dir = "../single_band_mosaics"
    os.makedirs(output_dir, exist_ok=True)

    # get image stacks
    images = glob(f"*.tif")
    images

    # get unique year and quarter
    unique_quarters = [
        "2020_Q01",
        "2020_Q02",
        "2020_Q03",
        "2020_Q04",
        "2021_Q01",
        "2021_Q02",
        "2021_Q03",
        "2021_Q04",
        "2022_Q01",
        "2022_Q02",
        "2022_Q03",
        "2022_Q04",
        "2023_Q01",
        "2023_Q02",
        "2023_Q03",
        "2023_Q04",
        "2024_Q01",
        "2024_Q02",
    ]

    # Define the bands in the desired order
    band_order = ["B8", "B11", "B12"]

    # get zone from args
    grid = args.north_or_south

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
            "2024_Q01",
            "2024_Q02",
        ]:
            continue
        print("working on quarter:", quarter, "north_south:", grid)

        if grid == "south":
            with gw.config.update(bigtiff="YES", ref_bounds=bounds):

                for i, value in enumerate(bgrn):

                    # open each band seperately and interate over quarters
                    with gw.open(
                        value[0],
                        band_names=unique_quarters,
                    ) as Banda:
                        Banda = Banda.sel(band=quarter)
                        with gw.open(
                            value[1],
                            band_names=unique_quarters,
                        ) as Bandb:
                            Bandb = Bandb.sel(band=quarter)
                            Band = da.maximum(
                                Banda, Bandb
                            )  # mosaic not working w very large files use instead
                            Band = Band.fillna(Band.mean(skipna=True))
                            out = Band * 10000
                            out.attrs = Band.attrs

                            print("files:", bgrn)
                            out = out.astype("int16")

                            out_name = f"{output_dir}/{band_order[i]}_S2_SR_{quarter}_{grid}.tif"
                            print(out_name)
                            out.gw.save(
                                filename=out_name,
                                nodata=nan,
                                overwrite=True,
                                num_workers=19,
                                compress="lzw",
                            )
        else:
            with gw.config.update(bigtiff="YES", ref_bounds=bounds):

                for i, value in enumerate(bgrn):

                    # open each band seperately and interate over quarters
                    with gw.open(
                        value[0],
                        band_names=unique_quarters,
                    ) as Banda:
                        Banda = Banda.sel(band=quarter)
                        Band = Banda.fillna(Banda.mean(skipna=True))
                        out = Band * 10000
                        out.attrs = Band.attrs

                        print("files:", bgrn)
                        out = out.astype("int16")

                        out_name = (
                            f"{output_dir}/{band_order[i]}_S2_SR_{quarter}_{grid}.tif"
                        )
                        print(out_name)
                        out.gw.save(
                            filename=out_name,
                            nodata=nan,
                            overwrite=True,
                            num_workers=19,
                            compress="lzw",
                        )


if __name__ == "__main__":
    main()
# Path: 4b_stack_2_single_band.py
