# switch to geowombat env
# %% attempt bgrn mosaic from interpolated stacks


def main():
    import argparse

    parser = argparse.ArgumentParser(description="stack bgrn bands into a mosaic")

    parser.add_argument("north_or_south", type=str, help="type 'north' or 'south'")
    args = parser.parse_args()

    import geowombat as gw
    import os
    from helpers import list_files_pattern
    from numpy import nan
    from glob import glob
    import re
    from geowombat.backends.rasterio_ import get_file_bounds
    from xarray import concat

    # location of the interpolated image stacks
    # os.chdir(r"/CCAS/groups/engstromgrp/mike/interpolated/")
    os.chdir(r"/mnt/bigdrive/Dropbox/wb_malawi/malawi_imagery_new/interpolated")

    # output location
    output_dir = "../mosaic"
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
    band_order = ["B2", "B3", "B4", "B8"]

    grid = args.north_or_south
    # get zone for name
    north_south = re.search(r"(north|south)", grid, re.IGNORECASE).group(0)

    # isolate the grid
    a_grid = sorted([f for f in images if grid in f])

    # get list of files for each band in order B2, B3, B4, B8
    bgrn = [sorted([f for f in a_grid if f"{band}" in f]) for band in band_order]
    bounds = get_file_bounds(
        bgrn[0],
        bounds_by="union",
        return_bounds=True,
    )
    print("bgrn:", bgrn)

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
        with gw.config.update(bigtiff="YES", ref_bounds=bounds):

            # open each band seperately and interate over quarters
            with gw.open(
                bgrn[0], band_names=unique_quarters, mosaic=True, overlap="max"
            ) as B2:
                B2 = B2.fillna(
                    B2.mean(skipna=True)
                )  # fill missing values that remain on edges
                with gw.open(
                    bgrn[1], band_names=unique_quarters, mosaic=True, overlap="max"
                ) as B3:
                    B3 = B3.fillna(B3.mean(skipna=True))
                    with gw.open(
                        bgrn[2],
                        band_names=unique_quarters,
                        mosaic=True,
                        overlap="max",
                    ) as B4:
                        B4 = B4.fillna(B4.mean(skipna=True))
                        with gw.open(
                            bgrn[3],
                            band_names=unique_quarters,
                            mosaic=True,
                            overlap="max",
                        ) as B8:
                            B8 = B8.fillna(B8.mean(skipna=True))

                            # stack the bands
                            bands = [
                                B2.sel(band=quarter),
                                B3.sel(band=quarter),
                                B4.sel(band=quarter),
                                B8.sel(band=quarter),
                            ]
                            out = concat(bands, dim="band")
                            out.attrs = B2.attrs

                            print("files:", bgrn)
                            out = out.astype("float32")

                            out_name = f"{output_dir}/S2_SR_{quarter}_{north_south}.tif"
                            out.gw.save(
                                filename=out_name,
                                nodata=nan,
                                overwrite=True,
                                num_workers=19,
                                compress="lzw",
                            )


if __name__ == "__main__":
    main()
