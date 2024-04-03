# use geepy environment, run earthengine authenticate in commandline first
# %%
# requires https://cloud.google.com/sdk/docs/install
# and https://developers.google.com/earth-engine/guides/python_install-conda


import pendulum
import ee

from helpers import *

# run authenticate first time
# ee.Authenticate()
ee.Initialize()
import geetools


# Create and return the Earth Engine Polygon Geometry


f_north = open("./data/north_adm2.geojson")
fc_north = create_ee_polygon_from_geojson(f_north)

f_south = open("./data/south_adm2.geojson")
fc_south = create_ee_polygon_from_geojson(f_south)

# %%
# Set parameters
# bands = ["B2", "B3", "B4", "B8"]
bands = [
    "B4",
    "B3",
    "B2",
    "B8",
]
scale = 10
# date_pattern = "mm_dd_yyyy"  # dd: day, MMM: month (JAN), y: year
folder = "malawi_imagery"

# extra = dict(sat="Sen_TOA")    # low filter and threshold working well
CLOUD_FILTER = 75
CLD_PRB_THRESH = 25
NIR_DRK_THRESH = 0.3
CLD_PRJ_DIST = 3
BUFFER = 120
SCALE = 10
# B3_min_threshold = 6000  # below 2000 masks urban as clouds


# %% QUARTERLY COMPOSITES
for site, name in zip([fc_north, fc_south], ["north", "south"]):
    q_finished = []
    for year in list(range(2021, 2022)):  # 2024
        for month in list(range(1, 2)):  # 1, 13

            dt = pendulum.datetime(year, month, 1)
            # avoid repeating same quarter
            yq = f"{year}_{dt.quarter}"
            if yq in q_finished:
                # print('skipping')
                continue
            else:
                # print(f"appending {year}_{dt.quarter}")
                q_finished.append(f"{year}_{dt.quarter}")

            print(f"Year: {year} Quarter: {dt.quarter}")

            # filter by date and cloud cover
            collection = get_s2A_SR_sr_cld_collection(
                site,
                dt.first_of("quarter").strftime(r"%Y-%m-%d"),
                dt.last_of("quarter").strftime(r"%Y-%m-%d"),
                CLOUD_FILTER=CLOUD_FILTER,
            )

            # mask water if B8 in bands
            if "B8" in bands:
                collection = collection.map(mask_water)

            # add cloud and shadow mask
            s2_sr = (
                collection.map(
                    lambda image: add_cld_shdw_mask(
                        image,
                        CLD_PRB_THRESH=CLD_PRB_THRESH,
                        NIR_DRK_THRESH=NIR_DRK_THRESH,
                        SCALE=SCALE,
                        # B3_min_threshold=B3_min_threshold,
                    )
                )
                .map(apply_cld_shdw_mask)
                .select(bands)
                .median()
            )

            # Mask AOI
            # Create a mask from the AOI: 1 inside the geometry, 0 outside.
            aoi_mask = ee.Image.constant(1).clip(site.buffer(100)).mask()
            s2_sr = s2_sr.updateMask(aoi_mask)
            s2_sr = s2_sr.select(["B4", "B3", "B2"])
            # Convert to float32
            s2_sr = s2_sr.toFloat()

            # # export clipped result in Tiff
            crs = "EPSG:4326"

            img_name = f"S2_SR_{year}_Q{str(dt.quarter).zfill(2)}_{name}_CLOUDS{CLOUD_FILTER}_CLDPRB{CLD_PRB_THRESH}_NIR_DRK_THRESH{NIR_DRK_THRESH}_CLD_PRJ_DIST{CLD_PRJ_DIST}_BUFFER{BUFFER}"
            export_config = {
                "scale": scale,
                "maxPixels": 50000000000,
                "driveFolder": folder,
                "region": site,
            }
            task = ee.batch.Export.image(s2_sr, img_name, export_config)
            task.start()

# %%
