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


# ## Define an ImageCollection
# site = ee.Geometry.Polygon(
#     [
#         [34.291442183768666, -6.361841661400507],
#         [36.543639449393666, -6.361841661400507],
#         [36.543639449393666, -4.07537105824348],
#         [34.291442183768666, -4.07537105824348],
#         [34.291442183768666, -6.361841661400507],
#     ]
# )


f_north = open("./data/north_adm2.geojson")
shapefile_north = json.load(f_north)
features_north = shapefile_north["features"]
fc_north = ee.FeatureCollection(features_north)

f_south = open("./data/south_adm2.geojson")
shapefile_south = json.load(f_south)
features_south = shapefile_south["features"]
fc_south = ee.FeatureCollection(features_south)

# Set parameters
bands = ["B2", "B3", "B4", "B8"]
scale = 10
# date_pattern = "mm_dd_yyyy"  # dd: day, MMM: month (JAN), y: year
folder = "malawi_imagery"

# extra = dict(sat="Sen_TOA")
CLOUD_FILTER = 75


# %% QUARTERLY COMPOSITES
for site in [fc_north, fc_south]:
    q_finished = []
    for year in list(range(2021, 2024)):
        for month in list(range(1, 13)):

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

            collection = get_s2A_SR_sr_cld_col(
                site,
                dt.first_of("quarter").strftime(r"%Y-%m-%d"),
                dt.last_of("quarter").strftime(r"%Y-%m-%d"),
                CLOUD_FILTER=CLOUD_FILTER,
            )

            s2_sr = (
                collection.map(add_cld_shdw_mask)
                .map(apply_cld_shdw_mask)
                .select(bands)
                .median()
            )
            # s2_sr = geetools.batch.utils.convertDataType("uint16")(s2_sr)
            # eprint(s2_sr)

            # # export clipped result in Tiff
            crs = "EPSG:4326"

            img_name = f"S2_SR_{year}_Q{str(dt.quarter).zfill(2)}"
            export_config = {
                "scale": scale,
                "maxPixels": 50000000000,
                "driveFolder": folder,
                "region": site,
            }
            task = ee.batch.Export.image(s2_sr, img_name, export_config)
            task.start()

# %%
