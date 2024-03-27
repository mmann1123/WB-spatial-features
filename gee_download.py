# use geepy environment, run earthengine authenticate in commandline first
# %%
# requires https://cloud.google.com/sdk/docs/install
# and https://developers.google.com/earth-engine/guides/python_install-conda


import pendulum
import ee

from helpers import *

# from ipygee import *
# import ipygee as ui

ee.Initialize()
import geetools
from geetools import ui, cloud_mask, batch


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

north_path = "./data/north_adm2.geojson"

try:
    north = create_ee_polygon_from_geojson(north_path)
    print(north.getInfo())  # Print the polygon's info to verify
except Exception as e:
    print(f"Error: {e}")

south_path = "./data/south_adm2.geojson"

try:
    south = create_ee_polygon_from_geojson(south_path)
    print(south.getInfo())  # Print the polygon's info to verify
except Exception as e:
    print(f"Error: {e}")

# Set parameters
bands = ["B2", "B3", "B4", "B8"]
scale = 10
# date_pattern = "mm_dd_yyyy"  # dd: day, MMM: month (JAN), y: year
folder = "malawi_imagery"

region = site
# extra = dict(sat="Sen_TOA")
CLOUD_FILTER = 75


# %% QUARTERLY COMPOSITES
# North
q_finished = []
for year in list(range(2021, 2023)):
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
        s2_sr = geetools.batch.utils.convertDataType("uint32")(s2_sr)
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
