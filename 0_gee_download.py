# use geepy environment, run earthengine authenticate in commandline first
#
# requires https://cloud.google.com/sdk/docs/install
# and https://developers.google.com/earth-engine/guides/python_install-conda
# %%

import ee
import pendulum

# from ipygee import *
from helpers import *

# run authenticate first time
# ee.Authenticate()
ee.Initialize()
# import geetools


# Create and return the Earth Engine Polygon Geometry
f_north = open("./data/north_adm2.geojson")
fc_north = create_ee_polygon_from_geojson(f_north)

f_south = open("./data/south_adm2.geojson")
fc_south = create_ee_polygon_from_geojson(f_south)


# %%
# Set parameters
bands = ["B2", "B3", "B4", "B8"]

# Cloud filter parameters
CLOUD_FILTER = 75  # 75  Maximum image cloud cover percent allowed in image collection
CLD_PRB_THRESH = 30  # 30 Cloud prob(%); values greater than are considered cloud
NIR_DRK_THRESH = 0.2  #  0.2  Minimum NIR refl to be considered potential cloud shadow

CLD_PRJ_DIST = 2  # 2 Maximum distance (km) to search for cloud shadows from cloud edges
BUFFER = 40  # was 40-50 A buffer around the AOI to apply cloud mask
folder = "malawi_imagery"
SCALE = 10


# %% QUARTERLY COMPOSITES
for site, name in zip([fc_north, fc_south], ["north", "south"]):

    q_finished = []
    for year in list(range(2021, 2024)):  # 2024
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

            # filter by date and cloud cover
            collection = get_s2A_SR_sr_cld_collection(
                site,
                dt.first_of("quarter").strftime(r"%Y-%m-%d"),
                dt.last_of("quarter").strftime(r"%Y-%m-%d"),
                CLOUD_FILTER=CLOUD_FILTER,
            )

            # add cloud and shadow mask
            s2_sr = (
                collection.map(
                    lambda image: add_cld_shdw_mask(
                        image,
                        CLD_PRB_THRESH=CLD_PRB_THRESH,
                        NIR_DRK_THRESH=NIR_DRK_THRESH,
                        CLD_PRJ_DIST=CLD_PRJ_DIST,
                        SCALE=SCALE,
                        BUFFER=BUFFER,
                    )
                )
                .map(apply_cld_shdw_mask)
                .select(bands)
                .median()
            )

            # Mask AOI
            # Create a mask from the AOI: 1 inside the geometry, 0 outside.
            aoi_mask = ee.Image.constant(1).clip(site.buffer(300)).mask()
            s2_sr = s2_sr.updateMask(aoi_mask)
            s2_sr = s2_sr.select(bands)
            # Convert to float32
            s2_sr = s2_sr.toFloat()

            # # export clipped result in Tiff
            img_name = f"S2_SR_{year}_Q{str(dt.quarter).zfill(2)}_{name}"
            export_config = {
                "scale": SCALE,
                "maxPixels": 50000000000,
                "driveFolder": folder,
                "region": site,
            }
            task = ee.batch.Export.image(s2_sr, img_name, export_config)
            task.start()


#  %% get time series bands of interest
# Red (Band 4): This band is sensitive to vegetation and can help differentiate between vegetated and non-vegetated areas within urban environments.
# NIR (Near Infrared - Band 8): NIR is useful for distinguishing between different types of surfaces based on their reflectance properties. Urban areas typically have low NIR reflectance due to the presence of buildings and pavement.
# SWIR (Shortwave Infrared - Bands 11 and 12): SWIR bands are sensitive to moisture content and surface roughness, which can help identify materials like concrete and asphalt commonly found in urban areas.
# SWIR2 (Shortwave Infrared - Band 12): This band is particularly useful for mapping urban materials with low reflectance in the visible and NIR regions, such as asphalt and dark rooftops.

# Set parameters
bands = [
    "B4",
    "B8",
    "B11",
    "B12",
]  # red and NIR already in other see 4_time_series_features.py

# Cloud filter parameters
CLOUD_FILTER = 75  # 75  Maximum image cloud cover percent allowed in image collection
CLD_PRB_THRESH = 30  # 30 Cloud prob(%); values greater than are considered cloud
NIR_DRK_THRESH = 0.2  #  0.2  Minimum NIR refl to be considered potential cloud shadow

CLD_PRJ_DIST = 2  # 2 Maximum distance (km) to search for cloud shadows from cloud edges
BUFFER = 40  # was 40-50 A buffer around the AOI to apply cloud mask
folder = "malawi_imagery"
SCALE = 10


# %% ANNUAL COMPOSITES
for band in bands:
    for site, name in zip([fc_north, fc_south], ["north", "south"]):
        for year in list(range(2019, 2024)):  # 2024

            dt = pendulum.datetime(year, 1, 1)

            print(f"Year: {year}")

            # filter by date and cloud cover
            collection = get_s2A_SR_sr_cld_collection(
                site,
                dt.first_of("year").strftime(r"%Y-%m-%d"),
                dt.last_of("year").strftime(r"%Y-%m-%d"),
                CLOUD_FILTER=CLOUD_FILTER,
            )

            # add cloud and shadow mask
            s2_sr = (
                collection.map(
                    lambda image: add_cld_shdw_mask(
                        image,
                        CLD_PRB_THRESH=CLD_PRB_THRESH,
                        NIR_DRK_THRESH=NIR_DRK_THRESH,
                        CLD_PRJ_DIST=CLD_PRJ_DIST,
                        SCALE=SCALE,
                        BUFFER=BUFFER,
                    )
                )
                .map(apply_cld_shdw_mask)
                .select(bands)
                .median()
            )

            # Mask AOI
            # Create a mask from the AOI: 1 inside the geometry, 0 outside.
            aoi_mask = ee.Image.constant(1).clip(site.buffer(300)).mask()
            s2_sr = s2_sr.updateMask(aoi_mask)
            s2_sr = s2_sr.select(band)
            # Convert to float32
            s2_sr = s2_sr.toFloat()

            # # export clipped result in Tiff
            img_name = f"{band}_S2_SR_{year}_{name}"
            export_config = {
                "scale": SCALE,
                "maxPixels": 50000000000,
                "driveFolder": folder,
                "region": site,
            }
            task = ee.batch.Export.image(s2_sr, img_name, export_config)
            task.start()


# %% QUARTERLY COMPOSITES
for band in bands:
    for site, name in zip([fc_north, fc_south], ["north", "south"]):
        q_finished = []
        for year in list(range(2021, 2024)):  # 2024
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

                # filter by date and cloud cover
                collection = get_s2A_SR_sr_cld_collection(
                    site,
                    dt.first_of("quarter").strftime(r"%Y-%m-%d"),
                    dt.last_of("quarter").strftime(r"%Y-%m-%d"),
                    CLOUD_FILTER=CLOUD_FILTER,
                )

                # add cloud and shadow mask
                s2_sr = (
                    collection.map(
                        lambda image: add_cld_shdw_mask(
                            image,
                            CLD_PRB_THRESH=CLD_PRB_THRESH,
                            NIR_DRK_THRESH=NIR_DRK_THRESH,
                            CLD_PRJ_DIST=CLD_PRJ_DIST,
                            SCALE=SCALE,
                            BUFFER=BUFFER,
                        )
                    )
                    .map(apply_cld_shdw_mask)
                    .select(bands)
                    .median()
                )

                # Mask AOI
                # Create a mask from the AOI: 1 inside the geometry, 0 outside.
                aoi_mask = ee.Image.constant(1).clip(site.buffer(300)).mask()
                s2_sr = s2_sr.updateMask(aoi_mask)
                s2_sr = s2_sr.select(band)
                # Convert to float32
                s2_sr = s2_sr.toFloat()

                # # export clipped result in Tiff
                img_name = f"{band}_S2_SR_{year}_Q{str(dt.quarter).zfill(2)}_{name}"
                export_config = {
                    "scale": SCALE,
                    "maxPixels": 50000000000,
                    "driveFolder": folder,
                    "region": site,
                }
                task = ee.batch.Export.image(s2_sr, img_name, export_config)
                task.start()


# %% Visualize

# # Import the folium library.
# import folium


# # Define a method for displaying Earth Engine image tiles to a folium map.
# def add_ee_layer(
#     self, ee_image_object, vis_params, name, show=True, opacity=1, min_zoom=0
# ):
#     map_id_dict = ee.Image(ee_image_object).getMapId(vis_params)
#     folium.raster_layers.TileLayer(
#         tiles=map_id_dict["tile_fetcher"].url_format,
#         attr='Map Data &copy; <a href="https://earthengine.google.com/">Google Earth Engine</a>',
#         name=name,
#         show=show,
#         opacity=opacity,
#         min_zoom=min_zoom,
#         overlay=True,
#         control=True,
#     ).add_to(self)


# # Add the Earth Engine layer method to folium.
# folium.Map.add_ee_layer = add_ee_layer


# def display_cloud_layers(col):
#     # Mosaic the image collection.
#     img = col.mosaic()

#     # Subset layers and prepare them for display.
#     clouds = img.select("clouds").selfMask()
#     shadows = img.select("shadows").selfMask()
#     dark_pixels = img.select("dark_pixels").selfMask()
#     probability = img.select("probability")
#     cloudmask = img.select("cloudmask").selfMask()
#     cloud_transform = img.select("cloud_transform")

#     # Create a folium map object.
#     center = AOI.centroid(10).coordinates().reverse().getInfo()
#     m = folium.Map(location=center, zoom_start=12)

#     # Add layers to the folium map.
#     m.add_ee_layer(
#         img,
#         {"bands": ["B4", "B3", "B2"], "min": 0, "max": 2500, "gamma": 1.1},
#         "S2 image",
#         True,
#         1,
#         9,
#     )
#     m.add_ee_layer(
#         probability, {"min": 0, "max": 100}, "probability (cloud)", False, 1, 9
#     )
#     m.add_ee_layer(clouds, {"palette": "e056fd"}, "clouds", False, 1, 9)
#     m.add_ee_layer(
#         cloud_transform,
#         {"min": 0, "max": 1, "palette": ["white", "black"]},
#         "cloud_transform",
#         False,
#         1,
#         9,
#     )
#     m.add_ee_layer(dark_pixels, {"palette": "orange"}, "dark_pixels", False, 1, 9)
#     m.add_ee_layer(shadows, {"palette": "yellow"}, "shadows", False, 1, 9)
#     m.add_ee_layer(cloudmask, {"palette": "orange"}, "cloudmask", True, 0.5, 9)

#     # Add a layer control panel to the map.
#     m.add_child(folium.LayerControl())

#     # Display the map.
#     display(m)


# # %%
# s2_sr_cld_col_eval_disp = s2_sr_cld_col_eval.map(add_cld_shdw_mask)

# display_cloud_layers(s2_sr_cld_col_eval_disp)

# # def quarterly_composites(start_year, end_year, aoi):
# #     for site, name in zip([fc_north, fc_south], ["north", "south"]):
# #         if name == "north":
# #             continue
# #         for year in range(start_year, end_year + 1)[0:1]:
#             for quarter in range(1, 5)[0:1]:

#                 start_date, end_date = pendulum.datetime(
#                     year, 3 * quarter - 2, 1
#                 ).first_of("quarter").strftime(r"%Y-%m-%d"), pendulum.datetime(
#                     year, 3 * quarter, 1
#                 ).last_of(
#                     "quarter"
#                 ).strftime(
#                     r"%Y-%m-%d"
#                 )
#                 print(start_date, end_date)

#                 current_quarter = pendulum.datetime(year, 3 * quarter - 2, 1).quarter

#                 # filter by date and cloud cover
#                 s2_sr_col = get_s2A_SR_sr_cld_collection(
#                     site,
#                     start_date,
#                     end_date,
#                     CLOUD_FILTER=CLOUD_FILTER,
#                 )

#                 # add cloud and shadow mask
#                 s2_sr = s2_sr_col.map(
#                     lambda image: apply_masks(
#                         image,
#                         CLD_PRB_THRESH=CLD_PRB_THRESH,
#                         NIR_DRK_THRESH=NIR_DRK_THRESH,
#                         CLD_PRJ_DIST=CLD_PRJ_DIST,
#                         BUFFER=50,
#                     )
#                 ).select(["B4", "B3", "B2"])

#                 display(s2_sr.getInfo())

#                 s2_sr_median = s2_sr_col.median()

#                 # Create a mask from the AOI: 1 inside the geometry, 0 outside.
#                 aoi_mask = ee.Image.constant(1).clip(site.buffer(100)).mask()
#                 s2_sr_median = s2_sr_median.updateMask(aoi_mask)

#                 # Convert to float32
#                 s2_sr_median = s2_sr_median.toFloat()

#                 img_name = f"S2_SR_{year}_Q{str(current_quarter).zfill(2)}_{name}_CLOUDS{CLOUD_FILTER}_CLDPRB{CLD_PRB_THRESH}_NIR_DRK_THRESH{NIR_DRK_THRESH}_CLD_PRJ_DIST{CLD_PRJ_DIST}_BUFFER{BUFFER}"
#                 export_config = {
#                     "scale": scale,
#                     "maxPixels": 50000000000,
#                     "driveFolder": folder,
#                     "region": site,
#                 }
#                 task = ee.batch.Export.image(s2_sr_median, img_name, export_config)
#                 task.start()


# # Example usage: Create quarterly composites between 2001 and 2003 for Malawi
# quarterly_composites(2021, 2022, AOI)


# %%
