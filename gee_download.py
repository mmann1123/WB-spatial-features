# use geepy environment, run earthengine authenticate in commandline first
# %%
# requires https://cloud.google.com/sdk/docs/install
# and https://developers.google.com/earth-engine/guides/python_install-conda


import ee
import pendulum
from ipygee import *
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
 

# Define your Area of Interest (AOI)
AOI = ee.FeatureCollection("FAO/GAUL/2015/level0").filter(
    ee.Filter.eq("ADM0_NAME", "Malawi")
)

# Cloud filter parameters
CLOUD_FILTER = 85  # Maximum image cloud cover percent allowed in image collection
CLD_PRB_THRESH = 30  # Cloud probability (%); values greater than are considered cloud
NIR_DRK_THRESH = 0.15
CLD_PRJ_DIST = 1
BUFFER = 50
folder = "malawi_imagery"
scale = 10


# Add cloud probability and is_cloud bands to the image
def add_cloud_bands(img,CLD_PRB_THRESH):
    cld_prb = ee.Image(img.get("s2cloudless")).select("probability")
    is_cloud = cld_prb.gt(CLD_PRB_THRESH).rename("clouds")
    return img.addBands(ee.Image([cld_prb, is_cloud]))


# Add shadow bands to the image
def add_shadow_bands(img,NIR_DRK_THRESH, CLD_PRJ_DIST):
    not_water = img.select("SCL").neq(6)
    dark_pixels = (
        img.select("B8")
        .lt(NIR_DRK_THRESH * 1e4)
        .multiply(not_water)
        .rename("dark_pixels")
    )
    shadow_azimuth = ee.Number(90).subtract(
        ee.Number(img.get("MEAN_SOLAR_AZIMUTH_ANGLE"))
    )
    cld_proj = (
        img.select("clouds")
        .directionalDistanceTransform(shadow_azimuth, CLD_PRJ_DIST * 10)
        .reproject( 'EPSG:4326', None, 20)
        .select("distance")
        .mask()
        .rename("cloud_transform")
    )
    shadows = cld_proj.multiply(dark_pixels).rename("shadows")
    return img.addBands(ee.Image([dark_pixels, cld_proj, shadows]))


# Combine cloud and shadow mask, and add water mask
def apply_masks(img,CLD_PRB_THRESH=30,
                            NIR_DRK_THRESH=0.15,
                            CLD_PRJ_DIST=1,
                            BUFFER = 50):
    img_cloud = add_cloud_bands(img, CLD_PRB_THRESH)
    img_cloud_shadow = add_shadow_bands(img_cloud,NIR_DRK_THRESH, CLD_PRJ_DIST)
    is_cld_shdw = (
        img_cloud_shadow.select("clouds").add(img_cloud_shadow.select("shadows")).gt(0)
    )
    is_cld_shdw_masked = (
        is_cld_shdw.focal_min(2)
        .focal_max(BUFFER * 2 / 20)
        .reproject(**{"crs": img.select([0]).projection(), "scale": 20})
        .rename("cloudmask")
    )
    not_cld_shdw = is_cld_shdw_masked.Not()
    img_masked = img.updateMask(not_cld_shdw)

    # Mask water
    ndwi = img.normalizedDifference(["B3", "B8"])
    water_mask = ndwi.gt(0).Not()
    img_masked = img_masked.updateMask(water_mask)
    return img_masked


def get_s2A_SR_sr_cld_collection(
    aoi, start_date, end_date, product="S2_SR", CLOUD_FILTER=50
):
    print("get_s2A_SR_sr_cld_collection")
    print("Cloud Filter:", CLOUD_FILTER)

    # Import and filter S2 SR.
    s2_sr_col = (
        ee.ImageCollection("COPERNICUS/" + product)
        .filterBounds(aoi)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", CLOUD_FILTER))
    )

    # Import and filter s2cloudless.
    s2_cloudless_col = (
        ee.ImageCollection("COPERNICUS/S2_CLOUD_PROBABILITY")
        .filterBounds(aoi)
        .filterDate(start_date, end_date)
    )

    # Join the filtered s2cloudless collection to the SR collection by the 'system:index' property.
    return ee.ImageCollection(
        ee.Join.saveFirst("s2cloudless").apply(
            **{
                "primary": s2_sr_col,
                "secondary": s2_cloudless_col,
                "condition": ee.Filter.equals(
                    **{"leftField": "system:index", "rightField": "system:index"}
                ),
            }
        )
    )

def quarterly_composites(start_year, end_year, aoi):
    for site, name in zip([fc_north, fc_south], ["north", "south"]):
        if name == "north":
            continue
        for year in range(start_year, end_year + 1)[0:1]:
            for quarter in range(1, 5)[0:1]:

                start_date, end_date = pendulum.datetime(
                    year, 3 * quarter - 2, 1
                ).first_of("quarter").strftime(r"%Y-%m-%d"), pendulum.datetime(
                    year, 3 * quarter, 1
                ).last_of(
                    "quarter"
                ).strftime(
                    r"%Y-%m-%d"
                )
                print(start_date, end_date)
  
                current_quarter = pendulum.datetime(year, 3 * quarter - 2, 1).quarter
  
  
                  # filter by date and cloud cover
                s2_sr_col = get_s2A_SR_sr_cld_collection(
                    site,
                    start_date,
                    end_date,
                    CLOUD_FILTER=CLOUD_FILTER,
                )

                # add cloud and shadow mask
                s2_sr = (
                    s2_sr_col.map(
                        lambda image: apply_masks(
                            image,
                            CLD_PRB_THRESH=CLD_PRB_THRESH,
                            NIR_DRK_THRESH=NIR_DRK_THRESH,
                            CLD_PRJ_DIST=CLD_PRJ_DIST,
                            BUFFER = 50
                        )
                    )
                    .select(["B4", "B3", "B2"])
                )
 
                display(s2_sr.getInfo())

                s2_sr_median = s2_sr_col.median()

                # Create a mask from the AOI: 1 inside the geometry, 0 outside.
                aoi_mask = ee.Image.constant(1).clip(site.buffer(100)).mask()
                s2_sr_median = s2_sr_median.updateMask(aoi_mask)

                # Convert to float32
                s2_sr_median = s2_sr_median.toFloat()


                img_name = f"S2_SR_{year}_Q{str(current_quarter).zfill(2)}_{name}_CLOUDS{CLOUD_FILTER}_CLDPRB{CLD_PRB_THRESH}_NIR_DRK_THRESH{NIR_DRK_THRESH}_CLD_PRJ_DIST{CLD_PRJ_DIST}_BUFFER{BUFFER}"
                export_config = {
                    "scale": scale,
                    "maxPixels": 50000000000,
                    "driveFolder": folder,
                    "region": site,
                }
                task = ee.batch.Export.image(s2_sr_median, img_name, export_config)
                task.start()


# Example usage: Create quarterly composites between 2001 and 2003 for Malawi
quarterly_composites(2021, 2022, AOI)


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

#Cloud filter parameters
CLOUD_FILTER = 85  # Maximum image cloud cover percent allowed in image collection
CLD_PRB_THRESH = 30  # Cloud probability (%); values greater than are considered cloud
NIR_DRK_THRESH = 0.15
CLD_PRJ_DIST = 1
BUFFER = 50
folder = "malawi_imagery"
SCALE = 10


# %% QUARTERLY COMPOSITES
for site, name in zip([fc_north, fc_south], ["north", "south"]):
    q_finished = []
    for year in list(range(2021, 2022)):  # 2024
        for month in list(range(1, 4)):  # 1, 13

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
            s2_sr = s2_sr.select(["B4", "B3", "B2"])
            # Convert to float32
            s2_sr = s2_sr.toFloat()

            # # export clipped result in Tiff
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
