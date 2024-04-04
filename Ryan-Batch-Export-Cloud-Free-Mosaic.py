# %%
import ee
import json
from helpers import create_ee_polygon_from_geojson

# # Trigger the authentication flow.
# ee.Authenticate()

# # Initialize the library.
ee.Initialize()

import json
import sys


# Create and return the Earth Engine Polygon Geometry
f_north = open("./data/north_adm2.geojson")
fc_north = create_ee_polygon_from_geojson(f_north)

f_south = open("./data/south_adm2.geojson")
fc_south = create_ee_polygon_from_geojson(f_south)


# %% [markdown]
# ## Assemble cloud mask components
#
# This section builds an S2 SR collection and defines functions to add cloud and cloud shadow component layers to each image in the collection.

# ### Build a Sentinel-2 collection
#
# [Sentinel-2 surface reflectance](https://developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S2_SR) and [Sentinel-2 cloud probability](https://developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S2_CLOUD_PROBABILITY) are two different image collections. Each collection must be filtered similarly (e.g., by date and bounds) and then the two filtered collections must be joined.
#
# Define a function to filter the SR and s2cloudless collections according to area of interest and date parameters, then join them on the `system:index` property. The result is a copy of the SR collection where each image has a new `'s2cloudless'` property whose value is the corresponding s2cloudless image.


# %%
def get_s2_sr_cld_col(aoi, start_date, end_date):
    # Import and filter S2 SR.
    s2_sr_col = (
        ee.ImageCollection("COPERNICUS/S2_SR")
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


# %% [markdown]
# ##### Applying the get_s2_sr_cld_col function to build a collection according to the parameters defined above.

# %% [markdown]
# ### Define cloud mask component functions

# %% [markdown]
# #### Cloud components
#
# Define a function to add the s2cloudless probability layer and derived cloud mask as bands to an S2 SR image input.


# %%
def add_cloud_bands(img):
    # Get s2cloudless image, subset the probability band.
    cld_prb = ee.Image(img.get("s2cloudless")).select("probability")

    # Condition s2cloudless by the probability threshold value.
    is_cloud = cld_prb.gt(CLD_PRB_THRESH).rename("clouds")

    # Add the cloud probability layer and cloud mask as image bands.
    return img.addBands(ee.Image([cld_prb, is_cloud]))


# %% [markdown]
# #### Cloud shadow components
#
# Define a function to add dark pixels, cloud projection, and identified shadows as bands to an S2 SR image input. Note that the image input needs to be the result of the above `add_cloud_bands` function because it relies on knowing which pixels are considered cloudy (`'clouds'` band).


# %%
def add_shadow_bands(img):
    # Identify water pixels from the SCL band.
    not_water = img.select("SCL").neq(6)

    # Identify dark NIR pixels that are not water (potential cloud shadow pixels).
    SR_BAND_SCALE = 1e4
    dark_pixels = (
        img.select("B8")
        .lt(NIR_DRK_THRESH * SR_BAND_SCALE)
        .multiply(not_water)
        .rename("dark_pixels")
    )

    # Determine the direction to project cloud shadow from clouds (assumes UTM projection).
    shadow_azimuth = ee.Number(90).subtract(
        ee.Number(img.get("MEAN_SOLAR_AZIMUTH_ANGLE"))
    )

    # Project shadows from clouds for the distance specified by the CLD_PRJ_DIST input.
    cld_proj = (
        img.select("clouds")
        .directionalDistanceTransform(shadow_azimuth, CLD_PRJ_DIST * 10)
        .reproject(**{"crs": img.select(0).projection(), "scale": 100})
        .select("distance")
        .mask()
        .rename("cloud_transform")
    )

    # Identify the intersection of dark pixels with cloud shadow projection.
    shadows = cld_proj.multiply(dark_pixels).rename("shadows")

    # Add dark pixels, cloud projection, and identified shadows as image bands.
    return img.addBands(ee.Image([dark_pixels, cld_proj, shadows]))


# %% [markdown]
# #### Final cloud-shadow mask
#
# Define a function to assemble all of the cloud and cloud shadow components and produce the final mask.


# %%
def add_cld_shdw_mask(img):
    # Add cloud component bands.
    img_cloud = add_cloud_bands(img)

    # Add cloud shadow component bands.
    img_cloud_shadow = add_shadow_bands(img_cloud)

    # Combine cloud and shadow mask, set cloud and shadow as value 1, else 0.
    is_cld_shdw = (
        img_cloud_shadow.select("clouds").add(img_cloud_shadow.select("shadows")).gt(0)
    )

    # Remove small cloud-shadow patches and dilate remaining pixels by BUFFER input.
    # 20 m scale is for speed, and assumes clouds don't require 10 m precision.
    is_cld_shdw = (
        is_cld_shdw.focalMin(2)
        .focalMax(BUFFER * 2 / 20)
        .reproject(**{"crs": img.select([0]).projection(), "scale": 20})
        .rename("cloudmask")
    )

    # Add the final cloud-shadow mask to the image.
    return img_cloud_shadow.addBands(is_cld_shdw)


# %% [markdown]
# ### Define cloud mask application function
#
# Define a function to apply the cloud mask to each image in the collection.


# %%
def apply_cld_shdw_mask(img):
    # Subset the cloudmask band and invert it so clouds/shadow are 0, else 1.
    not_cld_shdw = img.select("cloudmask").Not()

    # Subset reflectance bands and update their masks, return the result.
    return img.select("B.*").updateMask(not_cld_shdw)


# %% [markdown]
# ### Process the collection
#
# Add cloud and cloud shadow component bands to each image and then apply the mask to each image. Reduce the collection by median.

# %% [markdown]
# ### Define collection filter and cloud mask parameters
#
# Define parameters that are used to filter the S2 image collection and determine cloud and cloud shadow identification.
#
# |Parameter | Type | Description |
# | :-- | :-- | :-- |
# | `AOI` | `ee.Geometry` | Area of interest |
# | `START_DATE` | string | Image collection start date (inclusive) |
# | `END_DATE` | string | Image collection end date (exclusive) |
# | `CLOUD_FILTER` | integer | Maximum image cloud cover percent allowed in image collection |
# | `CLD_PRB_THRESH` | integer | Cloud probability (%); values greater than are considered cloud |
# | `NIR_DRK_THRESH` | float | Near-infrared reflectance; values less than are considered potential cloud shadow |
# | `CLD_PRJ_DIST` | float | Maximum distance (km) to search for cloud shadows from cloud edges |
# | `BUFFER` | integer | Distance (m) to dilate the edge of cloud-identified objects |

# %% [markdown]
# ### Exporting images

# %%
# clip image
# clip = malawi_north.clipToCollection(fc)
# export clipped result in Tiff

# Parameters
AOI = fc_north  # ee.Geometry.MultiPolygon(shapefile["features"][0]["geometry"]["coordinates"])
CLOUD_FILTER = 85
CLD_PRB_THRESH = 30
NIR_DRK_THRESH = 0.15
CLD_PRJ_DIST = 1
BUFFER = 50


def export_quarter(year, quarter):
    if quarter["start"] == "12-01":
        start_year = year
        end_year = year + 1
    else:
        start_year = year
        end_year = year
    start = f'{start_year}-{quarter["start"]}'
    end = f'{end_year}-{quarter["end"]}'
    s2_sr_cld_col = get_s2_sr_cld_col(AOI, start, end)
    s2_sr_median = (
        s2_sr_cld_col.map(add_cld_shdw_mask).map(apply_cld_shdw_mask).median()
    )
    export_image(start, end, s2_sr_median)


def export_image(START_DATE, END_DATE, img_s2_sr_median):
    malawi_north = img_s2_sr_median.select(["B2", "B3", "B4", "B8"])
    clip = malawi_north.clip(AOI.buffer(200))  # .clipToCollection(AOI).divide(10000)
    # region = AOI.geometry()

    scale = 10
    folder = "malawi_imagery"
    img_name = "Cloud_free_malawi_north_img_" + START_DATE + "_" + END_DATE
    export_config = {
        "scale": scale,
        "maxPixels": 5000000000,
        "driveFolder": folder,
        "region": AOI,
    }
    task = ee.batch.Export.image(clip, img_name, export_config)
    task.start()


# %%
export_quarter(2021, {"start": "01-01", "end": "03-28"})


# %%
import datetime

quarters = {
    "Q1": {"start": "06-01", "end": "09-01"},
    "Q2": {"start": "09-01", "end": "12-01"},
    "Q3": {"start": "12-01", "end": "03-01"},
    "Q4": {"start": "03-01", "end": "06-01"},
}


for year in range(2020, 2024):
    for quarter in quarters:
        export_quarter(year, quarters[quarter])
        print(year, quarters[quarter])

# %%
export_quarter(2022, {"start": "12-01", "end": "03-01"})
# export_quarter(2022, {"start": "03-01", "end": "06-01"})
# export_quarter(2023, {"start": "03-01", "end": "06-01"})
# export_quarter(2023, {"start": "06-01", "end": "09-01"})

# %%
