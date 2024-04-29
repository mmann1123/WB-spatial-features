try:
    import ee
except ImportError:
    print(
        "earthengine-api is not installed. Please install it to use the GEE functions."
    )
    ee = None


# CLOUD_FILTER = 30 set by user
# CLD_PRB_THRESH = 80
# NIR_DRK_THRESH = 1
# CLD_PRJ_DIST = 2
# BUFFER = 90
# SCALE = 10

# # ryans setting
# CLOUD_FILTER = 60  # was 60
# CLD_PRB_THRESH = 60
# NIR_DRK_THRESH = 1
# CLD_PRJ_DIST = 6
# BUFFER = 100

import json
from geowombat.backends.rasterio_ import get_file_bounds
from shapely.geometry import Polygon
import geopandas as gpd
from math import ceil
import pendulum


def get_quarter_dates(quarter_str):
    """Get the start and end dates for a quarter
    Args:
        quarter_str (str): Quarter string in the format "YYYY_QN"
    Returns:
        tuple: (start_date, end_date) in the format "YYYY-MM-DD"

    # Example usage
    start, end = get_quarter_dates("2021_Q03")
    print(f"Start: {start}, End: {end}")
    """

    # Parse the year and quarter number
    year, q = map(int, quarter_str.split("_Q"))

    # Calculate the first month of the quarter
    start_month = 3 * (q - 1) + 1

    # Create a date for the first day of the quarter
    start_date = pendulum.date(year, start_month, 1)

    # Get the last day of the quarter by adding 3 months and subtracting 1 day
    end_date = start_date.add(months=3).subtract(days=1)
    print(type(start_date))
    # Return formatted dates
    return start_date.format("YYYY_MM_DD"), end_date.format("YYYY_MM_DD")


# def mask_water(image):
#     """Mask water using MNDWI index. MNDWI = (Green - SWIR) / (Green + SWIR)
#     Args:
#         image: ee.Image
#     Returns:
#         image: ee.Image
#     """

#     ndwi = image.normalizedDifference(["B3", "B11"])
#     # Create a water mask (1 for water, 0 for non-water)
#     water_mask = ndwi.gt(0)

#     return image.updateMask(water_mask.Not())


# def mask_water_from_SCL(image):
#     """Scene Classification Layer (SCL)  In the Scene Classification Layer, water bodies are typically classified under the class value of "6". This classification allows users to identify water pixels within the imagery. The SCL provides a convenient way to mask out water bodies when analyzing Sentinel-2 imagery for applications that require distinguishing between water and non-water pixels.

#     Args:
#         image: ee.Image
#     Returns:
#         image: ee.Image
#     """
#     # Create a water mask (1 for water, 0 for non-water)
#     not_water = image.select("SCL").neq(6)

#     return image.updateMask(not_water.Not())


def bounds_tiler(image_list, max_area=2.5e10):
    """Breaks the image into smaller blocks if the area is too large
    geowobat has trouble with large image blocks

    Args:
        image_list (list): list of images
        max_area (float): maximum area in meters for a block

    Returns:
        list: list of bounds

    Example:

    bounds_list = bounds_tiler([images, images, images])
            for bounds in bounds_list:

                with gw.config.update(bigtiff="YES", ref_bounds=bounds):
                    with gw.open(bgrn, stack_dim="band") as src:
                        src.gw.save(
                            filename=f"../stacks/S2_SR_{quarter}_{grid}_{round(bounds[1],2)},{round(bounds[3],2)}",
                            nodata=nan,
                            overwrite=True,
                            num_workers=12,
                            compress="lzw",
                        )

    """
    bounds = get_file_bounds(
        image_list,
        return_bounds=True,
    )
    # convert bounds to meters in global equal area projection
    minx, miny, maxx, maxy = bounds
    bounds_gdf = gpd.GeoDataFrame(
        geometry=[Polygon([(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)])],
        crs="EPSG:4326",
    )
    area = bounds_gdf.to_crs("EPSG:6933").area.values
    # if small return one block
    if area < max_area:
        return [bounds]
    elif area > max_area:
        print("Large block found, breaking into 2.5e10m^2 blocks")
        num_blocks = int(area / max_area) + 1
        print("num_blocks:", num_blocks)

        # get the bounds of the block
        y_step = (maxy - miny) / num_blocks
        blocks = [
            [minx, miny + i * y_step, maxx, miny + (i + 1) * y_step]
            for i in range(num_blocks)
        ]
        return blocks


def list_files_pattern(images, pattern):
    """List files that match a pattern
    Args:
        images: list of image paths
        pattern: regex pattern
    Returns:
        list: list of files that match the pattern
    """
    import re

    return sorted(
        list(
            set(
                [
                    re.search(pattern, file_path).group()
                    for file_path in images
                    if re.search(pattern, file_path)
                ]
            )
        )
    )


def convert_to_float(image):
    """Convert all bands to float.
    Args:
        image: ee.Image
    Returns:
        image: ee.Image
    """
    return image.toFloat()


def add_cloud_bands(img, CLD_PRB_THRESH):
    """Add cloud probability and cloud mask bands to image.
    Args:
        img: ee.Image
        CLD_PRB_THRESH: int, cloud probability threshold
    Returns:
        ee.Image
    """
    # Get s2cloudless image, subset the probability band.
    cld_prb = ee.Image(img.get("s2cloudless")).select("probability")

    # Condition s2cloudless by the probability threshold value.
    is_cloud = cld_prb.gt(CLD_PRB_THRESH).rename("clouds")

    # Add the cloud probability layer and cloud mask as image bands.
    return img.addBands(ee.Image([cld_prb, is_cloud]))


def add_shadow_bands(img, NIR_DRK_THRESH, CLD_PRJ_DIST):
    """Add cloud shadow band to image.
    Args:
        img: ee.Image
        NIR_DRK_THRESH: float, NIR dark pixel threshold
        CLD_PRJ_DIST: int, cloud projection distance
    Returns:
        ee.Image
    """

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
        .reproject(
            **{"crs": img.select(0).projection(), "scale": 100}
        )  # scale can also be 20
        .select("distance")
        .mask()
        .rename("cloud_transform")
    )

    # Identify the intersection of dark pixels with cloud shadow projection.
    shadows = cld_proj.multiply(dark_pixels).rename("shadows")

    # Add dark pixels, cloud projection, and identified shadows as image bands.
    return img.addBands(ee.Image([dark_pixels, cld_proj, shadows]))


def add_cld_shdw_mask(
    img,
    CLD_PRB_THRESH=100,
    NIR_DRK_THRESH=1,
    SCALE=10,
    CLD_PRJ_DIST=2,
    BUFFER=90,
    B3_min_threshold=1000,
):
    """Add cloud and cloud shadow bands to image.
    Args:
        img: ee.Image
        CLD_PRB_THRESH: int, cloud probability threshold
        NIR_DRK_THRESH: float, NIR dark pixel threshold
        SCALE: int, image scale in meters
        CLD_PRJ_DIST: int, cloud projection distance
        BUFFER: int, buffer distance around cloud objects
    Returns:
        ee.Image
    """

    # Add cloud component bands.

    img_cloud = add_cloud_bands(img, CLD_PRB_THRESH)

    # Add cloud shadow component bands.
    img_cloud_shadow = add_shadow_bands(img_cloud, NIR_DRK_THRESH, CLD_PRJ_DIST)

    # Combine cloud and shadow mask, set cloud and shadow as value 1, else 0.
    is_cld_shdw = (
        img_cloud_shadow.select("clouds").add(img_cloud_shadow.select("shadows")).gt(0)
    )

    # Remove small cloud-shadow patches and dilate remaining pixels by BUFFER input.
    # 20 m scale is for speed, and assumes clouds don't require 10 m precision.
    is_cld_shdw = (
        is_cld_shdw.focalMin(2)
        .focalMax(BUFFER * 2 / SCALE)
        .reproject(**{"crs": img.select([0]).projection(), "scale": 20})
        .rename("cloudmask")
    )

    # Add the final cloud-shadow mask to the image.
    return img_cloud_shadow.addBands(is_cld_shdw)


def get_s2A_SR_sr_cld_collection(
    aoi, start_date, end_date, product="S2_SR", CLOUD_FILTER=50
):
    """Get Sentinel-2A surface reflectance, cloud probability, and cloud mask collection.
    Args:
        aoi: ee.Geometry, area of interest
        start_date: str, start date in 'YYYY-MM-DD' format
        end_date: str, end date in 'YYYY-MM-DD' format
        product: str, Sentinel-2 product ID
        CLOUD_FILTER: int, maximum cloud cover percentage
    Returns:
        ee.ImageCollection
    """

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


def get_s2A_SR_sr_cld_col(
    aoi, start_date, end_date, product="S2_SR_HARMONIZED", CLOUD_FILTER=30
):
    """Get Sentinel-2A surface reflectance, cloud probability, and cloud mask collection.
    Args:
        aoi: ee.Geometry, area of interest
        start_date: str, start date in 'YYYY-MM-DD' format
        end_date: str, end date in 'YYYY-MM-DD' format
        product: str, Sentinel-2 product ID
        CLOUD_FILTER: int, maximum cloud cover percentage
    Returns:
        ee.ImageCollection
    """

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


# Combine cloud and shadow mask, and add water mask
def apply_masks(img, CLD_PRB_THRESH=30, NIR_DRK_THRESH=0.15, CLD_PRJ_DIST=1, BUFFER=50):
    """Apply cloud and cloud shadow masks to image.
    Args:
        img: ee.Image
        CLD_PRB_THRESH: int, cloud probability threshold
        NIR_DRK_THRESH: float, NIR dark pixel threshold
        CLD_PRJ_DIST: int, cloud projection distance
        BUFFER: int, buffer distance around cloud objects
    Returns:
        ee.Image
    """

    img_cloud = add_cloud_bands(img, CLD_PRB_THRESH)
    img_cloud_shadow = add_shadow_bands(img_cloud, NIR_DRK_THRESH, CLD_PRJ_DIST)
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

    return img_masked


def apply_cld_shdw_mask(img):
    """Apply cloud and cloud shadow masks to image.
    Args:
        img: ee.Image
    Returns:
        ee.Image
    """

    # Subset the cloudmask band and invert it so clouds/shadow are 0, else 1.
    not_cld_shdw = img.select("cloudmask").Not()

    # Subset reflectance bands and update their masks, return the result.
    return img.select("B.*").updateMask(not_cld_shdw)


def create_ee_polygon_from_geojson(geojson_path):
    """
    Reads a .geojson file and returns an ee.Geometry.Polygon object.

    Parameters:
    - geojson_path: Path to the .geojson file

    Returns:
    - An ee.Geometry.Polygon object based on the .geojson coordinates

    Raises:
    - FileNotFoundError: If the .geojson file does not exist at the specified path
    - ValueError: If the .geojson structure is not supported
    """
    try:
        # Load the GeoJSON file
        geojson = json.load(geojson_path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"No .geojson file found at the specified path: {geojson_path}"
        )

    # Check and extract coordinates based on the GeoJSON structure
    if geojson["type"] == "Feature":
        geom_type = geojson["geometry"]["type"]
        coordinates = geojson["geometry"]["coordinates"]
        if geom_type == "Polygon":
            return ee.Geometry.Polygon(coordinates)
        elif geom_type == "MultiPolygon":
            return ee.Geometry.MultiPolygon(coordinates)
        else:
            raise ValueError(
                "The GeoJSON Feature must have Polygon or MultiPolygon geometry"
            )
    elif geojson["type"] == "FeatureCollection":
        polygons = []
        multi_polygons = []
        for feature in geojson["features"]:
            geom_type = feature["geometry"]["type"]
            coordinates = feature["geometry"]["coordinates"]
            if geom_type == "Polygon":
                polygons.append(coordinates)
            elif geom_type == "MultiPolygon":
                multi_polygons.extend(coordinates)  # Flatten MultiPolygon coordinates
            else:
                raise ValueError(
                    "FeatureCollection contains unsupported geometry types"
                )

        # Use MultiPolygon if any MultiPolygons exist or if there are multiple polygons
        if multi_polygons or len(polygons) > 1:
            return ee.Geometry.MultiPolygon(polygons + multi_polygons)
        elif polygons:
            return ee.Geometry.Polygon(
                polygons[0]
            )  # Use the first Polygon if that's all we have

    else:
        raise ValueError(
            "The GeoJSON must be a Feature or FeatureCollection with Polygon or MultiPolygon geometries"
        )
