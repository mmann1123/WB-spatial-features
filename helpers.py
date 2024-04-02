import ee

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


def mask_water(image):
    # Calculate NDWI
    ndwi = image.normalizedDifference(
        ["B3", "B8"]
    )  # NDWI = (Green - NIR) / (Green + NIR)
    # Create a water mask (1 for water, 0 for non-water)
    water_mask = ndwi.gt(0)  # Threshold can be adjusted depending on the scene.
    # Update the image's mask to exclude water
    return image.updateMask(water_mask.Not())


def add_cloud_bands(img, CLD_PRB_THRESH):
    # Get s2cloudless image, subset the probability band.
    cld_prb = ee.Image(img.get("s2cloudless")).select("probability")

    # Condition s2cloudless by the probability threshold value.
    is_cloud = cld_prb.gt(CLD_PRB_THRESH).rename("clouds")

    # Add the cloud probability layer and cloud mask as image bands.
    return img.addBands(ee.Image([cld_prb, is_cloud]))


def add_shadow_bands(img, NIR_DRK_THRESH, CLD_PRJ_DIST):
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


def add_cld_shdw_mask(
    img,
    CLD_PRB_THRESH=100,
    NIR_DRK_THRESH=1,
    SCALE=10,
    CLD_PRJ_DIST=2,
    BUFFER=90,
    B3_min_threshold=1000,
):
    # Add cloud component bands.

    # print("Adding cloud bands using paremeters:")
    # print(f"CLD_PRB_THRESH: {CLD_PRB_THRESH}")
    # print(f"NIR_DRK_THRESH: {NIR_DRK_THRESH}")
    # print(f"SCALE: {SCALE}")
    # print(f"CLD_PRJ_DIST: {CLD_PRJ_DIST}")
    # print(f"BUFFER: {BUFFER}")

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
        .reproject(**{"crs": img.select([0]).projection(), "scale": SCALE})
        .rename("cloudmask")
    )

    # mask out all pixels where B3 is greater than 1000 as clouds
    img_cloud_shadow = img_cloud_shadow.updateMask(
        img_cloud_shadow.select("B3").lt(B3_min_threshold)
    )

    # Add the final cloud-shadow mask to the image.
    return img_cloud_shadow.addBands(is_cld_shdw)


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


def get_s2A_SR_sr_cld_col(
    aoi, start_date, end_date, product="S2_SR_HARMONIZED", CLOUD_FILTER=30
):
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


def apply_cld_shdw_mask(img):
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
