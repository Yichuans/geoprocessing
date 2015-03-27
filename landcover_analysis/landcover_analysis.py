#-------------------------------------------------------------------------------
# Name:        Land cover analysis testing using open source libraries
# Purpose:     
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
# Created:     2015/03/19
#-------------------------------------------------------------------------------
import os
import numpy as np
from Yichuan10 import simple_time_tracker
from rasterstats import zonal_stats
from YichuanM import get_field_value
from osgeo import gdal, ogr, osr

LANDCOVER_INDEX_2000 = r"D:\Yichuan\NGCC\GLC_v1\index\index00.shp"
LANDCOVER_INDEX_2010 = r"D:\Yichuan\NGCC\GLC_v1\index\index10.shp"
LANDCOVER_TILE_PATH_2000 = r"D:\Yichuan\NGCC\GLC_v1\Globecover_2000_pub"
LANDCOVER_TILE_PATH_2010 = r"D:\Yichuan\NGCC\GLC_v1\Globecover_2010_pub"
# nodata_value
NO_DATA_VALUE = 0

def vector_conversion_matrix(a, b):
    """Create element wise concatenation"""
    return str(int(a)) + '-' + str(int(b))

def create_mem_ds_from_ds_by_wdpaid(datasource, wdpaid):
    """polygon data source input (shp), and specify wdpaid (int) -> <vector ds in mem>"""
    # open datasource, 0 = readonly, 1=update
    source_ds = ogr.Open(datasource, 0)

    # for shapefile, the only layer
    source_layer = source_ds.GetLayer(0)

    # spatial reference
    source_srs = source_layer.GetSpatialRef()

    # select by attribute
    where_clause = 'wdpaid = ' + str(wdpaid)
    source_layer.SetAttributeFilter(where_clause)

    # it should contain only one feature
    a_feature = source_layer.GetNextFeature()
    if a_feature is None:
        print 'Error: no feature has wdpaid =', wdpaid
        return None

    # create a new vector in memory
    mem_driver = ogr.GetDriverByName('Memory')
    mem_ds = mem_driver.CreateDataSource('temp_mem_ds')
    mem_layer = mem_ds.CreateLayer(name = 'temp_mem_lyr', srs=source_srs, geom_type=ogr.wkbPolygon)
    mem_feature = ogr.Feature(feature_def=mem_layer.GetLayerDefn())

    # assign a cloned geom to the newly create feature
    geom = a_feature.GetGeometryRef()
    mem_feature.SetGeometry(geom.Clone())
    mem_layer.CreateFeature(mem_feature)

    # one must return mem_ds, as mem_layer cannot be 
    # referenced with mem_ds destroyed
    return mem_ds

def rasterize_feature_by_id(source_path, wdpaid, pixel_size=0.00025, output_disk=False):
    """Select a feature by ID and rasterise it using the same SR -> <raster ds in mem>
    """
    import math

    select_source_ds = create_mem_ds_from_ds_by_wdpaid(source_path, wdpaid)
    select_source_layer = select_source_ds.GetLayer(0)
    select_source_srs = select_source_layer.GetSpatialRef()

    # get extent
    x_min, x_max, y_min, y_max = select_source_layer.GetExtent()

    # Create the destination data source
    x_res = int(math.ceil((x_max - x_min) / pixel_size))
    y_res = int(math.ceil((y_max - y_min) / pixel_size))

    # Create new data source
    # for debuging purposes only
    if output_disk:
        DRIVER_NAME = "GTiff"
        output_name = 'debug_raster.tif'
    else:
        DRIVER_NAME = "MEM"
        output_name = 'mem_raster'

    target_ds = gdal.GetDriverByName(DRIVER_NAME).Create(output_name, x_res, y_res, 1, gdal.GDT_Byte)

    target_ds.SetGeoTransform((
            x_min, pixel_size, 0,
            y_max, 0, -pixel_size,
        ))

    # set spatial reference
    if select_source_srs:
        # Ensure the target raster has the same projection as the source
        target_ds.SetProjection(select_source_srs.ExportToWkt())
    else:
        # Source has no projection (needs GDAL >= 1.7.0 to work)
        target_ds.SetProjection('LOCAL_CS["arbitrary"]')

    # Rasterize with 1
    err = gdal.RasterizeLayer(target_ds, [1], select_source_layer,
            burn_values=(1,))

    if err != 0:
        raise Exception("error rasterizing layer: %s" % err)

    return target_ds

# def _test_rasterize_feature_by_id(source_path=r"D:\Yichuan\TEMP\dump_jor_n.shp", wdpaid=478381, pixel_size=0.00025, output_disk=True):
#     return rasterize_feature_by_id(source_path, wdpaid, pixel_size, output_disk)

def overlay_feature_array(select_feature_ds, landcover_tile_path):
    """ <geometry object>, <tif path> -> numpy array of overlap
    SRs of feature geom and landcover tile must be the same
    """
    from math import floor, ceil


    # prepare raster
    lc_ds = gdal.Open(landcover_tile_path)
    lc_rb = lc_ds.GetRasterBand(1)

        # raster size in number of pixels
    lc_rsize = (lc_ds.RasterXSize, lc_ds.RasterYSize)

        # georeference, units in actual coordinates
    lc_gt = lc_ds.GetGeoTransform()
    xOrigin = lc_gt[0] # orgin starts from upper left
    yOrigin = lc_gt[3]
    pixelWidth = lc_gt[1]
    pixelHeight = lc_gt[5] # minus
    
        # raster source is the target coordinate system
    lc_sr_wkt = lc_ds.GetProjection()
    target_sr = osr.SpatialReference()
    target_sr.ImportFromWkt(lc_sr_wkt)

    # prepare vector
    fc_ds = select_feature_ds
    v_layer = fc_ds.GetLayer()

    v_feature = v_layer.GetNextFeature()
    v_geom = v_feature.geometry().Clone()

    v_sr = v_layer.GetSpatialRef()
        # projection
    coords_trans = osr.CoordinateTransformation(v_sr, target_sr)
    v_geom.Transform(coords_trans)

        # calculate bounding box of the projected bbox
    xmin, xmax, ymin, ymax = v_geom.GetEnvelope()

    # if boundary don't overlap return None
    if xmin > xOrigin + pixelWidth*lc_rsize[0] or xmax < xOrigin or ymin > yOrigin or ymax < yOrigin + pixelHeight*lc_rsize[1]:
        return None

    # calculate offsets to read array
    x1 = int(floor((xmin - xOrigin)/pixelWidth))
    if x1 < 0:
        x1 = 0

    y1 = int(floor((ymax - yOrigin)/pixelHeight))
    if y1 < 0:
        y1 = 0

    x2 = int(ceil((xmax - xOrigin)/pixelWidth))
    if x2 > lc_rsize[0]:
        x2 = lc_rsize[0]

    y2 = int(ceil((ymin - yOrigin)/pixelHeight))
    if y2 > lc_rsize[1]:
        y2 = lc_rsize[1]

    xoff = x1
    yoff = y1
    xsize = x2 - x1
    ysize = y2 - y1

    # read raster array
    ras_array = lc_rb.ReadAsArray(xoff, yoff, xsize, ysize)

    # read vector array
    new_gt = (
        xOrigin + (x1 * pixelWidth),
        pixelWidth,
        0.0,
        yOrigin + (y1 * pixelHeight),
        0.0,
        pixelHeight
    )

    # debug
    v_target_ds = gdal.GetDriverByName('GTiff').Create('v_rasterise1.tif', xsize, ysize, 1, gdal.GDT_Byte)
    # v_target_ds = gdal.GetDriverByName('MEM').Create('v_rasterise', xsize, ysize, 1, gdal.GDT_Byte)
        # set new georeference for the vector rasterization
    v_target_ds.SetGeoTransform(new_gt)
        # use the raster source spatial reference
    v_target_ds.SetProjection(lc_sr_wkt)

    gdal.RasterizeLayer(v_target_ds, [1], v_layer, burn_values=(1,))

    v_array = v_target_ds.ReadAsArray()

    # combine as a masked array
    out_array = np.ma.MaskedArray(
                ras_array,
                mask=np.logical_or(
                    ras_array == NO_DATA_VALUE,
                    np.logical_not(v_array)
                )
            )

    return out_array

def _test_overlay_feature_array():

    datasource = r"D:\Yichuan\TEMP\dump_jor_n.shp"
    wdpaid = 17240
    select_feature_ds = create_mem_ds_from_ds_by_wdpaid(datasource, wdpaid)

    lyr = select_feature_ds.GetLayer(0)
    feature = lyr[0]
    feature_geom = feature.geometry().Clone()

    path_list = find_landcover_tile(feature_geom, year = 2000)
    print path_list
    landcover_tile_path = path_list[0]

    return overlay_feature_array(select_feature_ds, landcover_tile_path), 


def find_landcover_tile(feature_geom, year):
    """ <geometry object>, <landcover shapefile index> -> path
    SRs must all be WGS84

    """
    if year == 2000:
        LANDCOVER_INDEX = LANDCOVER_INDEX_2000
        LANDCOVER_TILE_PATH = LANDCOVER_TILE_PATH_2000

    elif year == 2010:
        LANDCOVER_INDEX = LANDCOVER_INDEX_2010
        LANDCOVER_TILE_PATH = LANDCOVER_TILE_PATH_2010
    else:
        raise Exception('year must be 2000 or 2010')

    index_ds = ogr.Open(LANDCOVER_INDEX)
    index_layer = index_ds.GetLayer()
    index_layer.SetSpatialFilter(feature_geom)

    tile_id_list = [feature.GetField('idx') for feature in index_layer]

    path_list = [LANDCOVER_TILE_PATH + os.sep + tile_id + '_' + str(year) + 'LC030' + os.sep + tile_id + '_' + str(year) + 'LC030.tif' for tile_id in tile_id_list]

    return path_list

# def _test_find_landcover_tile(feature_geom, landcover_index = LANDCOVER_INDEX_2000, year = 2000):
#     return find_landcover_tile(feature_geom, landcover_index, year)





def test_rasterize2(pixel_size=0.00803):
    import random, math

    source_path = r"D:\Yichuan\TEMP\dump_jor_n.shp"
    # Open the data source
    orig_data_source = ogr.Open(source_path)
    # Make a copy of the layer's data source because we'll need to 
    # modify its attributes table
    source_ds = ogr.GetDriverByName("Memory").CopyDataSource(orig_data_source, "")

    source_layer = source_ds.GetLayer(0)
    source_srs = source_layer.GetSpatialRef()

    # select a feature and rasterise
    select_source_ds = create_mem_ds_from_ds_by_wdpaid(source_path, 3215)
    select_source_layer = select_source_ds.GetLayer(0)

    # get geometry of the first 
    # a_geom = select_source_layer.GetNextFeature().geometry()

    # x_min, x_max, y_min, y_max = a_geom.GetEnvelope()

    # no longer need geometry
    x_min, x_max, y_min, y_max = select_source_layer.GetExtent()

    # print x_min, x_max, y_min, y_max

    # Create the destination data source
    x_res = int(math.ceil((x_max - x_min) / pixel_size))
    y_res = int(math.ceil((y_max - y_min) / pixel_size))

    # print x_res, y_res
    target_ds = gdal.GetDriverByName('GTiff').Create('output_dst5.tif', x_res, y_res, 1, gdal.GDT_Byte)

    target_ds.SetGeoTransform((
            x_min, pixel_size, 0,
            y_max, 0, -pixel_size,
        ))

    if source_srs:
        # Make the target raster have the same projection as the source
        target_ds.SetProjection(source_srs.ExportToWkt())
    else:
        # Source has no projection (needs GDAL >= 1.7.0 to work)
        target_ds.SetProjection('LOCAL_CS["arbitrary"]')

    # Rasterize
    err = gdal.RasterizeLayer(target_ds, [1], select_source_layer,
            burn_values=(1,))

    if err != 0:
        raise Exception("error rasterizing layer: %s" % err)



def _test_get_features(vectors, layer_num=0):
    from osgeo import osr
    class OGRError(Exception):
        pass

    def get_ogr_ds(vds):
        from osgeo import ogr
        if not isinstance(vds, str):
            raise OGRError("OGR cannot open %r: not a string" % vds)

        ds = ogr.Open(vds)
        if not ds:
            raise OGRError("OGR cannot open %r" % vds)

        return ds

    def ogr_srs(vector, layer_num):
        ds = get_ogr_ds(vector)
        layer = ds.GetLayer(layer_num)
        return layer.GetSpatialRef()


    def ogr_records(vector, layer_num=0):
        ds = get_ogr_ds(vector)
        layer = ds.GetLayer(layer_num)
        if layer.GetFeatureCount() == 0:
            raise OGRError("No Features")
        feature = layer.GetNextFeature()
        while feature is not None:
            yield feature_to_geojson(feature)
            feature = layer.GetNextFeature()


    spatial_ref = osr.SpatialReference()
    if isinstance(vectors, str):
        try:
            # either an OGR layer ...
            get_ogr_ds(vectors)
            features_iter = ogr_records(vectors, layer_num)
            spatial_ref = ogr_srs(vectors, layer_num)
            strategy = "ogr"
        except (OGRError, AttributeError):
            # ... or a single string to be parsed as wkt/wkb/json
            feat = parse_geo(vectors)
            features_iter = [feat]
            strategy = "single_geo"
    elif isinstance(vectors, bytes):
        # wkb
        feat = parse_geo(vectors)
        features_iter = [feat]
        strategy = "single_geo"
    elif hasattr(vectors, '__geo_interface__'):
        geotype = vectors.__geo_interface__['type']
        if geotype.lower() == 'featurecollection':
            # ... a featurecollection
            features_iter = geo_records(vectors.__geo_interface__['features'])
            strategy = "geo_featurecollection"
        else:
            # ... or an single object
            feat = parse_geo(vectors)
            features_iter = [feat]
            strategy = "single_geo"
    elif isinstance(vectors, dict):
        # ... or an python mapping
        feat = parse_geo(vectors)
        features_iter = [feat]
        strategy = "single_geo"
    else:
        # ... or an iterable of objects
        features_iter = geo_records(vectors)
        strategy = "iter_geo"

    return features_iter, strategy, spatial_ref

def _test_rasterise_vector():

    vds = ogr.Open(r"D:\Yichuan\TEMP\jor2.shp")
    vlayer = vds.GetLayer(0)

    output_dst = r"D:\yichuan\temp\gdal_test_out3.tif"

    target_ds = gdal.GetDriverByName('GTiff').Create(output_dst, 1000, 1000)

    return vds, vlayer, target_ds
    # target_ds.SetGeoTransform((x_start, pixelWidth, 0, y_start, 0, pixelHeight))

    # gdal.RasterizeLayer(target_ds, [1], vlayer, None, None,
    #                     burn_values=[1])


def _test_rasterize_original(pixel_size=0.00803):
    import random
    RASTERIZE_COLOR_FIELD = "__color__"

    # Open the data source
    orig_data_source = ogr.Open(r"D:\Yichuan\TEMP\rus_magadansky.shp")
    # Make a copy of the layer's data source because we'll need to 
    # modify its attributes table
    source_ds = ogr.GetDriverByName("Memory").CopyDataSource(
            orig_data_source, "")

    source_layer = source_ds.GetLayer(0)
    source_srs = source_layer.GetSpatialRef()
    x_min, x_max, y_min, y_max = source_layer.GetExtent()
    print x_min, x_max, y_min, y_max
    # Create a field in the source layer to hold the features colors
    field_def = ogr.FieldDefn(RASTERIZE_COLOR_FIELD, ogr.OFTReal)
    source_layer.CreateField(field_def)
    source_layer_def = source_layer.GetLayerDefn()
    field_index = source_layer_def.GetFieldIndex(RASTERIZE_COLOR_FIELD)
    # Generate random values for the color field (it's here that the value
    # of the attribute should be used, but you get the idea)
    for feature in source_layer:
        feature.SetField(field_index, random.randint(0, 255))
        source_layer.SetFeature(feature)
    # Create the destination data source
    x_res = int((x_max - x_min) / pixel_size)
    y_res = int((y_max - y_min) / pixel_size)
    print x_res, y_res
    target_ds = gdal.GetDriverByName('GTiff').Create('output_dst4.tif', 1257, 600, 3, gdal.GDT_Byte)

    target_ds.SetGeoTransform((
            151.125, pixel_size, 0,
            y_max, 0, -pixel_size,
        ))
    if source_srs:
        # Make the target raster have the same projection as the source
        target_ds.SetProjection(source_srs.ExportToWkt())
    else:
        # Source has no projection (needs GDAL >= 1.7.0 to work)
        target_ds.SetProjection('LOCAL_CS["arbitrary"]')
    # Rasterize
    err = gdal.RasterizeLayer(target_ds, (3, 2, 1), source_layer,
            burn_values=(0, 0, 0),
            options=["ATTRIBUTE=%s" % RASTERIZE_COLOR_FIELD])
    if err != 0:
        raise Exception("error rasterizing layer: %s" % err)


def _test_rasterize(pixel_size=0.00803):
    import random
    RASTERIZE_COLOR_FIELD = "__color__"

    # Open the data source
    orig_data_source = ogr.Open(r"D:\Yichuan\TEMP\rus_magadansky.shp")
    # Make a copy of the layer's data source because we'll need to 
    # modify its attributes table
    source_ds = ogr.GetDriverByName("Memory").CopyDataSource(
            orig_data_source, "")

    source_layer = source_ds.GetLayer(0)
    source_srs = source_layer.GetSpatialRef()
    x_min, x_max, y_min, y_max = source_layer.GetExtent()
    print x_min, x_max, y_min, y_max

    # Create the destination data source
    x_res = int((x_max - x_min) / pixel_size)
    y_res = int((y_max - y_min) / pixel_size)
    print x_res, y_res
    target_ds = gdal.GetDriverByName('GTiff').Create('output_dst4.tif', x_res, y_res, 1, gdal.GDT_Byte)

    target_ds.SetGeoTransform((
            x_min, pixel_size, 0,
            y_max, 0, -pixel_size,
        ))
    if source_srs:
        # Make the target raster have the same projection as the source
        target_ds.SetProjection(source_srs.ExportToWkt())
    else:
        # Source has no projection (needs GDAL >= 1.7.0 to work)
        target_ds.SetProjection('LOCAL_CS["arbitrary"]')
    # Rasterize
    err = gdal.RasterizeLayer(target_ds, [1], source_layer,
            burn_values=(1,))

    if err != 0:
        raise Exception("error rasterizing layer: %s" % err)

# _test_rasterize()

# _test_create_layer_from_geom()
@simple_time_tracker
def _test():
    ras = r"D:\Yichuan\BrianO\WA\clip_loss\0.tif"
    ras
    vec = r"D:\Yichuan\BrianO\WA\wa_teow.shp"
    myshp = get_field_value(vec, 0, 'shape@', 'patch_id')
    result = zonal_stats(myshp, ras, categorical=True)
    return result


def _test_read_write():
    data_source = r"D:\Yichuan\BrianO\WA\clip_base\13.tif"
    output_dst = r"D:\yichuan\temp\gdal_test_out2.tif"

    # read raster
    # get georeference info
    ds = gdal.Open(data_source)

    transform = ds.GetGeoTransform()
    xOrigin = transform[0]
    yOrigin = transform[3]
    pixelWidth = transform[1]
    pixelHeight = transform[5]

    # let's read some data as array
    rb = ds.GetRasterBand(1)

    # offset of 10 units
    x_start = xOrigin + 10 * pixelWidth
    y_start = yOrigin + 10 * pixelHeight

    print x_start, y_start

    # read 100 units
    read_units_x = 100
    read_units_y = 100

    myarray = rb.ReadAsArray(10, 10, read_units_x, read_units_y)

    # output raster 
    target_ds = gdal.GetDriverByName('GTiff').Create(output_dst, 100, 100)

    # adjust georef and proj
    target_ds.SetGeoTransform((x_start, pixelWidth, 0, y_start, 0, pixelHeight))
    target_ds.SetProjection(ds.GetProjection())

    # write raster
    target_ds.GetRasterBand(1).WriteArray(myarray)
    target_ds.FlushCache()

    # clean up
    ds = None
    target_ds = None
