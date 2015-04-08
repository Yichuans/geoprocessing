#-------------------------------------------------------------------------------
# Name:        Land cover analysis testing using open source libraries
# Purpose:     
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
# Created:     2015/03/19
#-------------------------------------------------------------------------------
import os, sys
import numpy as np
import logging, time
from Yichuan10 import simple_time_tracker, time_tracker, memory_tracker, GetUniqueValuesFromFeatureLayer_ogr
from osgeo import gdal, ogr, osr

# logging setup
logging.basicConfig(filename='log.txt', level=logging.DEBUG)

# constant
LANDCOVER_INDEX_2000 = r"D:\Yichuan\NGCC\GLC_v1\index\index00.shp"
LANDCOVER_INDEX_2010 = r"D:\Yichuan\NGCC\GLC_v1\index\index10.shp"
LANDCOVER_TILE_PATH_2000 = r"D:\Yichuan\NGCC\GLC_v1\Globecover_2000_pub"
LANDCOVER_TILE_PATH_2010 = r"D:\Yichuan\NGCC\GLC_v1\Globecover_2010_pub"
WH_DATASOURCE = r'D:\Yichuan\WHS_dump_SHP\whs_dump_140724.shp'
WH_ID = 'wdpaid'
OUTPUT_FILE = 'wh_landcover_analysis.txt'

# nodata_value
NO_DATA_VALUE = 0

# @time_tracker
# @memory_tracker
def vectorise_conversion_matrix(a, b):
    # NOT TO BE USED - for checking results only - high memory usage
    def conversion_matrix(a, b):
        """Create element wise concatenation"""
        return str(int(a)) + '-' + str(int(b))

    vfunc = np.vectorize(conversion_matrix)
    result = vfunc(a, b)
    print type(result[0][0])
    return result


# @time_tracker
# @memory_tracker
def vectorise_conversion_matrix_mk2(a, b):
    """Create element wise concatenation with mask"""
    # more efficient than v1
    a_str = a.astype(np.string_)
    b_str = b.astype(np.string_)

    if not np.array_equal(a_str.mask, b_str.mask):
        raise Exception('Error: array mask different')

    else:
        a_str_ = np.char.add(a_str, '-')
        a_str_b_str = np.char.add(a_str_, b_str)
        a_str_b_str_with_mask = np.ma.array(a_str_b_str, mask=a_str.mask)
        return a_str_b_str_with_mask

# @time_tracker
# @memory_tracker
def create_mem_ds_from_ds_by_wdpaid(datasource, wdpaid):
    """polygon data source input (shp), and specify wdpaid (int) -> <vector ds in mem>"""
    # open datasource, 0 = readonly, 1=update
    source_ds = ogr.Open(datasource, 0)

    # for shapefile, the only layer; if filegdb, the first feature class within FGDB
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

    # clean up
    source_ds = None

    # one must return mem_ds, as mem_layer cannot be 
    # referenced with mem_ds destroyed
    return mem_ds

# @time_tracker
# @memory_tracker
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



# @time_tracker
# @memory_tracker
def overlay_feature_array(select_feature_ds, raster_ds, flag_out_array=True):
    """ <geometry object>, <gdal datasource>, <flag_to_output_array: default True> -> numpy array of overlap
    if <flag_to_output_array> is specificed as False -> overlap datasource in MEM
    the first feature in the <select_feature_ds> is used as input to intersect
    """
    from math import floor, ceil

    # prepare raster
    # lc_ds = gdal.Open(landcover_tile_path)

    lc_ds = raster_ds

    if lc_ds is None:
        # no raster input and no output is needed
        if flag_out_array:
            return None, None
        else:
            return None

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

    # # debug
    # print fc_ds
    # print v_layer
    # print v_feature

    v_geom = v_feature.geometry().Clone()

    v_sr = v_layer.GetSpatialRef()
        # projection
    coords_trans = osr.CoordinateTransformation(v_sr, target_sr)
    v_geom.Transform(coords_trans)

        # calculate bounding box of the projected bbox
    xmin, xmax, ymin, ymax = v_geom.GetEnvelope()

    # if boundary don't overlap return None
    if xmin > xOrigin + pixelWidth*lc_rsize[0] or xmax < xOrigin or ymin > yOrigin or ymax < yOrigin + pixelHeight*lc_rsize[1]:
        print "Info: no overlap between bounding boxes"
        if flag_out_array:
            return None, None
        else:
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

    # read raster array (safe to read the entire tile - it is no more than 500MB)
    ras_array = lc_rb.ReadAsArray(xoff, yoff, xsize, ysize)

    # debug print data type
    # print 'raster datatype', ras_array.dtype

    # read vector array
    new_gt = (
        xOrigin + (x1 * pixelWidth),
        pixelWidth,
        0.0,
        yOrigin + (y1 * pixelHeight),
        0.0,
        pixelHeight
    )

    # # debug
    # v_target_ds = gdal.GetDriverByName('GTiff').Create('v_debug_rasterise_test.tif', xsize, ysize, 1, gdal.GDT_Byte)
    # print 'origin:', (new_gt[0], new_gt[3])

    v_target_ds = gdal.GetDriverByName('MEM').Create('v_rasterise', xsize, ysize, 1, gdal.GDT_Byte)

        # set new georeference for the vector rasterization
    v_target_ds.SetGeoTransform(new_gt)
        # use the raster source spatial reference
    v_target_ds.SetProjection(lc_sr_wkt)

    gdal.RasterizeLayer(v_target_ds, [1], v_layer, burn_values=(1,))

    v_array = v_target_ds.ReadAsArray()
    # print 'vector converted datatype', v_array.dtype

    # combine as a masked array
    out_array = np.ma.MaskedArray(
                ras_array,
                mask=np.logical_or(
                    ras_array == NO_DATA_VALUE,
                    np.logical_not(v_array)
                )
            )

    # get the target srid
    target_srid = int(target_sr.GetAttrValue('AUTHORITY', 1))
    array_params = [target_srid, (new_gt[0], new_gt[3]), new_gt[1], new_gt[5]]


    # if output is raster ds of the overlap
    if not flag_out_array:
        # output in memory
        array_params.append(True)

        return array2raster(out_array, 'MEM', *array_params)

    # if output is array
        # free memory
    lc_rb = None
    lc_ds = None
    v_target_ds = None

        # the second output for debugging
        # print 'DEBUG:', 'the size of array in MB', out_array.nbytes/1024/1024
    return out_array, array_params

# @time_tracker
# @memory_tracker
def array2raster(input_array, output_raster_path, target_srid, rasterOrigin, pixelWidth, pixelHeight, flag_in_mem = False):
    """
    Utility for checking output array
    <input_array: make sure it is not a masked array>, <output raster in Gtiff>, <srid>, <upper left in tupple>
    <x cell size>, <y cell size, negative>
    """

    cols = input_array.shape[1]
    rows = input_array.shape[0]
    originX = rasterOrigin[0]
    originY = rasterOrigin[1]

    if flag_in_mem == True:
        driver = gdal.GetDriverByName('MEM')
    else:
        driver = gdal.GetDriverByName('GTiff')

    outRaster = driver.Create(output_raster_path, cols, rows, 1, gdal.GDT_Byte)
    outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))
    outband = outRaster.GetRasterBand(1)
    outband.WriteArray(input_array)
    outRaster_sr = osr.SpatialReference()
    outRaster_sr.ImportFromEPSG(target_srid)
    outRaster.SetProjection(outRaster_sr.ExportToWkt())
    outband.FlushCache()

    # outband = None
    # outRaster = None

    return outRaster

# @time_tracker
# @memory_tracker
def analyse_categorical(input_array_with_mask):
    """return the count of each category in the masked array"""
    from collections import Counter

    # disregard masked values
    result = input_array_with_mask.compressed()
    return Counter(result)

# @time_tracker
# @memory_tracker
def analyse_categorical_conversion(array1, array2):
    """Conversion matrix"""
    from collections import Counter

    out_array = vectorise_conversion_matrix_mk2(array1, array2)
    result = out_array.compressed()
    return Counter(result)

def find_landcover_tile(feature_geom, year):
    # DO NOT USE THIS function
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

def find_landcover_tile_mk2(feature_geom):
    """ <geometry object> -> pathlist [(2000, 2010),...]
    SRs must all be WGS84
    """
    path_list = list()

    # get any tile
    LANDCOVER_INDEX = LANDCOVER_INDEX_2010
    index_ds = ogr.Open(LANDCOVER_INDEX)

    index_layer = index_ds.GetLayer()
    index_layer.SetSpatialFilter(feature_geom)

    # get tile id
    tile_id_list = [feature.GetField('idx') for feature in index_layer]

    for tile_id in tile_id_list:
        path_2000 = LANDCOVER_TILE_PATH_2000 + os.sep + tile_id + '_' + '2000' + 'LC030' + os.sep + tile_id + '_' + '2000'
        path_2000_shp = path_2000 + '.shp'
        path_2000_tif = path_2000 + 'LC030.tif'

        path_2010 = LANDCOVER_TILE_PATH_2010 + os.sep + tile_id + '_' + '2010' + 'LC030' + os.sep + tile_id + '_' + '2010'
        path_2010_shp = path_2010 + '.shp'
        path_2010_tif = path_2010 + 'LC030.tif'

        if os.path.exists(path_2000_tif) and os.path.exists(path_2010_tif):
            path_list.append([path_2000_shp, path_2000_tif, path_2010_shp, path_2010_tif])
        else:
            print 'Warning:', path_2000, 'or', path_2010, 'doesn\'t exist'

    return path_list

def get_all_wh_id():
    inputFc = WH_DATASOURCE
    inputField = WH_ID
    return GetUniqueValuesFromFeatureLayer_ogr(inputFc, inputField)

def get_fc_extent(fc_datasource):
    source_ds = ogr.Open(fc_datasource, 0)
    # for shapefile, the only layer; if filegdb, the first feature class within FGDB
    source_layer = source_ds.GetLayer(0)

    # spatial reference
    source_srs = source_layer.GetSpatialRef()

    # it should contain only one feature
    layer_extent = source_layer.GetExtent()
    xmin = layer_extent[0]
    xmax = layer_extent[1]
    ymin = layer_extent[2]
    ymax = layer_extent[3]

    # create geometry of the extent: anti-clock wise
    ring = ogr.Geometry(ogr.wkbLinearRing)
    poly = ogr.Geometry(ogr.wkbPolygon)

    cell_density = 10000

    # need to densify for correct projection
    for x in np.arange(xmin, xmax, (xmax-xmin)/cell_density):
        ring.AddPoint(x, ymin)

    for y in np.arange(ymin, ymax, (ymax-ymin)/cell_density):
        ring.AddPoint(xmax, y)

    for x in np.arange(xmax, xmin, -(xmax-xmin)/cell_density):
        ring.AddPoint(x, ymax)

    for y in np.arange(ymax, ymin, -(ymax-ymin)/cell_density):
        ring.AddPoint(xmin, y)

    # end point
    ring.AddPoint(xmin,ymin)

    poly.AddGeometry(ring)

    # create a new vector in memory
    mem_driver = ogr.GetDriverByName('Memory')

    # # DEBUG
    # mem_driver = ogr.GetDriverByName('ESRI Shapefile')

    mem_ds = mem_driver.CreateDataSource('temp_mem_extent')
    mem_layer = mem_ds.CreateLayer(name = 'temp_mem_extent_lyr', srs=source_srs, geom_type=ogr.wkbPolygon)
    mem_feature = ogr.Feature(feature_def=mem_layer.GetLayerDefn())

    mem_feature.SetGeometry(poly)
    mem_layer.CreateFeature(mem_feature)

    # one must return mem_ds, as mem_layer cannot be 
    # referenced with mem_ds destroyed
    return mem_ds

def adjust_shape(input_array, newshape):
    """split input array according to newshape -> upper left array after split"""

    output_array = np.vsplit(input_array, (newshape[0],))[0]
    output_array = np.hsplit(output_array, (newshape[1],))[0]

    return output_array


def process_each_wh_site_tile(wdpaid, path_list):
    
    # by tile
    result_list = list()

    path_2000_shp = path_list[0]
    path_2000_tif = path_list[1]
    path_2010_shp = path_list[2]
    path_2010_tif = path_list[3]

    try:
        # get data sources
        tile_2000_ds = gdal.Open(path_2000_tif)
        tile_2010_ds = gdal.Open(path_2010_tif)

        # make sure overlapping tile boundaries are reconciled (input land cover data overlap along tile boundaries)
            # get extent using index shapefile
        tile_extent_2000_ds = get_fc_extent(path_2000_shp)
        tile_extent_2010_ds = get_fc_extent(path_2010_shp)


            # get landcover tile within the bound of the above index shapefile
        clean_tiles_2000_ds = overlay_feature_array(tile_extent_2000_ds, tile_2000_ds, False)
        clean_tiles_2010_ds = overlay_feature_array(tile_extent_2010_ds, tile_2010_ds, False)

        # raster - vector overlap as numpy array
            # two duplicate datasource to avoid next feature method returns None
        wh_ds_mem1 = create_mem_ds_from_ds_by_wdpaid(WH_DATASOURCE, wdpaid)
        wh_ds_mem2 = create_mem_ds_from_ds_by_wdpaid(WH_DATASOURCE, wdpaid)

        out_array_2000, array2raster_param_2000 = overlay_feature_array(wh_ds_mem1, clean_tiles_2000_ds, True)
        out_array_2010, array2raster_param_2010 = overlay_feature_array(wh_ds_mem2, clean_tiles_2010_ds, True)

            # if no overlap is found, skip
        if out_array_2010 is None or out_array_2010 is None:
            return result_list

        # reconcile array shape difference (input land cover data do not snap)
        if out_array_2000.shape != out_array_2010.shape:
            msg = """%s; %s; inconsistent output array shape. Automatically split using the smaller array shape."""%(time.ctime(), wdpaid)
            logging.warning(msg)

            # if shape different, choose the smaller
            shape_rows = min(out_array_2000.shape[0], out_array_2010.shape[0])
            shape_cols = min(out_array_2000.shape[1], out_array_2010.shape[1])
            newshape = (shape_rows, shape_cols)

            # DEBUG
            msg = """%s; %s; original shape %s and %s, new shape %s"""%(time.ctime(), wdpaid, out_array_2000.shape, out_array_2010.shape, newshape)

            logging.warning(msg)

            out_array_2000 = adjust_shape(out_array_2000, newshape = newshape)
            out_array_2010 = adjust_shape(out_array_2010, newshape = newshape)

        # reconcile mask so that stats match: use the maximum extent
        combined_mask = np.logical_and(out_array_2000.mask, out_array_2010.mask)
        out_array_2000 = np.ma.array(out_array_2000.data, mask = combined_mask)
        out_array_2010 = np.ma.array(out_array_2010.data, mask = combined_mask)

    # return out_array_2000, out_array_2010


        # analyse arrays
        stats_2000 = analyse_categorical(out_array_2000)
        stats_2010 = analyse_categorical(out_array_2010)
        stats_conv = analyse_categorical_conversion(out_array_2000, out_array_2010)

        # format
        result_list.extend([[wdpaid, '2000', str(each_class), stats_2000[each_class], path_2000_tif, '-1'] for each_class in stats_2000])
        result_list.extend([[wdpaid, '2010', str(each_class), stats_2010[each_class], '-1', path_2010_tif] for each_class in stats_2010])
        result_list.extend([[wdpaid, '2000-2010', str(each_class), stats_conv[each_class], path_2000_tif, path_2010_tif] for each_class in stats_conv])

    except Exception, e:
        import traceback
        # if things go wrong - log failed ids
        msg = """%s; %s; Failed. %s"""%(time.ctime(), wdpaid, e)
        print 'Failed ID:', wdpaid
        print traceback.print_exc()

        logging.error(msg)
        result_list = [[wdpaid, 'failed', '-1', '-1', path_2000_tif, path_2010_tif]]

    return result_list

def process_each_wh_site_mk2(wdpaid):
    print 'Processing ID:', wdpaid
    # for each WH site by id 
    wh_ds_mem = create_mem_ds_from_ds_by_wdpaid(WH_DATASOURCE, wdpaid)

    # get geom
    lyr = wh_ds_mem.GetLayer(0)
    feature = lyr[0]
    feature_geom = feature.geometry().Clone()

    # get paths of 2000 and 2010 tiles
    path_list = find_landcover_tile_mk2(feature_geom)
    
    # main
    result_list = list()

    # for each wdpaid and path_tile(2000 and 2010)
    for each_path_list in path_list:
        result = process_each_wh_site_tile(wdpaid, each_path_list)
        result_list.extend(result)

    return result_list


def write_each_wh_result(result_list, output=OUTPUT_FILE):
    if not os.path.exists(output):
        f = open(output, 'w')
        f.write('wdapaid,year,class,pixels,path_2000_tif,path_2010_tif\n')

    else:
        f = open(output, 'a')

    for result in result_list:
        f.write(','.join(map(str, result)) + '\n')

    f.close()


# -------------- TEST suites----------------

def _test_process_each_wh_site(wdpaid = 2010):
    return process_each_wh_site_mk2(wdpaid)


def _test_overlay_feature_ds(wdpaid = 2577):
    datasource = WH_DATASOURCE
    select_feature_ds = create_mem_ds_from_ds_by_wdpaid(datasource, wdpaid)

    lyr = select_feature_ds.GetLayer(0)
    feature = lyr[0]
    feature_geom = feature.geometry().Clone()

    path_list = find_landcover_tile_mk2(feature_geom)
    print path_list

    path_2000_shp, path_2000_tif, path_2010_shp, path_2010_tif = path_list[0]

    landcover_tile_path1 = path_2000_tif

    print landcover_tile_path1
    # run result
    fc_extent = get_fc_extent(path_2000_shp)

    ras = gdal.Open(landcover_tile_path1)

    out_ds1, array2raster_param1 = overlay_feature_array(fc_extent, ras)

    # return result
    array2raster(out_ds1.mask, 'debug_mask4.tif', *array2raster_param1)



def _test_performance():
    process_each_wh_site(900881)


def _test_find_tiles():
    datasource = r"D:\Yichuan\TEMP\dump_jor_n.shp"
    wdpaid = 17240
    select_feature_ds = create_mem_ds_from_ds_by_wdpaid(datasource, wdpaid)

    lyr = select_feature_ds.GetLayer(0)
    feature = lyr[0]
    feature_geom = feature.geometry().Clone()

    path_list = find_landcover_tile_mk2(feature_geom)
    return path_list

def _test_overlay_feature_array(wdpaid = 2575):
    datasource = WH_DATASOURCE
    select_feature_ds = create_mem_ds_from_ds_by_wdpaid(datasource, wdpaid)

    lyr = select_feature_ds.GetLayer(0)
    feature = lyr[0]
    feature_geom = feature.geometry().Clone()

    path_list = find_landcover_tile_mk2(feature_geom)
    print path_list

    path_pair = path_list[0]

    landcover_tile_path1 = path_pair[0]
    landcover_tile_path2 = path_pair[1]

    print landcover_tile_path1
    # run result
    out_array1, array2raster_param1 = overlay_feature_array(select_feature_ds, landcover_tile_path1)
    out_array2, array2raster_param2 = overlay_feature_array(select_feature_ds, landcover_tile_path2)

    array2raster(out_array1.mask, 'debug_mask1.tif', *array2raster_param1)
    array2raster(out_array2.mask, 'debug_mask2.tif', *array2raster_param2)
    # return result
    return out_array1, out_array2

def _test_array2raster(input_array, rasterOrigin):

    output_raster_path = 'v_debug_rasterise_test_array.tif'
    target_srid = 32636
    pixelWidth = 30
    pixelHeight = -30
    array2raster(input_array.filled(99), output_raster_path, target_srid, rasterOrigin, pixelWidth, pixelHeight)


def _test_get_params():
    datasource = r"D:\Yichuan\TEMP\dump_jor_n.shp"

def _test_ofc():

    datasource = r"D:\Yichuan\TEMP\dump_jor_n.shp"
    wdpaid = 17240
    select_feature_ds = create_mem_ds_from_ds_by_wdpaid(datasource, wdpaid)

    lyr = select_feature_ds.GetLayer(0)
    feature = lyr[0]
    feature_geom = feature.geometry().Clone()

    path_list = find_landcover_tile(feature_geom, year = 2000)
    print path_list
    landcover_tile_path = path_list[0]

    return overlay_feature_array(select_feature_ds, landcover_tile_path)


def _test_get_geom_from_shape():
    datasource = r"D:\Yichuan\WDPA\WDPA_poly_Jan2014.shp"
    return create_mem_ds_from_ds_by_wdpaid(datasource, 40603)