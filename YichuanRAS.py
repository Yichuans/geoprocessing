#-------------------------------------------------------------------------------
# Name:        Raster generic
# Purpose:      GDAL, arcpy needed
#
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
#
# Created:     2014/03/20
# Copyright:   (c) Yichuan Shi 2014
#-------------------------------------------------------------------------------

import os, sys, codecs
import arcpy, numpy, math, time
import Yichuan10
from osgeo import gdal

# in order to work with spatialanalyst
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput= True


def clip_raster(featurelayer, in_raster, out_raster, no_data=-999):
    """wrapper around arcpy.clip_management, note no_data may scale up pixel depth
    thus requiring more computation time and space; e.g clipping 30m over
    GBR takes a long time, consider to change this to 254, thus fitting int8 """
    starttime = time.time()
    # print 'Clipping ', in_raster, 'by', featurelayer, ', output:', out_raster, ', nodata:', no_data
    try:
        arcpy.Clip_management(in_raster, "#", out_raster, featurelayer, no_data, "ClippingGeometry")
        print 'Clip successful'
    except Exception as e:
        print "Clip failed:"
        print arcpy.GetMessages(), str(e), sys.exc_info()[0]
    finally:
        stoptime = time.time()
        print 'Time taken:', stoptime - starttime, 's'


def gdal_tif_to_numpy(ras):
    """raster source (tif) -> numpy;
    tested against arcpy rastertonumpy"""
    # raster object gdal
    try:
        ras_gdal = gdal.Open(ras)

    except Exception as e:
        print "Open raster error:"
        print arcpy.GetMessages(), str(e), sys.exc_info()[0]

    # get first band (tif)
    ras_band = ras_gdal.GetRasterBand(1)

    # read full ras
    np_ras = ras_band.ReadAsArray()
    # print 'Convert to numpy array (gdal):', ras, np_ras.shape, float(np_ras.nbytes)/1024/1024, 'MB', np_ras.dtype

    # memory management
    ras_gdal = None
    ras_band = None

    # return
    return np_ras

def arcpy_tif_to_numpy(ras, no_data=-999):
    """raster source (tif) -> numpy; memory issue even in 64 bit? crash + error 99999/8"""
    # arc_ras = arcpy.Raster(ras)
    np_ras = arcpy.RasterToNumPyArray(ras, nodata_to_value=no_data)

    print 'Covert to numpy array (arcpy):',  ras, np_ras.shape, float(np_ras.nbytes)/1024/1024, 'MB', np_ras.dtype

    # return
    return np_ras

def raster_area_lat(ras):
    """calculate area per lat cellsize -> numpy
    this function takes an raster object or path and creates an numpy array with areas per cell
    This should be used in conjunction with the numpy array created using ArcGIS's
    RasterToNumpyArray
    Fixed an issue with numpy read order:
    Vstack: a1=[1,2,3], a2[2,3,4], vstack((a1,a2)) = [[1,2,3], [2,3,4]

    Arcpy.Raster-> Numpy and Numpy-> Raster, all table like storage
    test run: 1) size algorithm 2) order """
    # trying to calculate true ellipsoidal area
    # nrow is the row number (lat), derived from raster object

    # eccentricity e
    e = 0.081819190842621

    # semi-major axis = equatorial radius a
    a = 6378.137

    # this function calculates the formula in solving integral
    def f(lat):
        from numpy import sin, power, log
        return sin(lat)/(1-power(e, 2)*power(sin(lat), 2)) + (0.5/e)*log((1+e*sin(lat))/(1-e*sin(lat)))

    # make sure this is the arcpy raster object
    if not isinstance(ras, arcpy.Raster):
        raster_object = arcpy.Raster(ras)

    # height and width measured in number of unit cell
    # not to be confused with raster_object.extent.height, which measured in map units
    cellsize = raster_object.meanCellHeight
    nrow = raster_object.height
    ncol = raster_object.width
    ymin = raster_object.extent.YMin
    ymax = raster_object.extent.YMax # cell upper left corner, not centroid of cell

    # in rad
    cellsize_pi = cellsize * numpy.pi / 180

    ymax_pi = ymax * numpy.pi / 180

    stack_list = list()
    for i in range(nrow):
        # all in degress: order corrected
        y2 = ymax_pi - i*cellsize_pi
        y1 = y2 - cellsize_pi

        # calculate area
        ith_area = 0.5 * a * a * (1 - e * e) * cellsize_pi * (f(y2) - f(y1))

        # all same latitude cells have same areas, 'float32' to reduce size
        ith_array = numpy.array([ith_area], 'float32')

        # append to list for vstack later
        stack_list.append(ith_array)

    # create ndarray using vstack; note storage. e.g. i = 0 refers to the top row in raster and in np array
    result = numpy.vstack(stack_list)

    # free memory not sure needed
    del raster_object

    print 'Lat area grid numpy:', result.shape, float(result.nbytes)/1024/1024, 'MB', result.dtype

    return result

def np_calculate_raster_quantity(ras_np, ras_area_lat_np, no_data=-999):
    """Get quantity, i.e. area * density (raster value)"""
    ras_mask = numpy.ma.masked_not_equal(ras_np, no_data)

    count_pixel = ras_mask.mask.sum()
    total_quantity = (ras_mask.mask * ras_np * ras_area_lat_np).sum()

    return (count_pixel, total_quantity)


def np_calculate_raster_area(ras_np, ras_area_lat_np, ras_option='RASTER', no_data=-999):
    """generic raster function to return areas
    calculate based on the ras_option:
    'ALL' -> ras rectangle extent
    'RASTER' -> ras extent expect no data param
    other numeric values -> equal to this value, such as 8, 9, 10.2
    """
    # raster rectangle
    if ras_option == 'ALL':
        # use all 1 array
        # for some reason this generates the same as 'RASTER'
##        ras_ones = numpy.ones(ras_np.shape, dtype='int8')
##        count_pixel = ras_ones.sum()
##        total_area = (ras_ones * ras_area_lat_np).sum()
        count_pixel = ras_np.size

        # columns
        cols = ras_np.shape[1]

        # size
        total_area = (ras_area_lat_np * cols).sum()

    # raster
    else:
        # choose area of interest by means of mask
        if ras_option == 'RASTER':
            ras_mask =  numpy.ma.masked_not_equal(ras_np, no_data)

        else:
            # use True as 1
            ras_mask = numpy.ma.masked_equal(ras_np, ras_option)

        # free space
        del ras_np

        # use the mask
        count_pixel = ras_mask.mask.sum()

        # area
        total_area = (ras_mask.mask * ras_area_lat_np).sum()

    return (count_pixel, total_area)

def main():
    pass

if __name__ == '__main__':
    main()


def _test_np(wdpaid=2571, arc=False):
    out_ras = r"C:\test" + os.sep + str(wdpaid) + '.tif'
    print out_ras
    if arc:
        return arcpy_tif_to_numpy(out_ras)
    else:
        return gdal_tif_to_numpy(out_ras)

def _test_area(wdpaid = 2571):
    out_ras = r"C:\test" + os.sep + str(wdpaid) + '.tif'
    print out_ras
    return raster_area_lat(out_ras)


def _test_clip(wdpaid = 2571, no_data=253):
    featurelayer = r"C:\Ys\whs_dump_140113.shp"
    in_raster = r"C:\Ys\Hansen2013\loss_year.gdb\hansenlossyear"


    where_clause = '\"wdpaid\" = ' + str(wdpaid)
    with arcpy.da.SearchCursor(featurelayer, ('wdpaid', 'name', 'SHAPE@'), where_clause=where_clause) as cursor:
        # for each site
        for row in cursor:
            # if the raster extent contains the feature geom, clip rasters
            geom = row[2]
            wdpaid = row[0]
            pa_name = row[1]

            out_ras = r"C:\test" + os.sep + str(wdpaid) + '.tif'
            clip_raster(geom, in_raster, out_ras, no_data)

def _test_suit():
    a = _test_np()
    a = a.astype('int16')
    b = _test_area()
    np_ones = numpy.ones(a.shape, dtype='int8')

    return (a, b, np_ones)



