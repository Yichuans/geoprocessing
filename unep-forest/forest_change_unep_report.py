#-------------------------------------------------------------------------------
# Name:        UNEP regional report - forest change by Hansen 2013 data
# Purpose:	   Adapt WH forest script to estimate forest change in UNEP regions
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
# Created:     2015/01/23
#-------------------------------------------------------------------------------
import Yichuan10
import os, sys, codecs
import arcpy, numpy, math

from YichuanRAS import *

def raster_area(raster_object):
    """ this function takes an raster object or path and creates an numpy array with areas per cell
    This should be used in conjunction with the numpy array created using ArcGIS's
    RasterToNumpyArray

    Corrected"""
    # nrow is the row number (lat), derived from raster object
    # for each row calculate area A=r^2 * cellsize * (sin(ymin + (i+1)*cellsize) - sin(ymin + icellsize))

    # authalic radius: assuming same surface area (spheroid) as if the earth was a perfect sphere
    # http://en.wikipedia.org/wiki/Earth_radius#Authalic_radius

    # make sure this is the arcpy raster object
    if not isinstance(raster_object, arcpy.Raster):
        raster_object = arcpy.Raster(raster_object)

    r = 6371.0072
    cellsize = raster_object.meanCellHeight
    nrow = raster_object.height
    ncol = raster_object.width
    ymax = raster_object.extent.YMax

    # in rad
    cellsize_pi = cellsize * numpy.pi / 180
    ymax_pi = ymax * numpy.pi / 180

    stack_list = list()
    for i in range(nrow):
        # all in degress
        y2 = ymax_pi - i*cellsize_pi
        y1 = y2 - cellsize_pi

        # calculate area
        ith_area = (r * r * cellsize_pi) * (numpy.sin(y2) - numpy.sin(y1))

        # all same latitude cells have same areas
        ith_array = numpy.array([ith_area], 'float32')

        # append to list for vstack later
        stack_list.append(ith_array)

    # create ndarray using vstack; note the difference in storage. e.g. i = 0 refers to the bottom row in raster
    # whilst it is the first row in the array
    result = numpy.vstack(stack_list)

    print result.shape
    return result

def forest(workspace, fc, outputfile, fail_log=None):
    # input data mosaic
    rasterpath = r"C:\Ys\Hansen2013\loss_year.gdb\hansenlossyear"

    # set workspace
    if not os.path.exists(workspace):
        os.mkdir(workspace)
    workspace_clip = workspace + os.sep + 'clip'
    if not os.path.exists(workspace_clip):
        os.mkdir(workspace_clip)

    # os workspace
    os.chdir(workspace)
    # arcgis workspace
    arcpy.env.workspace = workspace_clip

    # create header:
    headerlist = ['wdpaid',
            'pa_name',
            'year',
            'count_pixel',
            'total_area']

    # for assessing result dict
    datalist = headerlist[2:]

    header = ','.join(headerlist) + '\n'

    # create logs etc
    if not fail_log:
        where_clause = None
        f = codecs.open(outputfile, 'w', 'utf-8')
        f.write(header)
        f.close()

    else:
        # if with list

        where_clause = '\"wdpaid\" in (' + Yichuan10.CreateListFromTxtTable(fail_log) + ')'


    raster = arcpy.Raster(rasterpath)
    pa_set = set(Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(fc, 'wdpaid'))
    pa_unfinish_set = set()

    # where_clause = '\"wdpaid\" = 2017'

    # PA clipping carbon rasters
    with arcpy.da.SearchCursor(fc, ('wdpaid', 'name', 'SHAPE@'), where_clause=where_clause) as cursor:
        # for each site
        for row in cursor:
            # if the raster extent contains the feature geom, clip rasters
            geom = row[2]
            wdpaid = row[0]
            pa_name = row[1]
            try:
                if raster.extent.overlaps(geom) or raster.extent.contains(geom) :

                    out_ras = str(wdpaid) +'.tif'

                    # clip
                    clip_raster(geom, rasterpath, out_ras, no_data=0)

                    # output
                    f = codecs.open(outputfile, 'a', 'utf-8')

                    # returns a dictionary
                    result = desforestation(arcpy.env.workspace+os.sep+out_ras, 0)

                    # add to line list
                    for year in result:
                        # for each wdpaid
                        line = str(wdpaid) + ',\"' + pa_name + '\",'

                        # year, count pixels, total area
                        all_results = [year, result[year][0], result[year][1]]

                        # write result
                        line += ','.join([str(x) for x in all_results])
                        f.write(line)
                        f.write('\n')

                    # complete
                    print 'complete: ' + str(wdpaid)

                else:
                    print 'pass: ' + str(wdpaid)

                # finally remove id
                pa_set.remove(wdpaid)

            except Exception as e:
                pa_unfinish_set.add(wdpaid)
                Yichuan10.Printboth('Error: ' + str(wdpaid))
                Yichuan10.Printboth(str(e))
                Yichuan10.Printboth(sys.exc_info()[0])
            finally:

                print '-----\n'

        f.close()
        Yichuan10.ExportListToTxt(pa_set, 'log_left.txt')
        Yichuan10.ExportListToTxt(pa_unfinish_set, 'log_fail.txt')


def desforestation(ras, nodata):
    """input raster path -> return stats"""
    """input raster path -> return stats"""

    # get area grid
    area_grid = raster_area_lat(ras) # true WGS84 spheroid
    # getting numpy object

    #ras_np_raw = arcpy.RasterToNumPyArray(ras, ncols = ncols, nrows = nrows, nodata_to_value= nodata)
    ras_np_raw = gdal_tif_to_numpy(ras)
    # masking data not need as further masked below
    # ras_np = numpy.ma.masked_values(ras_np_raw, nodata)

    # 0 - no loss, 1 - change in 2000-2001, .. 12 change 2011-2012
    years = range(0, 14)
    year_dict = dict()

    for year in years:
        # get subset of the year, i.e. all other valuse are masked
        # ras_sub = numpy.ma.masked_not_equal(ras_np_raw, year)

        # the mask is useful
        ras_sub_mask = numpy.ma.masked_equal(ras_np_raw, year)

        # use count (no mask) NOT size (including mask)
        # count_pixel = ras_sub.count()
        count_pixel = ras_sub_mask.mask.sum()

        # True is treated as 1
        total_area = (ras_sub_mask.mask * area_grid).sum()

        year_dict[year] = [count_pixel, total_area]

    return year_dict