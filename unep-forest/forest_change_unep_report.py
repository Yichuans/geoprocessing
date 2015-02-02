#-------------------------------------------------------------------------------
# Name:        UNEP regional report - forest change by Hansen 2013 data
# Purpose:	   Adapt WH forest script to estimate forest change in UNEP regions
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
# Created:     2015/01/23
#-------------------------------------------------------------------------------
import Yichuan10
import os, sys, codecs
import arcpy, numpy, math
from Yichuan10 import simple_time_tracker

from YichuanRAS import *


def forest(workspace, fc, outputfile, fail_log=None):
    # input data mosaic
    rasterpath = r"D:\Yichuan\Hansen\data.gdb\loss_year"

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
    headerlist = ['patch_id',
            'year',
            'count_pixel',
            'total_area_km2']

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
        where_clause = '\"patch_id\" in (' + Yichuan10.CreateListFromTxtTable(fail_log) + ')'


    raster = arcpy.Raster(rasterpath)
    pa_set = set(Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(fc, 'patch_id'))
    pa_unfinish_set = set()

    # where_clause = '\"patch_id\" = 2017'

    # PA clipping carbon rasters
    with arcpy.da.SearchCursor(fc, ('patch_id', 'SHAPE@'), where_clause=where_clause) as cursor:
        # for each site
        for row in cursor:
            # if the raster extent contains the feature geom, clip rasters
            geom = row[1]
            patch_id = row[0]
            try:
                if raster.extent.overlaps(geom) or raster.extent.contains(geom) :

                    out_ras = str(patch_id) +'.tif'

                    # clip
                    clip_raster(geom, rasterpath, out_ras, no_data=0)

                    # output
                    f = codecs.open(outputfile, 'a', 'utf-8')

                    # returns a dictionary
                    result = desforestation(arcpy.env.workspace+os.sep+out_ras)

                    # add to line list
                    for year in result:
                        # for each patch_id
                        line = str(patch_id) + ','

                        # year, count pixels, total area
                        all_results = [year, result[year][0], result[year][1]]

                        # write result
                        line += ','.join([str(x) for x in all_results])
                        f.write(line)
                        f.write('\n')

                    # complete
                    print 'complete: ' + str(patch_id)

                else:
                    print 'pass: ' + str(patch_id)

                # finally remove id
                pa_set.remove(patch_id)

            except Exception as e:
                pa_unfinish_set.add(patch_id)
                Yichuan10.Printboth('Error: ' + str(patch_id))
                Yichuan10.Printboth(str(e))
                Yichuan10.Printboth(sys.exc_info()[0])
            finally:

                print '-----\n'

        f.close()
        Yichuan10.ExportListToTxt(pa_set, 'log_left.txt')
        Yichuan10.ExportListToTxt(pa_unfinish_set, 'log_fail.txt')


def desforestation(ras):
    """input raster path -> return stats"""
    """input raster path -> return stats"""

    # get area grid
    area_grid = raster_area_lat(ras) # true WGS84 spheroid

    # getting numpy object
    ras_np_raw = gdal_tif_to_numpy(ras)
    # masking data not need as further masked below

    # 0 - no loss, 1 - change in 2000-2001, .. 12 change 2011-2013
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

# main function
def main():
	workspace = r"D:\Yichuan\BrianO\UNEP-report\analysis"
	fc = r"D:\Yichuan\BrianO\UNEP-report\ce_africa_selection.shp"
	outputfile = "result.txt"
	forest(workspace, fc, outputfile)


@simple_time_tracker
def _test():
	#out_ras = r"C:\Ys\Hansen2013\wh_clip\clip" + os.sep + str(2571) + '.tif'
	out_ras = r"C:\Users\yichuans\Documents\ArcGIS\loss_year_Clip_china.tif"
	return desforestation(out_ras)


main()