#-------------------------------------------------------------------------------
# Name:        UNEP regional report - forest change by Hansen 2013 data
# Purpose:	   Adapt WH forest script to estimate forest change in UNEP regions
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
# Created:     2015/01/23
# Revision:    2015/03/12
#-------------------------------------------------------------------------------
import Yichuan10
import os, sys, codecs
import numpy, math
import arcpy
from Yichuan10 import simple_time_tracker


from YichuanRAS import *

# CONSTANT
# unique id in fc, this needs to be created if not exist
patch_id = 'patch_id'

clip_loss = 'clip_loss'
clip_base = 'clip_base'

def desforestation_loss_year(ras):
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
        # need to include dtype = 'float64' otherwise the calcaulate
        # will produce incorrect result (overflow?)

        total_area = (ras_sub_mask.mask * area_grid).sum(dtype='float64')

        year_dict[year] = [count_pixel, total_area]

    return year_dict



def desforestation_base(ras, threshold=25):
    """input raster path -> return stats"""
    """input raster path -> return stats"""

    # get area grid
    area_grid = raster_area_lat(ras) # true WGS84 spheroid

    # getting numpy object
    ras_np_raw = gdal_tif_to_numpy(ras)
    # masking data not need as further masked below

    # create mask greater than 25, the same used by Hansen
    # ras_sub_mask = numpy.ma.masked_greater_equal(ras_np_raw, 10)
    ras_sub_mask = numpy.ma.masked_greater_equal(ras_np_raw, threshold)

    # use count (no mask) NOT size (including mask)
    # count_pixel = ras_sub.count()
    count_pixel = ras_sub_mask.mask.sum()

    # True is treated as 1
    total_area = (ras_sub_mask.mask * area_grid).sum(dtype ='float64')

    result = [count_pixel, total_area]

    return result


def alt_desforestation_no_treshold(ras):
    """input raster path -> return stats"""
    """input raster path -> return stats"""
    ### This function calculates sub-pixel forest loss


    # get area grid
    area_grid = raster_area_lat(ras) # true WGS84 spheroid

    # getting numpy object
    ras_np_raw = gdal_tif_to_numpy(ras)
    # masking data not need as further masked below

    # create mask greater than 10
    ras_sub_mask = numpy.ma.masked_greater_equal(ras_np_raw, 10)

    # use count (no mask) NOT size (including mask)
    # count_pixel = ras_sub.count()
    count_pixel = ras_sub_mask.mask.sum()

    # True is treated as 1
    total_area = (ras_np_raw * ras_sub_mask.mask * area_grid / 100).sum(dtype ='float64')

    result = [count_pixel, total_area]

    return result


def clip_forest_raster(workspace, clip, fc, raster_path, fail_log = None):
    # if not exist
    workspace_clip = workspace + os.sep + clip
    if not os.path.exists(workspace_clip):
        os.mkdir(workspace_clip)

    raster = arcpy.Raster(raster_path)
    # debug
    # print 'debug', patch_id
    pa_list = Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(fc, patch_id)
    pa_set = set(pa_list)
    pa_unfinish_set = set()

    # if fail log exist
    if not fail_log:
        where_clause = None
    else:
        # if with list
        where_clause = '\"' + patch_id + '\" in (' + Yichuan10.CreateListFromTxtTable(fail_log) + ')'

    # Make sure the clips go to workspace_clip
    arcpy.env.workspace = workspace_clip

    # PA clipping carbon rasters
    with arcpy.da.SearchCursor(fc, (patch_id, 'SHAPE@'), where_clause=where_clause) as cursor:
        # for each site
        for row in cursor:
            # if the raster extent contains the feature geom, clip rasters
            geom = row[1]
            patch_id_value = row[0]
            try:
                if raster.extent.overlaps(geom) or raster.extent.contains(geom) :

                    out_ras = str(patch_id_value) +'.tif'

                    # clip
                    clip_raster(geom, raster_path, out_ras, no_data=0)

                    # complete
                    print 'complete clipping: ' + str(patch_id_value)

                else:
                    print 'pass: ' + str(patch_id_value)

                # finally remove id
                pa_set.remove(patch_id_value)

            except Exception as e:
                pa_unfinish_set.add(patch_id_value)
                print 'Error: ' + str(patch_id_value)
                print str(e)
                print sys.exc_info()[0]



# run forest loss
def forest_loss(workspace, fc, outputfile_loss):
    os.chdir(workspace)
      

    # LOSS HERE
    headerlist = ['patch_id','year','count_pixel','total_area_km2']
    header = ','.join(headerlist) + '\n'
    f = codecs.open(outputfile_loss, 'w', 'utf-8')
    f.write(header)
    f.close()

    patchlist = Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(fc, patch_id)

    # for each patchlist
    f = codecs.open(outputfile_loss, 'a', 'utf-8')

    for patch_id_value in patchlist:

        try:
            out_ras = str(patch_id_value) +'.tif'

            # input ras
            ras = workspace + os.sep + clip_loss + os.sep + out_ras

            # returns a dictionary
            result = desforestation_loss_year(ras)

            for year in result:
                # for each patch_id_value
                line = str(patch_id_value) + ','

                # year, count pixels, total area
                all_results = [year, result[year][0], result[year][1]]

                # write result
                line += ','.join([str(x) for x in all_results])
                f.write(line)
                f.write('\n')
            print 'Completed,', patch_id_value

        except Exception as e:
            Yichuan10.Printboth('Error: ' + str(patch_id_value))
            Yichuan10.Printboth(str(e))
            Yichuan10.Printboth(sys.exc_info()[0])

        finally:
            print '-----\n'

    f.close()

    print 'YEAR loss Completed'

    for i in range(5):
      print '----------------'


def forest_base(workspace, fc, outputfile_base, threshold=25):
    os.chdir(workspace)


        # BASE HERE
    headerlist = ['patch_id',
        'count_pixel',
        'total_area_km2']

    header = ','.join(headerlist) + '\n'
    f = codecs.open(outputfile_base, 'w', 'utf-8')
    f.write(header)
    f.close()

    patchlist = Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(fc, patch_id)


    # for each patch
    f = codecs.open(outputfile_base, 'a', 'utf-8')

    for patch_id_value in patchlist:

        try:
            out_ras = str(patch_id_value) +'.tif'

            # input ras
            ras = workspace + os.sep + clip_base + os.sep + out_ras

            # returns a dictionary
            # result = desforestation_no_treshold(ras)
            result = desforestation_base(ras, threshold)

            # for each patch_id_value
            line = str(patch_id_value) + ','

            # year, count pixels, total area
            all_results = [result[0], result[1]]

            # write result
            line += ','.join([str(x) for x in all_results])
            f.write(line)
            f.write('\n')

            print 'Completed,', patch_id_value

        except Exception as e:
            Yichuan10.Printboth('Error: ' + str(patch_id_value))
            Yichuan10.Printboth(str(e))
            Yichuan10.Printboth(sys.exc_info()[0])

        finally:
            print '-----\n'

    f.close()

def asian_pacific():
    # patch feature class
    fc = r"D:\Yichuan\BrianO\AP\ap_teow.shp"
    workspace = r"D:\Yichuan\BrianO\AP"
    raster_loss_year = r"D:\Yichuan\Hansen\data.gdb\loss_year"
    raster_base = r"D:\Yichuan\Hansen\data.gdb\treecover"

#    CLIP for both LOSS and BASE -------------------------------
#        LOSS
    clip_forest_raster(workspace, clip_loss, fc, raster_loss_year)
        # BASE
    clip_forest_raster(workspace, clip_base, fc, raster_base)

    # LOSS ----------------------------------------
    loss_workspace = workspace

    # output
    outputfile_loss = "result_loss.txt"

    # run forest loss
    forest_loss(loss_workspace, fc, outputfile_loss)

    # BASE -----------------------------------------------
    base_workspace = workspace

    # output
    outputfile_base = "result_base25.txt"

    # run forest loss
    forest_base(base_workspace, fc, outputfile_base)    


def west_asia():
    # patch feature class
    fc = r"D:\Yichuan\BrianO\WA\wa_teow.shp"
    workspace = r"D:\Yichuan\BrianO\WA"
    raster_loss_year = r"D:\Yichuan\Hansen\data.gdb\loss_year"
    raster_base = r"D:\Yichuan\Hansen\data.gdb\treecover"

    # CLIP for both LOSS and BASE -------------------------------
    #     LOSS
    clip_forest_raster(workspace, clip_loss, fc, raster_loss_year)
        # BASE
    clip_forest_raster(workspace, clip_base, fc, raster_base)

    # LOSS ----------------------------------------
    loss_workspace = workspace

    # output
    outputfile_loss = "result_loss.txt"

    # run forest loss
    forest_loss(loss_workspace, fc, outputfile_loss)

    # BASE -----------------------------------------------
    base_workspace = workspace

    # output
    outputfile_base = "result_base25.txt"

    # run forest loss
    forest_base(base_workspace, fc, outputfile_base)    

# asian_pacific()
# west_asia()

# fix base

def asian_pacific_10():
    # patch feature class
    fc = r"D:\Yichuan\BrianO\AP\ap_teow.shp"
    workspace = r"D:\Yichuan\BrianO\AP"
    raster_loss_year = r"D:\Yichuan\Hansen\data.gdb\loss_year"
    raster_base = r"D:\Yichuan\Hansen\data.gdb\treecover"


    # BASE -----------------------------------------------
    base_workspace = workspace

    # output
    outputfile_base = "result_base10.txt"

    # run forest loss
    forest_base(base_workspace, fc, outputfile_base, 10)    


def west_asia_10():
    # patch feature class
    fc = r"D:\Yichuan\BrianO\WA\wa_teow.shp"
    workspace = r"D:\Yichuan\BrianO\WA"
    raster_loss_year = r"D:\Yichuan\Hansen\data.gdb\loss_year"
    raster_base = r"D:\Yichuan\Hansen\data.gdb\treecover"


    # BASE -----------------------------------------------
    base_workspace = workspace

    # output
    outputfile_base = "result_base10.txt"

    # run forest loss
    forest_base(base_workspace, fc, outputfile_base, 10)    

# asian_pacific_10()
# west_asia_10()

# test
# a = 20
# def _test():
#     # a = 10 
#     print a
#     print patch_id

# _test()

# 2016 LAC run
def lac():
    # patch feature class
    fc = r"E:\Yichuan\BrianO\LAC\lac_teow.shp"
    workspace = r"E:\Yichuan\BrianO\LAC"
    raster_loss_year = r"E:\Yichuan\Hansen\data.gdb\loss_year"
    raster_base = r"E:\Yichuan\Hansen\data.gdb\treecover"

    # CLIP for both LOSS and BASE -------------------------------
    #     LOSS
    clip_forest_raster(workspace, clip_loss, fc, raster_loss_year)
        # BASE
    clip_forest_raster(workspace, clip_base, fc, raster_base)

    # LOSS ----------------------------------------
    loss_workspace = workspace

    # output
    outputfile_loss = "result_loss.txt"

    # run forest loss
    forest_loss(loss_workspace, fc, outputfile_loss)

    # BASE -----------------------------------------------
    base_workspace = workspace

    # output
    outputfile_base = "result_base10.txt"

    # run forest loss
    forest_base(base_workspace, fc, outputfile_base, 10)


def lac_calculate_already_clipped():
    # patch feature class
    fc = r"E:\Yichuan\BrianO\LAC\lac_teow.shp"
    workspace = r"E:\Yichuan\BrianO\LAC"
    raster_loss_year = r"E:\Yichuan\Hansen\data.gdb\loss_year"
    raster_base = r"E:\Yichuan\Hansen\data.gdb\treecover"

    # CLIP for both LOSS and BASE -------------------------------
    # LOSS ----------------------------------------
    loss_workspace = workspace

    # output
    outputfile_loss = "result_loss.txt"

    # run forest loss
    forest_loss(loss_workspace, fc, outputfile_loss)

    # BASE -----------------------------------------------
    base_workspace = workspace

    # output
    outputfile_base = "result_base10.txt"

    # run forest loss
    forest_base(base_workspace, fc, outputfile_base, 10)


lac_calculate_already_clipped()
