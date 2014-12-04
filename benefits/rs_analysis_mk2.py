#-------------------------------------------------------------------------------
# Name:        WH benefits related
# Purpose:
#
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
#
# Created:     2014/01/21
# Modified:     2014/01/29: corrected area calculation
#               2014/02/03: added forest extent
# Copyright:   (c) Yichuan Shi 2014


#-------------------------------------------------------------------------------
import os, sys, codecs
import arcpy, numpy, math

import Yichuan10

from YichuanRAS import *

# in order to work with spatialanalyst
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput= True


def carbon(workspace, fc, outputfile, UID, UID_name):

    base_rasters = dict()
    base_rasters['africa'] = [r"C:\Ys\reg\Africa\carbon_saatchi_111103\africa_carbon_1km.tif",
                            r"C:\Ys\reg\Africa\carbon_saatchi_111103\africa_carbon_error_1km.tif",
                            r"C:\Ys\data.gdb\vcf_africa_2000_resample"]

    base_rasters['asia'] = [r"C:\Ys\reg\Asia\carbon_saatchi_111103\asia_carbon_1km.tif",
                            r"C:\Ys\reg\Asia\carbon_saatchi_111103\asia_carbon_error_1km.tif",
                            r"C:\Ys\data.gdb\vcf_asia_2000_resample"]

    base_rasters['america'] = [r"C:\Ys\reg\LatinAmerica\carbon_saatchi_111103\america_carbon_1km.tif",
                               r"C:\Ys\reg\LatinAmerica\carbon_saatchi_111103\america_carbon_error_1km.tif",
                               r"C:\Ys\data.gdb\vcf_america_2000_resample"]


    # set workspace
    if not os.path.exists(workspace):
        os.mkdir(workspace)
    workspace_clip = workspace + os.sep + 'clip'
    if not os.path.exists(workspace_clip):
        os.mkdir(workspace_clip)

    os.chdir(workspace)
    arcpy.env.workspace = workspace_clip

    # create logs etc
    f = codecs.open(outputfile, 'w', 'utf-8')

    # create header:
    headerlist = ['region', 'wdpaid', 'pa_name']
    for thres in [10, 25, 30]:
        headerlist.append('total_carbon' + str(thres))
        headerlist.append('forest_area' + str(thres))
        headerlist.append('density'+ str(thres))
        headerlist.append('abs_error' + str(thres))
        headerlist.append('rel_error' + str(thres))

    header = ','.join(headerlist) + '\n'


    f.write(header)
    f.close()

    # loop over all rasters
    for region in base_rasters.keys():
        # get region rasters
        raster = arcpy.Raster(base_rasters[region][0])
        raster_error = arcpy.Raster(base_rasters[region][1])
        raster_forest = arcpy.Raster(base_rasters[region][2])


        pa_set = set(Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(fc, UID))
        pa_unfinish_set = set()

        # PA clipping carbon rasters
        with arcpy.da.SearchCursor(fc, (UID, UID_name, 'SHAPE@')) as cursor:
            # for each site
            for row in cursor:
                # if the raster extent contains the feature geom, clip rasters
                geom = row[2]
                wdpaid = row[0]
                pa_name = row[1]
                try:
                    if raster.extent.overlaps(geom) or raster.extent.contains(geom) :
                        print 'Clip: ' + str(wdpaid)
                        out_ras = region + '_' + str(wdpaid) +'.tif'
                        out_ras_error = region + '_' + str(wdpaid) + '_error.tif'
                        out_ras_forest = region + '_' + str(wdpaid) + '_forest.tif'

                        # clip
                        clip_raster(geom, raster, out_ras, 0)
                        clip_raster(geom, raster_error, out_ras_error, 0)
                        clip_raster(geom, raster_forest, out_ras_forest, 0)


                        # output
                        f = codecs.open(outputfile, 'a', 'utf-8')

                        if type(pa_name) == type('text') or type(pa_name) == type(u'\xe1'):
                            line = region + ',' + str(wdpaid) + ',\"' + pa_name + '\",'
                        else:
                            line = region + ',' + str(wdpaid) + ',\"' + str(pa_name) + '\",'

                        all_results = []
                        # need to specify threshold for forests 10,25,30 tree cover (see data)
                        for thres in [10, 25, 30]:
                            # calculate carbon: caution the extents must be indentical
                            result = pa_carbon_mk3(out_ras, out_ras_error, out_ras_forest, thres)
                            all_results.extend(result)

                        # write result
                        line += ','.join([str(x) for x in all_results])
                        f.write(line)
                        f.write('\n')
                        print 'Complete:', str(wdpaid)
                    else:
                        print 'Pass:', str(wdpaid)

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

def water(workspace, fc, outputfile, fail_log=None):

    # data
    months = map(lambda x: '0'+x, map(str, range(1, 10))) + map(str, range(10, 13))
    etdict = dict()
    petdict = dict()
    pdict = dict()

    for month in months:
        etdict[month] = r"C:\Ys\water_workspace\avg" + os.sep + 'et_' + month + '.tif'
        petdict[month] = r"C:\Ys\water_workspace\avg" + os.sep + 'pet_' + month + '.tif'
        pdict[month] = r"C:\Ys\water_workspace\prec" + os.sep + 'prec' + '_' + str(int(month))

    inputdict = {'et': etdict,
                'pet': petdict,
                'p': pdict}

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

    # create logs etc
    f = codecs.open(outputfile, 'w', 'utf-8')

    # create header:
    headerlist = ['wdpaid',
            'pa_name',
            'type',
            'month',
            'total_amount',
            'total_amount_sphere',
            'total_area',
            'total_area_sphere',
            'simple_mean',
            'true_mean',
            'true_mean_sphere',
            'count_pixel']

    # for assessing result dict
    datalist = headerlist[4:]

    header = ','.join(headerlist) + '\n'
    f.write(header)
    f.close()

    # loop over all rasters
    for raster_type in inputdict.keys():

        # get data type
        raster_dict = inputdict[raster_type]

        # for each month
        for month in sorted(raster_dict.keys()):

            rasterpath = raster_dict[month]
            raster = arcpy.Raster(rasterpath)
            pa_set = set(Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(fc, 'wdpaid'))
            pa_unfinish_set = set()

            #where_clause = '\"wdpaid\" = 900629'
        # PA clipping carbon rasters
            with arcpy.da.SearchCursor(fc, ('wdpaid', 'name', 'SHAPE@')) as cursor:
                # for each site
                for row in cursor:
                    # if the raster extent contains the feature geom, clip rasters
                    geom = row[2]
                    wdpaid = row[0]
                    pa_name = row[1]
                    try:
                        if raster.extent.overlaps(geom) or raster.extent.contains(geom) :
                            print 'Clip: ' + str(wdpaid)
                            out_ras = raster_type + month + '_' + str(wdpaid) +'.tif'

                            # clip
                            clip_raster(geom, rasterpath, out_ras, 0)

                            # output
                            f = codecs.open(outputfile, 'a', 'utf-8')
                            line = str(wdpaid) + ',\"' + pa_name + '\",' + raster_type.upper() + ',' + month + ','

                            all_results = []

                            # returns a dictionary
                            result = ras_each(out_ras)

                            # add to line list
                            for each in datalist:
                                # add a check for et and pet, which have unit of 0.1mm
                                if raster_type == 'p':
                                    all_results.append(result[each])

                                # for et and pet
                                else:
                                    all_results.append(result[each]/10)

                            # write result
                            line += ','.join([str(x) for x in all_results])
                            f.write(line)
                            f.write('\n')
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
                        print '----\n'

        f.close()
        Yichuan10.ExportListToTxt(pa_set, 'log_left.txt')
        Yichuan10.ExportListToTxt(pa_unfinish_set, 'log_fail.txt')


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
    years = range(0, 13)
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

def ras_each(ras):
    """input raster path -> return stats"""
    param = arcpy.Raster(ras)
    nrows = param.height
    ncols = param.width

    # get area grid
    area_grid = raster_area_mk3(ras) # true WGS84 spheroid
    area_grid_sphere = raster_area(ras) # based on a sphere

    # getting numpy object
    ras_np = arcpy.RasterToNumPyArray(ras, ncols = ncols, nrows = nrows, nodata_to_value=0)

    print ras_np.shape

    #  calculate area (0, 1)
    ras_np_copy = numpy.copy(ras_np)
    ras_np_copy[ras_np_copy!=0] = 1

    # count
    count = ras_np_copy[ras_np_copy!=0].size

    # total
    total_amount = (ras_np * area_grid).sum()
    total_amount_sphere = (ras_np * area_grid_sphere).sum()

    total_area = (ras_np_copy * area_grid).sum()
    total_area_sphere = (ras_np_copy * area_grid_sphere).sum()
    try:
        avg = ras_np[ras_np!=0].mean()
    except:
        avg = 'NA'
    try:
        true_avg = total_amount/total_area
    except:
        true_avg = 'NA'
    try:
        true_avg_sphere = total_amount_sphere/total_area_sphere
    except:
        true_avg_sphere = 'NA'

    return {'total_amount': total_amount,
            'total_amount_sphere': total_amount_sphere,
            'total_area': total_area,
            'total_area_sphere': total_area_sphere,
            'simple_mean': avg,
            'true_mean': true_avg,
            'true_mean_sphere':true_avg_sphere,
            'count_pixel': count}


def pa_carbon_mk3(pa_carbon, pa_carbon_error, pa_forest, thres):
    """assuming the input are already in Mg/km2; 10^6 gram per square km
    it returns a tuple of absolute error and relative error - need to take into account
    calculation of cell area"""

    # convert rasters to numpy arrays

    param = arcpy.Raster(pa_carbon)
    nrows = param.height
    ncols = param.width

    carbon = arcpy.RasterToNumPyArray(pa_carbon, ncols = ncols, nrows = nrows, nodata_to_value=0)
    carbon_error = arcpy.RasterToNumPyArray(pa_carbon_error, ncols = ncols, nrows = nrows, nodata_to_value=0)
    forest = arcpy.RasterToNumPyArray(pa_forest, ncols = ncols, nrows = nrows, nodata_to_value=0)

    print carbon.shape, carbon_error.shape, forest.shape

    # debug magnitude: original unit is MgC/ha - this is to convert to MgC/km2
    carbon = carbon * 100
    carbon_error = carbon_error

    # calculate total biomass carbon
    # this is the area grid
##    print forest
    # order important
    forest[forest<thres] = 0
    forest[forest>0] = 1
##    # debug:

    # pa_area, area for each cell in carbon, unit: sqkm
    pa_area = raster_area_mk3(pa_carbon)

    # forest area
    forest_area = (pa_area * forest).sum()

    # total carbon
    total_carbon = (carbon * pa_area * forest).sum()

    # calculate uncertainty, i.e. standard error, error in data is presented without %
    abs_error = math.sqrt((carbon * pa_area * carbon * pa_area * carbon_error * carbon_error * forest / 10000.0).sum())

    # density: forest carbon in forested areas (average) unit: Mg/km2
    if forest_area:
        density = total_carbon/forest_area
    else:
        density = 'NaN'

    # check if total carbon is zero
    if total_carbon:
        rel_error = abs_error/total_carbon
    else:
        rel_error = 'NaN'

    return (total_carbon, forest_area, density, abs_error, rel_error)


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


def raster_area_mk3(raster_object):
    """ this function takes an raster object or path and creates an numpy array with areas per cell
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
    if not isinstance(raster_object, arcpy.Raster):
        raster_object = arcpy.Raster(raster_object)

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

        # all same latitude cells have same areas
        ith_array = numpy.array([ith_area], 'float32')

        # append to list for vstack later
        stack_list.append(ith_array)

    # create ndarray using vstack; note storage. e.g. i = 0 refers to the top row in raster and in np array
    result = numpy.vstack(stack_list)

    # free memory not sure needed
    del raster_object

    print result.shape
    return result


# average rasters, for this to happen one needs to check out licence
# mask is needed to prevent NA + number

# issue regarding average data where no-data is present

def arrange_raster():
    et_dict = compile_raster('ET')
    pet_dict = compile_raster('PET')
    outputfolder = r'D:\Yichuan\WH_benefits\data\water\et\avg'

    # 12 year average
    print 'processing ET:'
    for month in sorted(et_dict.keys()):
        outras = outputfolder + os.sep + 'et_' + month + '.tif'
        raster_average_mk2(et_dict[month], outras)
        print 'Finished year: ', month

    print 'processing PET:'
    for month in sorted(pet_dict.keys()):
        outras = outputfolder + os.sep + 'pet_' + month + '.tif'
        raster_average_mk2(pet_dict[month], outras)
        print 'Finished year: ', month


def raster_average_mk2(rasterobject_list, outras):
    # this function improves the previous version in that no value data is considered
    from arcpy.sa import Con, SetNull, CellStatistics

    n = len(rasterobject_list)

    # get mask
    rastermask_list = list()
    for each in rasterobject_list:
        eachmask = Con(each>32760, 1, 0)
        rastermask_list.append(eachmask)

    sum_mask = CellStatistics(rastermask_list, "SUM")

    # flip values and set null for mask
    # only do this for pixels having more than 6 NoData
##    sum_mask = Con(sum_mask>0, None, 1)
    sum_mask = SetNull(sum_mask>6, 1)

    # it doesn't honor mask
    outras_mask = r"C:\mask_temp.tif"
    sum_mask.save(outras_mask)

    # average, only operate on those valid values
    arcpy.env.mask = outras_mask

    # average
    avg_raster = CellStatistics(rasterobject_list, "MEAN", "DATA")
    avg_raster.save(outras)

    # clear mask
    arcpy.env.mask = None


def raster_average(rasterobject_list, outras):
    from arcpy.sa import Con, SetNull

    n = len(rasterobject_list)

    # get mask
    rastermask_list = list()
    for each in rasterobject_list:
        eachmask = Con(each>32760, 1, 0)
        rastermask_list.append(eachmask)

    sum_mask = rastermask_list[0]
    for each in rastermask_list[1:]:
        sum_mask += each

    # flip values and set null for mask
##    sum_mask = Con(sum_mask>0, None, 1)
    sum_mask = SetNull(sum_mask>0, 1)

    # it doesn't honor mask
    outras_mask = r"C:\mask_temp.tif"
    sum_mask.save(outras_mask)

    # average, only operate on those valid values
    arcpy.env.mask = outras_mask

##    arcpy.env.mask = sum_mask.catalogPath

    sum_raster = rasterobject_list[0]
    for each in rasterobject_list[1:]:
        sum_raster += each

    avg_raster = sum_raster/n

    avg_raster.save(outras)

    # clear mask
    arcpy.env.mask = None

# get a dictionary of month - years raster object
def compile_raster(type = 'ET'):
    # 'ET' or 'PET'

    folder = r"D:\Yichuan\WH_benefits\data\water\et\GEOTIFF_0.05degree"
    # 2000, 2001,... 2012
    years = map(str, range(2000, 2013))

    # 01, 02, ... 12
    months = map(lambda x: '0'+x, map(str, range(1, 10))) + map(str, range(10, 13))

    monthdict = dict()
    # by month
    for month in months:
        monthdict[month] = list()
        for year in years:
            rastername = "MOD16A2_" + type + "_0.05deg_GEO_" + year + "M" + month + '.tif'
            raster = arcpy.Raster(folder + os.sep + rastername)
            monthdict[month].append(raster)

    return monthdict

# carbon analysis
def run_carbon():
    print 'Run: carbon'

    UID = 'wdpaid'
    UID_name = 'name'
    workspace = r"C:\raster_workspace_3\carbon_wh"
    fc = r"C:\Ys\whs_dump_140113.shp"
    outputfile = 'result_wh.csv'
    carbon(workspace, fc, outputfile, UID, UID_name)
    print '---------Finish WH--------'


    UID = 'wdpaid'
    UID_name = 'name'
    workspace = r"C:\raster_workspace_3\carbon_pa"
    fc = r"C:\Ys\data.gdb\biome_intersect_pa_dis"
    outputfile = 'result_pa.csv'
    carbon(workspace, fc, outputfile, UID, UID_name)
    print '---------Finish PA--------'


    UID = 'OBJECTID'
    UID_name = 'name'
    workspace = r"C:\raster_workspace_3\carbon_pa_dis"
    fc = r"C:\Ys\data.gdb\biome_intersect_pa_dis_complete"
    outputfile = 'result_pa_dis.csv'
    carbon(workspace, fc, outputfile, UID, UID_name)
    print '---------Finish PA-dis -------'


    UID = 'OBJECTID'
    UID_name = 'BIOME'
    workspace = r"C:\raster_workspace_3\carbon_biome"
    fc = r"C:\Ys\data.gdb\biome_1237"
    outputfile = 'result_biome.csv'
    carbon(workspace, fc, outputfile, UID, UID_name)
    print '---------Finish all biome----------'


def run_forest():
    #forest
    print 'Run: forest'
    workspace = r"C:\raster_workspace_3\forest"
    fc = r"C:\Ys\whs_dump_140113.shp"
    outputfile = "resultforest.csv"
    forest(workspace, fc, outputfile)

def run_water():
    # water
    print 'Run: water'
    workspace = r"C:\raster_workspace_3\water"
    fc = r"C:\Ys\whs_dump_140113.shp"
    outputfile = "resultwater.csv"
    water(workspace, fc, outputfile)


