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

arcpy.env.overwriteOutput= True

def main(workspace, fc, outputfile):
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


        pa_set = set(Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(fc, 'wdpaid'))
        pa_unfinish_set = set()

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
                        print 'clip: ' + str(wdpaid)
                        out_ras = region + '_' + str(wdpaid) +'.tif'
                        out_ras_error = region + '_' + str(wdpaid) + '_error.tif'
                        out_ras_forest = region + '_' + str(wdpaid) + '_forest.tif'

                        # clip
                        clip_raster(geom, raster, out_ras)
                        clip_raster(geom, raster_error, out_ras_error)
                        clip_raster(geom, raster_forest, out_ras_forest)


                        # output
                        f = codecs.open(outputfile, 'a', 'utf-8')
                        line = region + ',' + str(wdpaid) + ',\"' + pa_name + '\",'

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
                    else:
                        print 'pass: ' + str(wdpaid)

                    # finally remove id
                    pa_set.remove(wdpaid)

                except Exception as e:
                        pa_unfinish_set.add(wdpaid)
                        Yichuan10.Printboth('Error: ' + str(wdpaid))
                        Yichuan10.Printboth(str(e))
                        Yichuan10.Printboth(sys.exc_info()[0])

        f.close()
        Yichuan10.ExportListToTxt(pa_set, 'log_left.txt')
        Yichuan10.ExportListToTxt(pa_unfinish_set, 'log_fail.txt')

def pa_carbon(pa_carbon, pa_carbon_error):
    """assuming the input are already in Mg/km2; 10^6 gram per square km
    it returns a tuple of absolute error and relative error"""

    # convert rasters to numpy arrays
    carbon = arcpy.RasterToNumPyArray(pa_carbon, nodata_to_value=0)
    carbon_error = arcpy.RasterToNumPyArray(pa_carbon_error, nodata_to_value=0)

    # calculate total biomass carbon
    total_carbon = carbon.sum()

    # calculate uncertainty, i.e. standard error, error in data is presented without %
    abs_error = math.sqrt((carbon * carbon * carbon_error * carbon_error / 10000.0).sum())
    rel_error = abs_error/total_carbon

    return (total_carbon, abs_error, rel_error)

def pa_carbon_mk2(pa_carbon, pa_carbon_error):
    """assuming the input are already in Mg/km2; 10^6 gram per square km
    it returns a tuple of absolute error and relative error - need to take into account
    calculation of cell area"""

    # convert rasters to numpy arrays
    carbon = arcpy.RasterToNumPyArray(pa_carbon, nodata_to_value=0)
    carbon_error = arcpy.RasterToNumPyArray(pa_carbon_error, nodata_to_value=0)

    # calculate total biomass carbon
    # this is the area grid
    pa_area = raster_area(carbon)
    total_carbon = (carbon * pa_area).sum()

    # calculate uncertainty, i.e. standard error, error in data is presented without %
    abs_error = math.sqrt((carbon * pa_area * carbon * pa_area * carbon_error * carbon_error / 10000.0).sum())


    return (total_carbon, abs_error, rel_error)

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


    # debug magnitude: original unit is MgC/ha - this is to convert to MgC/km2
    carbon = carbon * 100
    carbon_error = carbon_error

    # calculate total biomass carbon
    # this is the area grid

    # order important
    forest[forest<thres] = 0
    forest[forest>0] = 1

    # pa_area, area for each cell in carbon, unit: sqkm
    pa_area = raster_area(pa_carbon)

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
    """ this function takes an raster object and creates an numpy array with areas per cell
    This should be used in conjunction with the numpy array created using ArcGIS's
    RasterToNumpyArray"""
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
    ymin = raster_object.extent.YMin

    # in rad
    cellsize_pi = cellsize * numpy.pi / 180
    ymin_pi = ymin * numpy.pi / 180

    stack_list = list()
    for i in range(nrow):
        # all in degress
        y1 = ymin_pi + i*cellsize_pi
        y2 = y1 + cellsize_pi

        # calculate area
        ith_area = (r * r * cellsize_pi) * (numpy.sin(y2) - numpy.sin(y1))

        # all same latitude cells have same areas
        ith_array = numpy.array([ith_area]*ncol)

        # append to list for vstack later
        stack_list.append(ith_array)

    # create ndarray using vstack; note the difference in storage. e.g. i = 0 refers to the bottom row in raster
    # whilst it is the first row in the array
    result = numpy.vstack(stack_list)
    return result

def raster_area_mk2(raster_object):
    """ this function takes an raster object and creates an numpy array with areas per cell
    This should be used in conjunction with the numpy array created using ArcGIS's
    RasterToNumpyArray"""
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


    cellsize = raster_object.meanCellHeight
    nrow = raster_object.height
    ncol = raster_object.width
    ymin = raster_object.extent.YMin

    # in rad
    cellsize_pi = cellsize * numpy.pi / 180
    ymin_pi = ymin * numpy.pi / 180

    stack_list = list()
    for i in range(nrow):
        # all in degress
        y1 = ymin_pi + i*cellsize_pi
        y2 = y1 + cellsize_pi

        # calculate area
        ith_area = 0.5 * a * a * (1 - e * e) * cellsize_pi * (f(y2) - f(y1))

        # all same latitude cells have same areas
        ith_array = numpy.array([ith_area]*ncol)

        # append to list for vstack later
        stack_list.append(ith_array)

    # create ndarray using vstack; note the difference in storage. e.g. i = 0 refers to the bottom row in raster
    # whilst it is the first row in the array
    result = numpy.vstack(stack_list)
    return result



# clip raster
def clip_raster(featurelayer, in_raster, out_raster):
    try:
        arcpy.Clip_management(in_raster, "#", out_raster, featurelayer, "0", "ClippingGeometry")
    except:
        print "Clip failed."
        print arcpy.GetMessages()



# base rasters 2010
##base_rasters = dict()
##base_rasters['africa'] = [r"C:\Ys\reg\Africa\carbon_saatchi_111103\africa_carbon_1km.tif",
##                        r"C:\Ys\reg\Africa\carbon_saatchi_111103\africa_carbon_error_1km.tif",
##                        r"C:\Ys\data.gdb\vcf_africa_resample"]
##
##base_rasters['asia'] = [r"C:\Ys\reg\Asia\carbon_saatchi_111103\asia_carbon_1km.tif",
##                        r"C:\Ys\reg\Asia\carbon_saatchi_111103\asia_carbon_error_1km.tif",
##                        r"C:\Ys\data.gdb\vcf_asia_resample"]
##
##base_rasters['america'] = [r"C:\Ys\reg\LatinAmerica\carbon_saatchi_111103\america_carbon_1km.tif",
##                           r"C:\Ys\reg\LatinAmerica\carbon_saatchi_111103\america_carbon_error_1km.tif",
##                           r"C:\Ys\data.gdb\vcf_america_resample"]


# base rasters 2000
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


##if __name__ == '__main__':
##    workspace = r"D:\Yichuan\WH_benefits\analysis"
##    fc = r"D:\Yichuan\WHS.gdb\whs_dump_140113"
##    outputfile = 'result_wh.csv'
##    main(workspace, fc, outputfile)

##if __name__ == '__main__':
##    workspace = r"C:\Ys\carbon_workspace_pa"
##    fc = r"U:\WDPA_Jan2014.gdb\WDPA_poly_Jan2014"
##    outputfile = 'result_pa.csv'
##    main(workspace, fc, outputfile)

##if __name__ == '__main__':
##    workspace = r"C:\Ys\carbon_workspace_wh_2000"
##    fc = r"C:\Ys\whs_dump_140113.shp"
##    outputfile = 'result_wh.csv'
##    main(workspace, fc, outputfile)


##if __name__ == '__main__':
##    workspace = r"C:\Ys\carbon_workspace_wh"
##    fc = r"C:\Ys\test_br.shp"
##    outputfile = 'result_test.csv'
##    main(workspace, fc, outputfile)

# all WH
workspace = r"C:\Ys\carbon_wh"
fc = r"C:\Ys\whs_dump_140113.shp"
outputfile = 'result_wh.csv'
main(workspace, fc, outputfile)

workspace = r"C:\Ys\carbon_pa"
fc = r"C:\Ys\data.gdb\biome_intersect_pa_dis"
outputfile = 'result_pa.csv'
main(workspace, fc, outputfile)

workspace = r"C:\Ys\carbon_pa_dis"
fc = r"C:\Ys\data.gdb\biome_intersect_pa_dis_complete"
outputfile = 'result_pa_dis.csv'
main(workspace, fc, outputfile)

workspace = r"C:\Ys\carbon_biome"
fc = r"C:\Ys\data.gdb\biome_1237"
outputfile = 'result_biome.csv'
main(workspace, fc, outputfile)