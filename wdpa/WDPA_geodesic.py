#-------------------------------------------------------------------------------
# Name:        Check the effect of geodesic buffers
# Purpose:
#
# Author:      yichuans
#
# Created:     09/05/2014
# Copyright:   (c) yichuans 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os, sys, arcpy, random, numpy
import multiprocessing, logging
import Yichuan10

# debug
logger = multiprocessing.log_to_stderr()
logger.setLevel(multiprocessing.SUBDEBUG)


#
arcpy.env.overwriteOutput = True

# mollweide
sr_mwd = Yichuan10.createSpatialRefBySRID101(54009)
wdpa_shp = r"D:\Yichuan\WDPA\WDPA_Jan2014.gdb\WDPA_poly_Jan2014"
wdpa_shp = r"D:\Yichuan\WDPA\WDPA_poly_Jan2014.shp"

# debug
# wdpa_shp = r"D:\Yichuan\WHS_dump_SHP\whs_dump_131008.shp"
# get full list ids
wdpaidlist = Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(wdpa_shp, 'wdpaid')

def compare_geoms(wdpaid, origin_shp_poly, proj_shp, geodesic_shp):
    # compare geom when all shapes are ready, given id
    # comparison done in mwd
    # output: wdpaid, area_origin, area_proj, area_geodesic, dif_proj, dif_geodesic

    geom_origin = Yichuan10.GetFieldValueByID_mk2(origin_shp_poly, wdpaid)
    geom_proj = Yichuan10.GetFieldValueByID_mk2(proj_shp, wdpaid)
    geom_geodesic = Yichuan10.GetFieldValueByID_mk2(geodesic_shp, wdpaid)

    geom_origin = geom_origin.projectAs(sr_mwd)
    geom_geodesic = geom_geodesic.projectAs(sr_mwd)


    # area
    area_origin = geom_origin.getArea('PLANAR')/1000000
    # save computation
##    area_proj = geom_proj.getArea('PLANAR')/1000000
##    area_geodesic = geom_geodesic.getArea('PLANAR')/1000000

    # dif
    dif_proj_geom = geom_origin.intersect(geom_proj, 4)
    dif_geodesic_geom = geom_origin.intersect(geom_geodesic, 4)

    area_dif_proj = area_origin - (dif_proj_geom.getArea('PLANAR')/1000000)
    area_dif_geodesic = area_origin - (dif_geodesic_geom.getArea('PLANAR')/1000000)

    # return [wdpaid, area_origin, area_proj, area_geodesic, area_dif_proj, area_dif_geodesic]
    return [wdpaid, area_origin, area_dif_proj, area_dif_geodesic]

##def ordinary_buffer(origin_shp, proj_shp, sr_object):
##    # create buffer from centroid and buffer under the given SR
##    # output shp
##    pass

def my_buffer(origin_shp_pt, output_shp):
    # create buffer from centroid and buffer geodesically
    # settings: if origin_shp_pt is geographic -> geodesic
    # if origin_shp_pt is projected -> ordinary

    # create expression
    arcpy.AddField_management(origin_shp_pt, 'radius_f', 'TEXT')
    exp = 'str(math.sqrt(!gis_area!/math.pi)) + " kilometers"'

    arcpy.CalculateField_management(origin_shp_pt, 'radius_f', exp, 'PYTHON_9.3')
    arcpy.Buffer_analysis(origin_shp_pt, output_shp, 'radius_f')

    print "Buffering complete"


def subset_wdpa_random(wdpa_shp, outpath, outname, random_number=1000):
    # create a subset of wdpa, given the random_number (used to choose wdpaids)
    # output put

    subset_ids = random.sample(wdpaidlist, random_number)
    print "Random sampling size:", random_number

    origin_shp_poly = outpath + os.sep + outname

    exp = '(' + ','.join([str(x) for x in subset_ids]) + ')'
    exp = '\"wdpaid\" in ' + exp

    arcpy.FeatureClassToFeatureClass_conversion(wdpa_shp, outpath, outname, exp)
    print "Subsetting WDPA complete"

    return origin_shp_poly


def convert_centroid(origin_shp_poly, origin_shp_pt):
    # convert to points
    # 'INSDIE'
    arcpy.FeatureToPoint_management(origin_shp_poly, origin_shp_pt, 'CENTROID')
    print "Converting to centroids (inside) complete"

def project_centroid(origin_shp_pt, origin_prj_shp, sr_object = sr_mwd):
    # project
    arcpy.Project_management(origin_shp_pt, origin_prj_shp, sr_object)
    print "Projecting centroid complete"


def run_experiment(workspace, random_number=25000):

    #random_number=1000
    # input data

    # create workspace if not exist
    if not os.path.exists(workspace):
        os.mkdir(workspace)

    # set workspace
    arcpy.env.workspace = workspace

    # output origin_shp
    origin_shp_pt = 'origin_shp_pt.shp'
    origin_shp_poly = 'origin_shp_poly.shp'

    # projected centroid
    origin_prj_shp = 'proj_pt.shp'

    # output shape
    geodesic_shp = 'buffer_geodesic.shp'
    proj_shp = 'buffer_proj_mwd.shp'

    # create original_shape after random subsetting
    subset_wdpa_random(wdpa_shp, arcpy.env.workspace, origin_shp_poly, random_number)

    # create centroid
    convert_centroid(origin_shp_poly, origin_shp_pt)

    # geodesic buffer
    my_buffer(origin_shp_pt, geodesic_shp)

    # buffer in mwd
    project_centroid(origin_shp_pt, origin_prj_shp, sr_mwd)
    my_buffer(origin_prj_shp, proj_shp)

    # header
    outputfile = arcpy.env.workspace + os.sep + 'output.csv'
    # header = "wdpaid, area_origin, area_proj, area_geodesic, area_dif_proj, area_dif_geodesic\n"
    header = "wdpaid, area_origin, area_dif_proj, area_dif_geodesic\n"

    f = open(outputfile, 'w')
    f.write(header)
    f.close()

    # content

    # for each id
    wdpaidlist = Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(origin_shp_poly, 'wdpaid')
    for wdpaid in wdpaidlist:
        try:
            result = compare_geoms(wdpaid, origin_shp_poly, proj_shp, geodesic_shp)
            f = open(outputfile, 'a')
            line = ','.join([str(each) for each in result]) + '\n'
            f.write(line)
            f.close()
            print "Comparing wdpaid:", wdpaid, " complete"
        except:
            print "ERROR: failed wdpid:", wdpaid



if __name__ == '__main__':

##    pool = multiprocessing.Pool(processes=3)
##    outputs = [r"D:\Yichuan\Amy\Geodesic_buffer" + os.sep + str(each) for each in range(1,10)]
##    pool.map(run_experiment, outputs)
##    pool.close()


##    run_experiment(r"D:\Yichuan\Amy\Geodesic_buffer\single", random_number=500, wdpa_shp = wdpa_shp)
    # pool failed
##    pool = multiprocessing.Pool(4)
##    outputs = [r"D:\Yichuan\Amy\Geodesic_buffer" + os.sep + str(each) for each in range(1,11)]
##    tests = pool.map(run_experiment, outputs, 1)
##    print tests
##    pool.close()
##    pool.join()

    # serial slow
##    outputs = [r"D:\Yichuan\Amy\Geodesic_buffer" + os.sep + str(each) for each in range(1,11)]
##    for output in outputs:
##        run_experiment(output)

    # from basic: failed
##    jobs = []
##    for i in range(21, 200):
##        output = r"D:\Yichuan\Amy\Geodesic_buffer" + os.sep + str(i)
##        p = multiprocessing.Process(target=run_experiment, args=(output,))
##        jobs.append(p)
##        p.start()

    pool = multiprocessing.Pool(2)
    outputs = [r"D:\Yichuan\Amy\Geodesic_buffer" + os.sep + str(each) for each in range(60,70)]
    tests = pool.map_async(run_experiment, outputs)

    pool.close()
    pool.join()

##
