#-------------------------------------------------------------------------------
# Name:        Marine tag
# Purpose:
#
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
#
# Created:     2016/01/29
# Copyright:   (c) Yichuan Shi 2016

#-------------------------------------------------------------------------------
import os, sys, arcpy
from Yichuan10 import GetFieldValueByID_mk2

# constant
WDPA_SOURCE = r"E:\Yichuan\BrianM\test_pa_marine\wdpa.shp"
EEZ_SOURCE = r"E:\Yichuan\MyGDB.gdb\EEZ_v7"

def compute_marine_code(wdpaid, wdpa_source = WDPA_SOURCE, eez_source=EEZ_SOURCE):
    """worker process to calculate marine tag based on its spatial relationship with EEZ"""
    gis_area = get_gis_area(wdpaid, wdpa_source)
    land_area = get_land_area(wdpaid, wdpa_source, eez_source)
    marine_area = gis_area - land_area

    # get percentage
    if gis_area >= 0:
        per_land = land_area/gis_area
        per_marine = marine_area/gis_area
    else:
        print('Error: GIS_area = 0')
        return None

    # rules
    # land
    if per_land>0.98:
        result = 0

    # marine
    elif per_marine > 0.98:
        result = 2

    # coastal
    else:
        result = 1

    # DEBUG INFORMATION
    print('Debugging information below:')
    print('wdpaid: {}\ngis_area: {:.2f}\nland_area: {:.2f}, {:.2%}\nmarine_area: {:.2f}, {:.2%}\nresult: {}'.format(wdpaid, gis_area, land_area, per_land, marine_area, per_marine, result))

    return result

def get_land_area(wdpaid, wdpa_source, eez_source):
    if arcpy.Exists("pa"):
        arcpy.Delete_management("pa")

    if arcpy.Exists("eez_land"):
        arcpy.Delete_management("eez_land")

    # create a eez layer
    arcpy.MakeFeatureLayer_management(eez_source, 'eez_land', '"type"=\'Land\'')

    # create a pa layer
    arcpy.MakeFeatureLayer_management(wdpa_source, 'pa', '"wdpaid"=%s'%wdpaid)

    # intersect result
    geom_list = arcpy.Intersect_analysis(['eez_land', 'pa'], arcpy.Geometry())


    return sum([geom.getArea('GEODESIC', 'SQUAREKILOMETERS') for geom in geom_list])

def get_gis_area(wdpaid, wdpa_source):
    geom = GetFieldValueByID_mk2(wdpa_source, wdpaid)
    return geom.getArea('GEODESIC', 'SQUAREKILOMETERS')


def update_marine_code(wdpaid, marine_code, wdpa_source=WDPA_SOURCE):
    with arcpy.da.UpdateCursor(wdpa_source, ['MARINE'], '"wdpaid"=%s'%wdpaid) as cur:
        for row in cur:
            row[0] = marine_code
            cur.updateRow(row)

    return 0



# TEST SUITES
def test_wdpa(wdpaid=555538499):
    marine_code = compute_marine_code(wdpaid)
    # update_marine_code(wdpaid, marine_code)