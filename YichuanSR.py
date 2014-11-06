#-----------------------------------------
# Yichuan SHI
# Date: 31 October 2013
# Module on finding suitable coordinate systems
#
# ----------------------------------------

# Import system modules, create Geoprocessor object, load Arctoolbox
import os, sys, arcpy, re, numpy

from datetime import datetime

dt = datetime.now()
CurrentTime = str(dt.hour) + ':' + str(dt.minute) + ':' +str(dt.second) + ' ' + \
              str(dt.year)+'.'+str(dt.month)+'.' + str(dt.day)
ModifiedDate = 'ver: 31 October 2013'



# UTM srid and range list, a hack to get the range
UTM_RANGE_N = {i: [-180 + (i-32601)*6, -180 + (i-32601)*6 + 6, 'n'] for i in range(32601, 32661)}
UTM_RANGE_S = {i: [-180 + (i-32701)*6, -180 + (i-32701)*6 + 6, 's'] for i in range(32701, 32761)}

UTM_RANGE = UTM_RANGE_N.copy()
UTM_RANGE.update(UTM_RANGE_S)

# ------------------------------
def get_desirable_sr(shape):
    """<esri shape> in WGS84"""
    # this function returns a good sr - localised
    # mid-latitude: Lambert Conformal conic
    # equatorial:
    # decides what
    if isinstance(shape, arcpy.Geometry):
        extent = shape.extent
    elif isinstance(shape, arcpy.Extent):
        extent = shape
    else:
        print 'shape or extent expected'
        raise Exception
        
    xmin = extent.XMin
    xmax = extent.XMax
    ymin = extent.YMin
    ymax = extent.YMax

    sr = None

    # UTM coordinate system here
    if abs(abs(xmin) - abs(xmax)) <= 6:
        print 'UTM'
        if ymax <= 84 and ymin >= -80:
            # standard UTM number, if possible; input has to be small - large
            srid = get_srid([xmin, xmax], [ymin, ymax])
            sr = arcpy.SpatialReference(srid)

            # non standard UTM number custom
            if not srid:
                custom_meridian = snap_meridian([xmin, xmax])
                custom_ns = get_ns([ymin, ymax])
                sr = custom_utm(custom_meridian, custom_ns)


    # # Polar areas large scale - use other coordinate system
    #     elif ymax > 84 or ymin < -80:
    #         pass

    # # small scale here
    # if abs(abs(xmin) - abs(xmax)) > 6:
    #     pass

    else:
        print 'Others'
    # lazy solution: lambert azumethal projection customised
        #centroid = shape.centroid
        #lon = centroid.X
        #lat = centroid.Y
        
        lon = (xmin + xmax)/2.0
        lat = (ymin + ymax)/2.0

        # template using North Pole Azimuthal EA
        sr = arcpy.SpatialReference(102017)

        # modify params
        sr = modify_meridian(sr, lon)
        sr = modify_latitude_of_origin(sr, lat)

    print sr.name
    return sr

def modify_meridian(sr, meridian):
    # this function modify meridian

    wksr = sr.exportToString()

    # substitute
    custom_name = sr.name + '_custom_meridian'
    custom_name_re = 'PROJCS[\'' + custom_name + '\','
    wksr = re.sub('PROJCS\[.+?\'\,', custom_name_re, wksr)

    # substitute
    central_meridian = '[\'Central_Meridian\',' + str(meridian)+'],'
    wksr = re.sub('\[\'Central_Meridian\'\,.+?\]', central_meridian, wksr)

    sr.loadFromString(wksr)
    return sr

def modify_latitude_of_origin(sr, lat):
    # this function modify meridian

    wksr = sr.exportToString()
    # substitute
    custom_name = sr.name + '_custom_lat_origin'
    custom_name_re = 'PROJCS[\'' + custom_name + '\','
    wksr = re.sub('PROJCS\[.+?\'\,', custom_name_re, wksr)

    # substitute
    lat_of_origin = '[\'Latitude_Of_Origin\',' + str(lat)+'],'
    wksr = re.sub('\[\'Latitude_Of_Origin\'\,.+?\]', lat_of_origin, wksr)

    sr.loadFromString(wksr)
    return sr


def custom_utm(custom_meridian, custom_ns):
    # UTM north and south template N1 and S1
    n_srid_template = 32601
    s_srid_template = 32701

    if custom_ns == 'n':
        srid = n_srid_template
    else:
        srid = s_srid_template

    sr = arcpy.SpatialReference(srid)
    sr = modify_meridian(sr, custom_meridian)

    return sr

def snap_meridian(input_r_x):
    # return the custom meridian if input_r_x does not fall into any UTM zone
    # take care of 180/-180 issue
    if (input_r_x[1]) - (input_r_x[0]) >= 180:
        delta = ((input_r_x[0]) + (input_r_x[1]))/2.0
        # draw a circle to understand - mean_x should always be on the smaller arc
        if delta <= 0:
            mean_x = delta + 180
        else:
            mean_x = delta - 180

        del delta

    else:
        mean_x = (input_r_x[0] + input_r_x[1])/2.0


    # ceiling function, to determine which custom meridian should be used
    for utm_range in UTM_RANGE.values():
        if mean_x <= utm_range[1] and mean_x >= utm_range[0]:
            meridian = ((utm_range[0]) + (utm_range[1]))/2.0
            if mean_x <= meridian:
                snap_meridian = utm_range[0]
            else:
                snap_meridian = utm_range[1]
            break

    return snap_meridian


def get_ns(input_r_y):
    # return whether or not northern or southern hemisphere
    if input_r_y[0] >= 0:
        return 'n'
    elif input_r_y[1] < 0:
        return 's'
    else:
        mean_y = (input_r_y[0] + input_r_y[1])/2.0
        if mean_y >= 0:
            return 'n'
        else:
            return 's'



def get_srid(input_r_x, input_r_y):
    # return the srid of given input range of x and y
    # or None if range does not lie within standard utm zones
    for srid, utm_range in UTM_RANGE.items():
        # utm_range[2] checks north and south tag
        if test_in_range(input_r_x, utm_range) and get_ns(input_r_y) == utm_range[2]:
            return srid

    return None

def test_in_range(input_r_x, test_r_x):
    """<List: xmin, xmax>"""
    if input_r_x[0] >= test_r_x[0] and input_r_x[1] <= test_r_x[1]:
        return True
    else:
        return False


def set_sr_to_df(sr, mxd='CURRENT', dfname='*'):
    """set sr to the mxd, df by name"""
    # get mxd
    mxd = arcpy.mapping.MapDocument(mxd)

    # by default the first
    df = arcpy.mapping.ListDataFrames(mxd, dfname)[0]

    # set sr
    df.spatialReference = sr

    # refresh
    arcpy.RefreshActiveView()

if __name__ != "__main__":
    print "Spatial reference module imported %s, %s"%(CurrentTime,ModifiedDate)
else:
    pass
