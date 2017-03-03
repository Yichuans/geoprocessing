import arcpy
# check speed
import cProfile

fc = r"E:\Yichuan\Marine_D\JPN_cov_stats\Jan2017_second\JPN_JAN2017_second.gdb\WDPApoly_Jan2017_JPN"
where = "marine = '1'"


def has_index_on_pid(fc):
    indices = arcpy.ListIndexes(fc)

    print('Listing all indices:')

    for index in indices:
        print("Field       : {0}".format(index.fields[0].name))
        print("IsAscending : {0}".format(index.isAscending))
        print("IsUnique    : {0}".format(index.isUnique))

    if 'wdpa_pid'.upper() not in [index.fields[0].name.upper() for index in indices]:
        print('No index on wdpa_pid, please add an index first')
        return False

    return True

def find_self_intersection(fc, where):
    fl = 'feature_layer'
    fl_select = 'select_layer'
    FIELDS = ['wdpa_pid', 'name', 'rep_area', 'desig_eng', 'SHAPE@']
    result = []

    # make layer using sql
    arcpy.MakeFeatureLayer_management(fc, fl, where)
    arcpy.MakeFeatureLayer_management(fc, fl_select, where)

    # for each row
    with arcpy.da.SearchCursor(fl, FIELDS) as cur1:
        for i, row1 in enumerate(cur1):
            if i % 100 == 0:
                print('Features: {}'.format(i))

            # find those in the same fl_select that overlaps 
            arcpy.SelectLayerByLocation_management(fl_select, "INTERSECT", row1[-1])

            # using pid, any row but not self
            sql_not_self = "wdpa_pid <> '{}'".format(row1[0])
            with arcpy.da.SearchCursor(fl_select, FIELDS, sql_not_self) as cur2:
                for row2 in cur2:
                    try:
                        # polygon overlap
                        overlap_geom = row1[-1].intersect(row2[-1], 4)
                        overlap_area = overlap_geom.getArea('GEODESIC', 'SQUAREKILOMETERS')
                        output = list(row1[:-1]) + list(row2[:-1]) + [overlap_area]
                        result.append(output)
                    except:
                        print('Error intersection: {}, {}'.format(row1[0], row2[0]))

    # clean up
    arcpy.Delete_management(fl)
    arcpy.Delete_management(fl_select)

    return result

def dissolve_geometry_list(list_geoms):
    if len(list_geoms) == 0:
        raise Exception('Empty geom in the list')
    else:
        geom = list_geoms.pop()

        while list_geoms:
            another_geom = list_geoms.pop()
            geom = geom.union(another_geom)

        return geom

def find_self_intersection_dis(fc, where):
    # differ from the original version in that
    # 1. it does not calculate pair-wise intersection which complicate cases where >2 overlaps happen
    # 2. it iteratively 'union' geometries that overlaps with given row
    # 3. as a result, given a geom, it calculates the total overlap (size~overlap_area)
    fl = 'feature_layer'
    fl_select = 'select_layer'
    FIELDS = ['wdpa_pid', 'name', 'rep_area', 'desig_eng', 'SHAPE@']
    result = []
    

    # make layer using sql
    arcpy.MakeFeatureLayer_management(fc, fl, where)
    arcpy.MakeFeatureLayer_management(fc, fl_select, where)

    # for each row
    with arcpy.da.SearchCursor(fl, FIELDS) as cur1:
        for i, row1 in enumerate(cur1):
            if i % 100 == 0:
                print('Features: {}'.format(i))

            # holder for overlapping geoms
            list_geoms = []

            # find those in the same fl_select that overlaps 
            arcpy.SelectLayerByLocation_management(fl_select, "INTERSECT", row1[-1])

            # using pid, any row but not self
            sql_not_self = "wdpa_pid <> '{}'".format(row1[0])
            with arcpy.da.SearchCursor(fl_select, FIELDS, sql_not_self) as cur2:
                for row2 in cur2:
                    try:
                        # polygon overlap
                        overlap_geom = row1[-1].intersect(row2[-1], 4)
                        list_geoms.append(overlap_geom)
                     
                    except:
                        print('Error intersection: {}, {}'.format(row1[0], row2[0]))

                if list_geoms:
                    # dissolve all overlapping geoms
                    n_geoms = len(list_geoms)
                    dissolve_geom = dissolve_geometry_list(list_geoms)
                    overlap_area = dissolve_geom.getArea('GEODESIC', 'SQUAREKILOMETERS')
                    output = list(row1[:-1]) + [n_geoms, overlap_area]
                    result.append(output)
                else:
                    pass

    # clean up
    arcpy.Delete_management(fl)
    arcpy.Delete_management(fl_select)

    return result

def write_result(result, filepath):
    """<list>,<str>"""
    with open(filepath, 'w') as f:
        for line in result:
            f.write(','.join(map(lambda x: (isinstance(x, int) or isinstance(x, float)) and str(x) or "\"" + x + "\"", line)) + '\n')

# ===============TEST===================#
def test1():
    where = "marine = '1' and wdpaid in (555621413, 3246)"
    if has_index_on_pid(fc):
        b = find_self_intersection_dis(fc, where)
        # write_result(b, 'dis.csv')

def test2():
    where = "marine = '1' and wdpaid in (555621413, 3246, 3097,29811,12287)"
    if has_index_on_pid(fc):
        b = find_self_intersection_dis(fc, where)
        # write_result(b, 'dis.csv')


def test3():
    where = "marine = '1' and wdpaid in (555621413,3099,742,3097,29811,12287,3246,555575193,555575194,555575197,555575502,555575504,102048,17030,555575177,555571706,555621446,555621447,555621448,555621449,555621450,555621451,555621452,555621453,555621454,555621455,555621456,555621457,555621458,555621459,555621460,555621461,555621462,555621463,555621464,555621465,555621466,555621467,902482)"
    if has_index_on_pid(fc):
        b = find_self_intersection_dis(fc, where)
        # write_result(b, 'dis2.csv')

def check_test1():
    cProfile.run('test1()')

def check_test2():
    cProfile.run('test2()')
    # the longest running function call to be geometry.intersects 3.4s per call

# if __name__ == '__main__':
#     if has_index_on_pid(fc):
#         # a = find_self_intersection(fc, where)
#         b = find_self_intersection_dis(fc, where)
#         write_result(b, 'test_dis.csv')



