import sys
import numpy as np
import codecs
import copy
from collections import deque
import arcpy

def geom_description(geom):
    if not isinstance(geom, arcpy.Geometry):
        raise Exception('arcpy Geometry object expected')
    
    try:
        return 'area:{0:.2f}km2; point_count:{1}; part_count:{2}; centroid(xy):{3:.6f}-{4:.6f}'.format(geom.getArea(units='SQUAREKILOMETERS'),
        geom.pointCount,
        geom.partCount,
        geom.centroid.X,
        geom.centroid.Y)

    except:
        return 'Error in retrieving geometry information'

def get_row_info(wdpa_pid, data, fields_list):
    """"
    depending on the datatype of pid, generate correct SQL
    <wdpa_pid>, <fc/fl>, <list of fields>"""
    where_name = (isinstance(wdpa_pid, int) or isinstance(wdpa_pid, float)) and str(wdpa_pid) or ('\'' + str(wdpa_pid) + '\'')
    
    where_clause = "wdpa_pid = " + where_name

    with arcpy.da.SearchCursor(data, fields_list, where_clause=where_clause) as cur:
        for each in cur:
            # if found return the first value
            return each
    # if not found return None
    return None

def create_layer(data, out_layer, *arg, **karg):
    # not functional, but for the side effect
    if arcpy.Exists(out_layer):
        arcpy.Delete_management(out_layer)
    arcpy.MakeFeatureLayer_management(data, out_layer, *arg, **karg)

def main(old_data_path, new_data_path, out_file):
    """
    <FC1 path>, <FC2 path>, <result file>
    """
    # create layers
    old_data = 'old'
    new_data = 'new'

    # create layers for two monthly releases
    create_layer(old_data_path, old_data)
    create_layer(new_data_path, new_data)

    # ====== START =======
    q = deque()

    # order: id, name, iso3
    BASE = ['wdpa_pid', 'name', 'parent_iso3']
    HEADER = BASE + ['field', 'old_value', 'new_value']
    ATTR_FIELDS_TO_CHECK = ['name', 'orig_name', 'desig', 'desig_eng', 'desig_type', 'iucn_cat', 'no_take', 'status', 'status_yr']
    FIELDS_LIST = BASE + ATTR_FIELDS_TO_CHECK + ['SHAPE@']

    SKIP_BASE_LEN = len(BASE)

    # old wdpa_pid list, for reverse comparison
    o_idlist = list()

    # HEADER
    q.append(HEADER)


    # 0. check comparable fields 
    print 'Checking fields...'
    arcpy.AddMessage('Checking fields...')

    for field in BASE + ATTR_FIELDS_TO_CHECK:
        o_fields = arcpy.ListFields(old_data, field)
        n_fields = arcpy.ListFields(new_data, field)

        if o_fields and n_fields:
            if o_fields[0].type == n_fields[0].type:
                pass
            else:
                raise Exception('Inconsistent field types for [{}]: {} and {}'.format(field, o_fields[0].type, n_fields[0].type))

        else:
            raise Exception('Field missing in input: [{}]'.format(field))

    # 1. start from old: DELETION and CHANGE
    with arcpy.da.SearchCursor(old_data, FIELDS_LIST) as cur:
        # for each old row, find and compare new row
        for row_count, o_row in enumerate(cur):

            # tracking progress
            if row_count % 100 == 0:
                print 'Scanned rows:', row_count
                arcpy.AddMessage('Scanned rows: {}'.format(row_count))

            wdpa_pid = o_row[0]
            name = o_row[1]
            iso3 = o_row[2]
            n_row = get_row_info(wdpa_pid, new_data, fields_list=FIELDS_LIST)
            
            # debug
            if DEBUG:
                raise Exception('debug')

            # if no new row is found
            if not n_row:
                row = [wdpa_pid, name, iso3, 'DELETED', '', '']
                q.append(row)
            
            # if corresponding row found
            else:
                o_idlist.append(wdpa_pid)
                
                # Non spatial comparison, skip last element
                for i, field_name in enumerate(ATTR_FIELDS_TO_CHECK):
                    if n_row[i+SKIP_BASE_LEN] != o_row[i+SKIP_BASE_LEN]:
                        row = [wdpa_pid, name, iso3, field_name, o_row[i+SKIP_BASE_LEN], n_row[i+SKIP_BASE_LEN]]
                        q.append(row)

                # spatial comparison
                if n_row[-1] != o_row[-1]:
                    row = [wdpa_pid, name, iso3, 'Geometry', geom_description(o_row[-1]), geom_description(n_row[-1])]
                    q.append(row)


    # 2. start from new: ADD
    n_idlist = list()

    # new wdpa_pid list
    with arcpy.da.SearchCursor(new_data, BASE) as cur:
        for row in cur:
            n_idlist.append(row[0])

    n_id = np.array(n_idlist)
    o_id = np.array(o_idlist)

    np.setdiff1d(o_id, n_id)

    a_id = list(np.setdiff1d(n_id, o_id))

    for wdpa_pid in a_id:
        n_row = get_row_info(wdpa_pid, new_data, BASE)

        wdpa_pid = n_row[0]
        name = n_row[1]
        iso3 = n_row[2]
        
        row = [wdpa_pid, name, iso3, 'ADDED', '', '']
        q.append(row)

    # debug: deep copy to avoid overwriting
    # qq = copy.deepcopy(q)

    # 3. write to output
    with codecs.open(out_file, mode='w', encoding='utf-8') as f:
        while q:
            row = q.popleft()
            # convert int and float to str, noting unicode
            # add '"' for string and unicode
            # and-or trick: C equivalent boolean? x, y
            f.write(','.join(map(lambda x: (isinstance(x, int) or isinstance(x, float)) and str(x) or "\"" + x + "\"", row)) + '\n')


#### RUN ####
DEBUG = False

old_data_path = sys.argv[1]
new_data_path = sys.argv[2]
out_file = sys.argv[3]

main(old_data_path, new_data_path, out_file)