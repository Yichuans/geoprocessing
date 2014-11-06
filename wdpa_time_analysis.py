#-------------------------------------------------------------------------------
# Name:        wdpa time
# Purpose:
#
# Author:      Yichuans
#
# Created:     29/08/2014
# Copyright:   (c) Yichuans 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import YichuanDB

# compiling lists of tables
pntlist = ['pnt_'+str(i) for i in range(1998, 2015)]
polylist = ['poly_' + str(i) for i in range(1998, 2015)]
for value in ['pnt_' + str(i) for i in (1999, 2001, 2008)]:
    pntlist.remove(value)

for value in ['poly_' + str(i) for i in (1999, 2001, 2008)]:
    polylist.remove(value)


conn_param = YichuanDB.ConnectionParameter(db='whs', port=5432, user='wdpa_time', password='wdpa_time')
conn = conn_param.getConn()

def compile_fields(fromtable, excludefield = 'shape'):
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '%s' and column_name != '%s'
    """%(fromtable, excludefield)
    result = YichuanDB.get_sql_result(sql,conn)

    # change order of fields
    result.reverse()

    return ','.join([each[0] for each in result])


def create_non_spatial_view(fromtable):
    viewname = 'v_' + fromtable

    # concat field names
    concat_fields = compile_fields(fromtable)

    sql ="""
    CREATE OR REPLACE VIEW %s AS
    SELECT %s
    FROM %s
    """%(viewname, concat_fields, fromtable)

    YichuanDB.process_sql(sql, conn)

def drop_non_spatial_view(fromtable):
    viewname = 'v_' + fromtable
    sql ="""
    DROP VIEW IF EXISTS %s
    """%(viewname,)

    YichuanDB.process_sql(sql, conn)


def batch_create_non_spatial_view():
    for value in pntlist:
        create_non_spatial_view(value)

    for value in polylist:
        create_non_spatial_view(value)


def main():
    pass

if __name__ == '__main__':
    main()



