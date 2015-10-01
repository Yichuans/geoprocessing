#-------------------------------------------------------------------------------
# Name:        Comparative analysis
# Purpose:     For the annual comparative analysis of WH nominations
#              Consolidated version 2014
#
# Author:      Yichuans
#
# Created:     28/08/2014
# Copyright:   (c) Yichuans 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------

# import relevant functions
import Yichuan10
from YichuanDB import ConnectionParameter, get_sql_result, get_sql_result_as_list, process_sql

class PgIntersectView:
    # input all PgTable instances
    # Distinct clause need to be implemented in the database
    def __init__(self, conn_arg, base, theme, output):
        # assert not base.areFieldsNone() and not theme.areFieldsNone() and \
        #        hasattr(base, 'geom') and hasattr(theme, 'geom')
        self.conn = conn_arg.getConn()
        self.cur = self.conn.cursor()
        self.base = base
        self.theme = theme
        self.output = output
        self.sql = None

    def _gen_seg_sql(self):
        # with statement for intersect
        seg = """segkm2 AS (
        SELECT %s as tid, %s as bid, sum(area_mwd_km2(st_intersection(%s, %s))) as areakm2
        FROM %s, %s
        WHERE st_intersects(%s, %s)
        GROUP BY %s, %s)"""%(self.theme.t_id, self.base.t_id,
                             self.theme.t_geom, self.base.t_geom,
                             self.theme.s_name, self.base.s_name,
                             self.theme.t_geom, self.base.t_geom,
                             self.theme.t_id, self.base.t_id)
        return seg

    def _gen_theme_group_sql(self):
        # with statement for theme
        theme_group = """themekm2 AS (
        SELECT %s as tid, sum(area_mwd_km2(%s)) as areakm2
        FROM %s
        GROUP BY %s)"""%(self.theme.t_id,
                         self.theme.t_geom,
                         self.theme.s_name,
                         self.theme.t_id)
        return theme_group

    def _gen_base_group_sql(self):
        # with statement for base
        base_group = """basekm2 AS (
        SELECT %s as bid, sum(area_mwd_km2(%s)) as areakm2
        FROM %s
        GROUP BY %s)"""%(self.base.t_id,
                         self.base.t_geom,
                         self.base.s_name,
                         self.base.t_id)
        return base_group

    def _gen_header_sql_as_table(self):
        header = "CREATE TABLE %s AS"%(self.output.s_name)
        return header

    def _gen_header_sql(self):
        header = "CREATE OR REPLACE VIEW %s AS"%(self.output.s_name,)
        return header


    def _gen_intersect_sql_simple(self):
        # a simple yes or no approach
        header = self._gen_header_sql()
        select = """SELECT DISTINCT %s as tid, %s as bid
        FROM %s, %s
        WHERE st_intersects(%s, %s)
        """%(self.theme.t_id, self.base.t_id,
             self.theme.s_name, self.base.s_name,
             self.theme.t_geom, self.base.t_geom)
        sql = header + '\n' + select
        return sql


    def _gen_intersect_sql(self):
        # put all segments together
        # with statement
        header = self._gen_header_sql()
        seg = self._gen_seg_sql()
        theme_group = self._gen_theme_group_sql()
        base_group = self._gen_base_group_sql()

        # apply a smaller threshold 0.05 to avoid apparent intersections
        select = """SELECT segkm2.tid, segkm2.bid,
        segkm2.areakm2 as seg_area,
        themekm2.areakm2 as theme_area,
        basekm2.areakm2 as base_area
        FROM segkm2, themekm2, basekm2
        WHERE segkm2.tid = themekm2.tid AND segkm2.bid = basekm2.bid AND
        segkm2.areakm2/themekm2.areakm2 > 0.05

        ORDER BY segkm2.tid"""
        sql = header + '\n' + 'WITH ' + seg + ',\n' +\
              theme_group + ',\n' +\
              base_group + '\n' +\
              select
        return sql

    def _gen_intersect_sql_simple_as_table(self):
        # same as simple but creates a table
        header = self._gen_header_sql_as_table()
        select = """SELECT DISTINCT %s as tid, %s as bid
        FROM %s, %s
        WHERE st_intersects(%s, %s)
        """%(self.theme.t_id, self.base.t_id,
             self.theme.s_name, self.base.s_name,
             self.theme.t_geom, self.base.t_geom)
        sql = header + '\n' + select
        return sql


    def _gen_intersect_sql_as_table(self):
        # same as sql but make table
        # put all segments together
        # with statement
        header = self._gen_header_sql_as_table()
        seg = self._gen_seg_sql()
        theme_group = self._gen_theme_group_sql()
        base_group = self._gen_base_group_sql()
        select = """SELECT segkm2.tid, segkm2.bid,
        segkm2.areakm2 as seg_area,
        themekm2.areakm2 as theme_area,
        basekm2.areakm2 as base_area
        FROM segkm2, themekm2, basekm2
        WHERE segkm2.tid = themekm2.tid AND segkm2.bid = basekm2.bid AND
        segkm2.areakm2/themekm2.areakm2 > 0.05
        ORDER BY segkm2.tid"""
        sql = header + '\n' + 'WITH ' + seg + ',\n' +\
              theme_group + ',\n' +\
              base_group + '\n' +\
              select
        return sql

    def run(self, flag='view', simple=False):
        # decide if a table instead of a view is needed
        # a second condition has been added for simple intersection
        if not simple:
            if flag == 'view':
                self.sql = self._gen_intersect_sql()
            elif flag == 'table':
                self.sql = self._gen_intersect_sql_as_table()
        else:
            if flag == 'view':
                self.sql = self._gen_intersect_sql_simple()
            elif flag == 'table':
                self.sql = self._gen_intersect_sql_simple_as_table()

        assert self.sql != None
        print self.cur.mogrify(self.sql)
        self.cur.execute(self.sql)
        self.conn.commit()
        self.cur.close()
        self.conn.close()




class PgGroupView:
    def __init__(self, conn_arg, intersect, output):
        """intersect <pgTable>, output <pgTable>, group by {'base'|'theme'} """

        assert not output.areFieldsNone()

        self.conn = conn_arg.getConn()
        self.cur = self.conn.cursor()

        self.intersect = intersect
        self.output = output
##        self.flag = flag

    def _gen_base_stats(self):
        # group segments by base
        stats = """WITH stats AS (
        SELECT %s, count(seg_area) as count_seg,
        sum(seg_area) as sum_seg
        FROM %s
        GROUP BY %s)"""%(self.intersect.bid, self.intersect.s_name, self.intersect.bid)
        return stats

    def _gen_theme_stats(self):
        # group segments by theme
        stats = """WITH stats AS (
        SELECT %s, count(seg_area) as count_seg,
        sum(seg_area) as sum_seg
        FROM %s
        GROUP BY %s)"""%(self.intersect.tid, self.intersect.s_name, self.intersect.tid)
        return stats

    def _gen_theme_sql(self):
        header = "CREATE OR REPLACE VIEW %s AS"%(self.output.s_name)
        select = """
        SELECT %s,
        stats.count_seg,
        stats.sum_seg,
        base.theme_area,
        stats.sum_seg/base.theme_area as per_seg_theme
        FROM (SELECT DISTINCT ON (%s) * FROM %s) as base LEFT JOIN stats USING (%s)
        """%(self.intersect.tid,
             self.intersect.tid,
             self.intersect.s_name,
             self.intersect.tid)
        sql = header + '\n' + self._gen_theme_stats() + '\n' + select
        return sql

    def _gen_base_sql_simple(self):
        header = "CREATE OR REPLACE VIEW %s AS" %(self.output.s_name)
        select = """
        SELECT %s::varchar(255), count(distinct %s) as count_seg
        FROM %s
        GROUP BY %s"""%(self.intersect.bid, self.intersect.tid, self.intersect.s_name, self.intersect.bid)
        sql = header + '\n' +select
        return sql

    def _gen_base_sql(self):
        # the base id in some c
        header = "CREATE OR REPLACE VIEW %s AS" %(self.output.s_name)
        select = """
        SELECT %s::varchar(255),
        stats.count_seg,
        stats.sum_seg,
        base.base_area,
        stats.count_seg/base.base_area as per_count_base,
        1000000.0* (stats.count_seg/base.base_area) as per_count_base_m,
        stats.sum_seg/base.base_area as per_seg_base,
        stats.count_seg/sum(stats.count_seg) OVER () as per_self_count,
        stats.sum_seg/sum(stats.sum_seg) OVER () as per_self_seg
        FROM (SELECT DISTINCT ON (%s) * FROM %s) as base LEFT JOIN stats USING (%s)
        """%(self.intersect.bid,
             self.intersect.bid,
             self.intersect.s_name,
             self.intersect.bid)
        sql = header + '\n' + self._gen_base_stats() + '\n' + select
        return sql

    def _run_base(self):
        sql = self._gen_base_sql()
        print self.cur.mogrify(sql)
        self.cur.execute(sql)
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def _run_base_simple(self):
        sql = self._gen_base_sql_simple()
        print self.cur.mogrify(sql)
        self.cur.execute(sql)
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def _run_theme(self):
        sql = self._gen_theme_sql()
        print self.cur.mogrify(sql)
        self.cur.execute(sql)
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def run(self, flag = 'base', simple = False):
        # create the group view
        if not simple:
            if flag == 'base':
                self._run_base()
            else:
                self._run_theme()
        else:
            self._run_base_simple()



class PgTable:
    # this pgTable instance holds all field names (including qualified names)
    # if the specifc table is found, all other attributes will be filled
    # else the last optional list will be used, for creating new pgTable
    def areFieldsNone(self):
        return self.schema == None and \
               self.name == None and \
               self.id == None

    def __init__(self, conn_arg, schema = None, name = None, id = None, otherfields =[]):
        """schema, name, id field (for output view, id could be anything), otherfields"""
        # check if conn object is already present and no closed

        conn = conn_arg.getConn()

        self.schema = schema
        self.name = name
        self.id = id

        if not self.areFieldsNone():
            # qualified ID: table - id
            # qualified table name: schema - table
            self.t_id = self.name + '.' + self.id
            self.s_name = self.schema + '.' + self.name


        # get other fields
        def _getAllFields(self):
            cur = conn.cursor()
            sql = """select column_name from
            information_schema.columns where
            table_schema = '%s' and table_name = '%s'"""%(self.schema, self.name)
            cur.execute(sql)
            fields_raw = cur.fetchall()
            cur.close()
            return [fieldtuple[0] for fieldtuple in fields_raw]

        # add all other fields
        if not otherfields:
            # get existing table
            populated_fields = _getAllFields(self)
        else:
            # new table
            populated_fields = otherfields

        for eachfield in populated_fields:
            if eachfield not in ('name', 'schema', 'id'):
                setattr(self, eachfield, eachfield)
                setattr(self, 't_' + eachfield, self.name + '.'+ eachfield)

        conn.close()


def post_intersection_mk2(conn_arg, intersect, output, nomi_id):
    """ this function first filters the intersect table by its wdpaid """

    conn = conn_arg.getConn()

    whereclause = 'tid = ' + str(nomi_id)

    header = "CREATE OR REPLACE VIEW %s AS "%(output.s_name)

    body = """
    SELECT * FROM %s
    WHERE (bid in (SELECT bid FROM %s WHERE %s))
    ORDER BY bid"""%(intersect.s_name,
                     intersect.s_name,
                     whereclause)

    sql = header + body

    process_sql(sql, conn)

    conn.close()

def get_ca_conn_arg(version=2014):
    """
    Get the default conn for comparative analysis
    """
    if version == 2014:
        conn_param = ConnectionParameter('localhost', 'whs', 5432, 'postgres', 'gisintern')

    elif version == 2015:
        conn_param = ConnectionParameter('localhost', 'whs_v2', 5432, 'postgres', 'gisintern')

    else:
        print('version number wrong')
        return None

    return conn_param



def create_combined_wh_nomination_view(input_nomination, WH_SHAPE, output_schema, conn):
    # not table always be schema + table
    sql_create = """
        CREATE OR REPLACE VIEW %s AS
        SELECT wdpaid, en_name, shape as geom
        FROM %s
        UNION
        SELECT wdpaid, name, shape as geom
        FROM %s
    """%(output_schema + '.' + COMBINED_WH_NOMINATION_VIEW, WH_SHAPE, input_nomination)

    process_sql(sql_create, conn)



def run_ca_for_a_theme(input_nomination, output_schema, themekey, conn_arg=get_ca_conn_arg()):
    """
    For each theme and all nominations (combined with WH sites)

    """
    with conn_arg.getConn() as conn:
        # get nomi_wdpaid
        sql = "SELECT wdpaid FROM %s"%(input_nomination,)
        nomi_wdpaid = get_sql_result_as_list(sql, conn)

        # get unique
        nomi_wdpaid = list(set(nomi_wdpaid))

    # test existing of combined view: WH and nomination

        sql_check = "SELECT * FROM information_schema.tables WHERE table_schema= '%s' AND table_name='%s'"%(output_schema, COMBINED_WH_NOMINATION_VIEW)
        if len(get_sql_result(sql_check, conn)) == 0: # the combine view doesn't exist
            create_combined_wh_nomination_view(input_nomination, WH_SHAPE, output_schema, conn)
        else:
            pass

    # get combined view
    catTab = PgTable(conn_arg, output_schema, COMBINED_WH_NOMINATION_VIEW, 'wdpaid')
    # get id
    themename, fieldname = BASE_LOOKUP[themekey]

    # ref baseTab
    baseTab = PgTable(conn_arg, BASEL_TABLE_SCHEMA, themename, fieldname)

    # intersection tab
    intersect_table_name = get_intersectname(themekey)

    intersectTab = PgTable(conn_arg, output_schema, intersect_table_name, 'id')

    # intersection view, the core intersection finishes. This is also useful for debugging, as
    # no duplicate intersection is needed
    pi = PgIntersectView(conn_arg, baseTab, catTab, intersectTab)
    try:
        #pi.run(flag='view') # for debugging
        pi.run(flag='table') # for running
    except Exception as e:
        print e
        print 'error creating tables, skip and continue'
        return 0

     # for each table in the base tab intersection view will have filtered results
    for nomi_id in nomi_wdpaid:
        # need to replace ',' in country iso3
        output_name = get_filtername(nomi_id, intersect_table_name)

        # output tab
        outTab = PgTable(conn_arg, output_schema, output_name, 'id')

        # filter and append wh attributes
        post_intersection_mk2(conn_arg, intersectTab, outTab, nomi_id)

def run_ca_tentative(conn_arg=get_ca_conn_arg()):
    """
    create tentative list simple comparartive stats against bases
    """


    for themekey in BASE_LOOKUP.keys():

        themename, fieldname = BASE_LOOKUP[themekey]
        intersect_table_name = get_intersectname(themekey)

        with conn_arg.getConn() as conn:
            sql= """
            CREATE OR REPLACE VIEW %s AS
            SELECT id as tid, base.%s as bid
            FROM %s, %s as base
            WHERE st_intersects(shape, geom)

            """%(TLS_SCHEMA + '.' + intersect_table_name, fieldname, TLS_SHAPE, BASEL_TABLE_SCHEMA + '.' + themename)

            process_sql(sql, conn)


def get_intersectname(themekey):
    """
    returns string "intersect_'themename'_'fieldname'"
    schema name needs to be add if qualified name is desirable

    """
    themename, fieldname = BASE_LOOKUP[themekey]
    return 'intersect_' + themename + '_' + fieldname

def get_filtername(nomi_id, intersect_table_name):
    """
    return string based on the intersect_table_name and nomi_id for filters

    """

    return 'filter_' + intersect_table_name + '_' + str(nomi_id)



### EXPORT TO EXCEL===========================================


# CONSTANTS
BASE_LOOKUP = {'eba': ['eba','ebaname'],
          'feow': ['feow', 'ecoregion'],
          'g200_terr': ['g200_terr', 'g200_regio'],
          'g200_marine': ['g200_marine', 'g200_regio'],
          'g200_fw': ['g200_fw', 'g200_regio'],
          'meow_200m_p': ['meow_200m', 'province'],
          'meow_200m_e': ['meow_200m', 'ecoregion'],
          'cpd': ['cpd', 'site_ref'],
          'udv_dcw_p': ['udv_dcw', 'provname'],
          'udv_dcw_b': ['udv_dcw', 'biome'],
          'udv_dcw_r': ['udv_dcw', 'realm'],
          'wwf_e': ['wwf_terr_ecos_clean', 'eco_name'],
          'wwf_b': ['wwf_terr_ecos_clean', 'biome'],
          'wwf_r': ['wwf_terr_ecos_clean', 'realm'],
          'wwf_rb': ['wwf_terr_ecos_clean', 'realm_biome'],
          'hs': ['hotspot_type_core', 'fname'],
          'ws': ['wilderness_type_core', 'wa_name'],
          'aze': ['aze', 'aze_id'],
          'iba': ['iba', 'sitrecid'],
          'kba': ['kba', 'sitrecid']}

# output schema and combined view name
COMBINED_WH_NOMINATION_VIEW = 'z_combined_wh_nomination_view'

# base tables schema
BASEL_TABLE_SCHEMA = 'pl'

# tentative list schema
TLS_SCHEMA = 'ca_tls'

# full qualified names, must have wdpaid and shape
WH_SHAPE = 'arcgis.v_wh_spatial'

# combined pt + pl
TLS_SHAPE = 'tls.tentative'


# Run ca 2014
def ca_2014():
    # output-schema needs to be created ahead
    # input_nomination has the standard wdpa schema
    input_nomination = 'ca_nomi.nomi_2014'
    output_schema = 'ca_2014'
    for themekey in BASE_LOOKUP.keys():
        run_ca_for_a_theme(input_nomination, output_schema, themekey, conn_arg=get_ca_conn_arg())

def ca_2014_tls():
    # tls comparartive analysis doesn't change
    # needs to update PL and TLS if updates are required
    run_ca_tentative()


# run ca 2015
def ca_2015():
    input_nomination = 'ca_nomi.nomi_2015'
    output_schema = 'ca_2015'
    for themekey in BASE_LOOKUP.keys():
        run_ca_for_a_theme(input_nomination, output_schema, themekey, conn_arg=get_ca_conn_arg(2015))

# run ca 2015
def ca_2015_add():
    input_nomination = 'ca_nomi.nomi_2015_add'
    output_schema = 'ca_2015_add'
    for themekey in BASE_LOOKUP.keys():
        run_ca_for_a_theme(input_nomination, output_schema, themekey, conn_arg=get_ca_conn_arg(2015))

def ca_2015_tls():
    # tls comparartive analysis doesn't change
    # needs to update PL and TLS if updates are required
    run_ca_tentative(get_ca_conn_arg(2015))

# clean ca if needed

def clean_view(schema_to_clean, conn_arg=get_ca_conn_arg()):
    """
    this function is used to clear ALL views in the given schema

    """
    sql = """
    SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'
    """%(schema_to_clean, )

    with conn_arg.getConn() as conn:
        views = get_sql_result_as_list(sql, conn)
        for view in views:
            sql = """
            DROP VIEW IF EXISTS %s CASCADE
            """%(schema_to_clean + '.' + view,)
            process_sql(sql, conn)


def _test():
    input_nomination = 'ca_nomi.kkfc'
    output_schema = 'ca_2014'
    themename = 'udv_dcw_r'
    run_ca_for_a_theme(input_nomination, output_schema, themename, conn_arg=get_ca_conn_arg())
