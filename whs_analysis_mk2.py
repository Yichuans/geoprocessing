# Need to ensure no duplicate in the data first
# Decomposed and OOP version, based on whs_analysis
# More generic functions
# Suitable for Comparative analysis and Gap analysis
# 17 July 2012 Yichuan Shi
# terms: theme, e.g. WH layer; base, e.g. IBA, KBA...


import os, sys, psycopg2, time

def startWith(string, pattern):
    return string[0:len(pattern)] == pattern

class BatchView:
    pass

class ConnectionParameter:
    def __init__(self, host = 'localhost',
                 db = 'world_heritage_sites',
                 port = '5433',
                 user = 'postgres',
                 password = 'gisintern'):
        self.host = host
        self.db = db
        self.port = port
        self.user = user
        self.password = password

    def toCommandString(self):
        return PSQL + ' -h ' + self.host + ' -d ' + self.db + ' -p ' + self.port + ' -U ' + self.user

    def getConn(self):
        conn = psycopg2.connect(host = self.host,
                              database = self.db,
                              port = self.port,
                              user = self.user,
                              password = self.password)
        return conn

class PgIntersectView:
    # input all PgTable instances
    # Distinct clause need to be implemented in the database
    def __init__(self, conn_arg, base, theme, output):
        assert not base.areFieldsNone() and not theme.areFieldsNone() and \
               hasattr(base, 'geom') and hasattr(theme, 'geom')
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
        WHERE segkm2.tid = themekm2.tid AND segkm2.bid = basekm2.bid
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

##    def run_make_table(self):
##        self.sql = self._gen_intersect_sql_as_table()
##        assert self.sql != None
##        print self.cur.mogrify(self.sql)
##        self.cur.execute(self.sql)
##        self.conn.commit()
##        self.cur.close()
##        self.conn.close()


class PgGroupView:
    def __init__(self, conn_arg, intersect, output):
        """intersect <pgTable>, output <pgTable>, group by {'base'|'theme'} """
##        assert intersect.isTableInDB() and \
##               not output.areFieldsNone() and \
##               not output.isTableInDB()
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

    def isTableInDB(self):
        cur = conn.cursor()
        sql = """SELECT exists(select * from information_schema.tables where table_name = %s
        and table_schema = %s)"""
        cur.execute(sql, (self.name, self.schema))
        result = cur.fetchone()[0]
        cur.close()
        return result

    def __init__(self, schema = None, name = None, id = None, otherfields =[]):
        """schema, name, id field (for output view, id could be anything), otherfields"""
        # check if conn object is already present and no closed
        assert conn != None and not conn.closed
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

def create_table(conn_arg, schema, table_name, fields_list):
    # fieldslist in the form of pairs
    conn = conn_arg.getConn()
    cur = conn.cursor()

    field_sql = ', '.join([field_pair[0] + ' ' + field_pair[1] for field_pair in fields_list])

    sql = """
    CREATE TABLE %s (
    %s
    );"""%(schema + '.' + table, field_sql)

    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


def testIntersect_group():
    pass

def validname(name):
    # regular expression here
    invalid_char = ['-', '\'', '/', '\\', '.', ' ']
    name = str(name).strip().lower()
    # make valid name
    for name_char in name:
        if name_char in invalid_char:
            name = name.replace(name_char, '_')

    if len(name) > 10:
        name = name[:10] + str(len(name))
    return name

def getListFromDBTable(conn_arg, schema, table, field):
    conn = conn_arg.getConn()
    cur = conn.cursor()

    sql = """
    SELECT DISTINCT %s
    FROM %s"""%(field, schema + '.' + table)

    cur.execute(sql)

    db_list = list()

    for each in cur.fetchall():
        db_list.append(each[0])

    cur.close()
    conn.close()
    return db_list

def getDictFromDBTable(conn_arg, schema, table, keyfield, valuefield):
    # returns a python dictionary object key_field, value_field
    """
    returns a key_field, value_field dictionary
    """
    conn = conn_arg.getConn()
    cur = conn.cursor()

    # sql
    sql = """
    SELECT %s, %s
    FROM %s"""%(keyfield, valuefield, schema + '.' + table)

    cur.execute(sql)

    lookup_dict = dict()
    for each in cur.fetchall():
        if not lookup_dict.has_key(each[0]):
            lookup_dict[each[0]] = each[1]

    cur.close()
    conn.close()
    return lookup_dict

def getDictOfListFromDBTable(conn_arg, schema, table, keyfield, valuefield):
    # returns a python dictionary object key_field, value_field
    conn = conn_arg.getConn()
    cur = conn.cursor()

    # sql
    sql = """
    SELECT %s, %s
    FROM %s"""%(keyfield, valuefield, schema + '.' + table)

    cur.execute(sql)

    lookup_dict = dict()
    for each in cur.fetchall():
        if not lookup_dict.has_key(each[0]):
            lookup_dict[each[0]] = [each[1]]
        else:
            lookup_dict[each[0]].append(each[1])

    cur.close()
    conn.close()
    return lookup_dict

def filter_generic(conn_arg, table_to_filter, output, idlist, idfield):
    # create generic filter views
    assert table_to_filter.isTableInDB()
##    assert intersect.isTableInDB() and \
##               not output.areFieldsNone() and \
##               not output.isTableInDB()

    conn = conn_arg.getConn()
    cur = conn.cursor()

    # get something like tid in (1,2,3,4)
    whereclause = '%s in ('%(idfield,) + ','.join(map(str, idlist)) + ')'

    # sql
    sql = """CREATE OR REPLACE VIEW %s AS
    SELECT * FROM %s
    WHERE %s"""%(output.s_name,
                     table_to_filter.s_name,
                     whereclause)
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def filter(conn_arg, intersect, output, idlist):
    # create filter views
    assert intersect.isTableInDB()
##    assert intersect.isTableInDB() and \
##               not output.areFieldsNone() and \
##               not output.isTableInDB()

    conn = conn_arg.getConn()
    cur = conn.cursor()

    # get something like tid in (1,2,3,4)
    whereclause = 'tid in (' + ','.join(map(str, idlist)) + ')'

    # sql
    sql = """CREATE OR REPLACE VIEW %s AS
    SELECT * FROM %s
    WHERE (bid in (SELECT bid FROM %s WHERE %s))
    ORDER BY bid"""%(output.s_name,
                     intersect.s_name,
                     intersect.s_name,
                     whereclause,)
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def add_attributes(conn_arg, filter, output):
##
##    assert intersect.isTableInDB() and \
##               not output.areFieldsNone() and \
##               not output.isTableInDB()
    assert filter.isTableInDB()

    conn = conn_arg.getConn()
    cur = conn.cursor()

    # this function still needs to be further developed
    # 1) adding TIDs in cases where nominated sites share same base units
    # 2) lookup tables for base name


    # get site information from filter
    # a dirty trick to get id
    id = filter.name[-7:]
    nomi_name = getDictFromDBTable(conn_arg, 'ca_data', 'nomi2012', 'wdpaid', 'name')

    # sql master attribute table pre-authored
    sql = """CREATE OR REPLACE VIEW %s AS
    WITH filter AS
    (SELECT case when tid = %s then null
    else tid end tid, bid::varchar
    FROM %s)
    SELECT bid, %s as nomi_wdpaid, '%s'::text as nomi_name, tid, whs.*
    FROM filter LEFT JOIN whs.view_ca_master_attributes as whs ON
    (filter.tid = whs.wdpaid)
    ORDER BY bid"""%(output.s_name,
                     id,
                     filter.s_name,
                     id,
                     nomi_name[int(id)])

    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

##def check_nomi_row_in_intersect_table(conn_arg, list_nomi_id, base_table, intersectTab, resultTab):
##    # this function returns 1) all unique intersections with nominations
##
##    bid_list = getListFromDBTable(conn_arg, intersectTab.schema, intersectTab.name, bid)
##
##    # for each bid check if nominated id is in
##    nomi_id_pool_set = set(list_nomi_id)
####    for bid in bid_list:
##
##    db_dict = getDictOfListFromDBTable(conn_arg, intersectTab.schema, intersectTab.name, bid, tid)
##    for bid in db_dict.keys():
##        wdpa_pool_set = set(db_dict[bid])
##
##        # all ids are nominated ids - unique/gap filled
##        if wdpa_pool_set.issubset(nomi_id_pool_set):
##            for tid in wdpa_pool_set:
####                add_result_tab(conn_arg, resultTab, tid, bid
##                overview_insert_row(conn_arg, resultTab, tid, base_table, 'Gap filled')
##
##        # if intersection - in bids already represented by existing WH
##        elif wdpa_pool_set.intersection(nomi_id_pool_set):
##            overview_insert_row(conn_arg, resultTab, tid, base_table, 'Already by other WH')
##
##        # if no intersection - only existing WHs overlap
##        else:
##            pass
##
##
##
##def overview_insert_row(conn_arg, overviewTab, tid, base_table, fill_text):
##
##    assert intersect.isTableInDB()
##    conn = conn_arg.getConn()
##    cur = conn.cursor()
##
##    sql = """INSERT INTO %s
##    VALUES (%s, %s, %s)
##    """%(overviewTab.s_name, tid,
##
##
##    cur.execute(sql)
##    conn.commit()
##    cur.close()
##    conn.close()



def post_intersection(conn_arg, intersect, output, nomi_id, nomi_name):
    # this function first filters the intersect table by its wdpaid and
    # then append attributes
    conn = conn_arg.getConn()
    cur = conn.cursor()

    whereclause = 'tid = ' + str(nomi_id)

    header = "CREATE OR REPLACE VIEW %s AS "%(output.s_name)

    withstatement = """WITH filter AS (
    SELECT * FROM %s
    WHERE (bid in (SELECT bid FROM %s WHERE %s))
    ORDER BY bid)"""%(intersect.s_name,
                     intersect.s_name,
                     whereclause)

    maintext = """
    SELECT
    '%s' as nomi_name, *
    FROM filter LEFT JOIN whs.view_ca_master_attributes as whs
    ON (filter.tid = whs.wdpaid)
    ORDER BY bid ASC
    """ %(nomi_name)


    sql = header + withstatement + maintext



    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


def post_intersection_mk2(conn_arg, intersect, output, nomi_id):
    # this function first filters the intersect table by its wdpaid and
    # then append attributes
    conn = conn_arg.getConn()

    whereclause = 'tid = ' + str(nomi_id)

    header = "CREATE OR REPLACE VIEW %s AS "%(output.s_name)

    sql = """
    SELECT * FROM %s
    WHERE (bid in (SELECT bid FROM %s WHERE %s))
    ORDER BY bid)"""%(intersect.s_name,
                     intersect.s_name,
                     whereclause)


    process_sql(sql, conn)

    conn.close()

def kor2012(lookup):
    # lookup dictionary stores (table, id) field
    conn_arg = ConnectionParameter()
    conn = conn_arg.getConn()

    caTab = PgTable('ca_data', 'peter_korea', 'wdpaid')
    for table, fieldname in lookup:
        baseTab = PgTable('pl', table, fieldname)
        # id for output table is trivial
        intersect_table_name = 'pk_' + table + '_' + fieldname
        output_table_name = intersect_table_name + '_filter'

        # objects for output
        intersectTab = PgTable('ca_peter', intersect_table_name, 'id')
        filterTab = PgTable('ca_peter', output_table_name, 'id')

        # intersect view
        pi = PgIntersectView(conn_arg, baseTab, caTab, intersectTab)
        pi.run()

        # filter based on intersect
        filter(conn_arg, intersectTab, filterTab, [9991213])

def island2013(lookup):
    # lookup dictionary stores (table, id) field
    conn_arg = ConnectionParameter()
    conn = conn_arg.getConn()

    caTab = PgTable('ca_data', 'tim_island13', 'wdpaid')
    for table, fieldname in lookup:
        baseTab = PgTable('pl', table, fieldname)
        # id for output table is trivial
        intersect_table_name = 'is13_' + table + '_' + fieldname
        output_table_name = intersect_table_name + '_filter'

        # objects for output
        intersectTab = PgTable('ca_island', intersect_table_name, 'id')
        filterTab = PgTable('ca_island', output_table_name, 'id')

        # intersect view
        pi = PgIntersectView(conn_arg, baseTab, caTab, intersectTab)
        pi.run()

        # filter based on intersect
        filter(conn_arg, intersectTab, filterTab, [9991301, 9991302])


def ca2013(lookup):
    # nomi_2013 includes nomi2013 and all WH
    conn_arg = ConnectionParameter()
    conn = conn_arg.getConn()
    catTab = PgTable('ca_data', 'nomi_2013', 'wdpaid')

    # nomination of 2013 sites
    nomi_2013_wdpaid = [9991300 + i for i in range(1, 6)]

    # wdpaid - country dictionary
    nomi_dict = getDictFromDBTable(conn_arg, 'ca_data', 'nomi2013', 'wdpaid', 'country')
    nomi_name_dict = getDictFromDBTable(conn_arg, 'ca_data', 'nomi2013', 'wdpaid', 'name')

    # overview tab for admin
##    fields_list = [['tid', 'integer'], ['base_layer', 'varchar[255]'], ['overlap','varchar[255]']]

##    overviewTab = PgTable('ca2013_nomi_view', 'all_overview_intersection', 'id')
##    create_table(conn_arg, 'ca2013_nomi_view', 'all_overview_intersection', fields_list)

    # intersection, filter and append attributes for all nominations and comparison
    for table, fieldname in lookup:
        # for each baselayer - id

        # base tab
        baseTab = PgTable('pl', table, fieldname)

        # intersection tab
        intersect_table_name = 'nomi_2013_' + table + '_' + fieldname
        intersectTab = PgTable('ca2013_nomi_view', intersect_table_name, 'id')

        # intersection view, the core intersection finishes. This is also useful for debugging, as
        # no duplicate intersection is needed
        pi = PgIntersectView(conn_arg, baseTab, catTab, intersectTab)
        try:
            pi.run(flag='table')
        except:
            print 'error creating tables, skip and continue'

##        # For admin after running the intersection, get an overview of the result
##        check_nomi_row_in_intersect_table(conn_arg, nomi_2013_wdpaid, table, intersectTab, overviewTab)

         # for each table in the base tab intersection view will have filtered results
        for nomi_id in nomi_2013_wdpaid:
            # need to replace ',' in country iso3
            output_name = nomi_dict[nomi_id].lower().replace(',', '') + '_' + intersect_table_name
            nomi_name = nomi_name_dict[nomi_id]
            outTab = PgTable('ca2013_nomi_view', output_name, 'id')

            # filter and append wh attributes
            post_intersection(conn_arg, intersectTab, outTab, nomi_id, nomi_name)

def cl2013(lookup):
    # cl_2013 includes nomi2013 and all WH
    conn_arg = ConnectionParameter()
    conn = conn_arg.getConn()
    catTab = PgTable('ca_data', 'cl_2013', 'wdpaid')

    # nomination of 2013 sites
    nomi_2013_wdpaid = [33177]

    # wdpaid - country dictionary
    nomi_dict = getDictFromDBTable(conn_arg, 'ca_data', 'cl2013', 'wdpaid', 'country')
    nomi_name_dict = getDictFromDBTable(conn_arg, 'ca_data', 'cl2013', 'wdpaid', 'name')

    # overview tab for admin
##    fields_list = [['tid', 'integer'], ['base_layer', 'varchar[255]'], ['overlap','varchar[255]']]

##    overviewTab = PgTable('ca2013_nomi_view', 'all_overview_intersection', 'id')
##    create_table(conn_arg, 'ca2013_nomi_view', 'all_overview_intersection', fields_list)

    # intersection, filter and append attributes for all nominations and comparison
    for table, fieldname in lookup:
        # for each baselayer - id

        # base tab
        baseTab = PgTable('pl', table, fieldname)

        # intersection tab
        intersect_table_name = 'cl_2013_' + table + '_' + fieldname
        intersectTab = PgTable('ca2013_nomi_view_cl', intersect_table_name, 'id')

        # intersection view, the core intersection finishes. This is also useful for debugging, as
        # no duplicate intersection is needed
        pi = PgIntersectView(conn_arg, baseTab, catTab, intersectTab)
        try:
            pi.run(flag='table')
        except:
            print 'error creating tables, skip and continue'

##        # For admin after running the intersection, get an overview of the result
##        check_nomi_row_in_intersect_table(conn_arg, nomi_2013_wdpaid, table, intersectTab, overviewTab)

         # for each table in the base tab intersection view will have filtered results
        for nomi_id in nomi_2013_wdpaid:
            # need to replace ',' in country iso3
            output_name = nomi_dict[nomi_id].lower().replace(',', '') + '_' + intersect_table_name
            nomi_name = nomi_name_dict[nomi_id]
            outTab = PgTable('ca2013_nomi_view_cl', output_name, 'id')

            # filter and append wh attributes
            post_intersection(conn_arg, intersectTab, outTab, nomi_id, nomi_name)


def ca2012(lookup):
     # lookup dictionary stores (table, id) field
    conn_arg = ConnectionParameter()
    conn = conn_arg.getConn()
    catTab = PgTable('ca_data', 'nomi_2012', 'wdpaid')

    # nomination of 2012 sites
    nomi_2012_wdpaid = [9991200 + i for i in range(1, 13)]

    # replace id with names
    nomi_dict = getDictFromDBTable(conn_arg, 'ca_data', 'nomi2012', 'wdpaid', 'country')

    for table, fieldname in lookup:
        # base tab
        baseTab = PgTable('pl', table, fieldname)

        # intersection tab
        intersect_table_name = 'nomi_2012_' + table + '_' + fieldname
        intersectTab = PgTable('ca2012_nomi_view', intersect_table_name, 'id')

        # intersection view
        pi = PgIntersectView(conn_arg, baseTab, catTab, intersectTab)
        try:
            pi.run(flag='table')
        except:
            print 'error creating tables, skip and continue'


        for nomi in nomi_2012_wdpaid:
            # for each table in the base tab intersection view will have all filtered
            # result
            filter_output_table_name = nomi_dict[nomi].lower() + '_' + intersect_table_name + '_filter_' + str(nomi)
            filterTab = PgTable('ca2012_nomi_view_full', filter_output_table_name, 'id')

            attributes_output_table_name =filter_output_table_name + 'attr'
            attrTab = PgTable('ca2012_nomi_view_full', attributes_output_table_name, 'id')

            filter(conn_arg, intersectTab, filterTab, [nomi])
            add_attributes(conn_arg, filterTab, attrTab)

            # only for biogeographic classification
            if table in ('udv_dcw', 'wwf_terr_ecos_clean') and fieldname != 'eco_name':

                # based on different field value
                # e.g. {'tree': [1,2,3,4], 'grass': [2,3,4,5]'}
                idlist_dict = getDictOfListFromDBTable(conn_arg, filterTab.schema, filterTab.name, 'bid', 'tid')

                for bid in idlist_dict:
                    # additional tables for comparative analysis
                    filter_priority_output_table_name = filter_output_table_name + '_' + validname(bid)
                    priorityTab = PgTable('ca2012_nomi_view_full', filter_priority_output_table_name, 'id')
                    # wdpaid in the master table
                    master_table = PgTable('gga', 'site_count_dump_nov12', 'wdpaid')
                    filter_generic(conn_arg, master_table, priorityTab, idlist_dict[bid], 'wdpaid')




def ca_tentative(lookup):
    # a simple yes/no answer
    conn = conn_arg.getConn()
    cur = conn.cursor()
    for table, fieldname in lookup:
        view_name = 'tentative_view.' + table + '_' + fieldname
        base = 'pl.' + table
        sql = """
        CREATE OR REPLACE VIEW %s AS
         SELECT natural_mix.nom_site AS tentative, natural_mix.objectid AS t_id, natural_mix.critere as t_crit,
         natural_mix.pays AS t_country, base.%s as bid
         FROM tentative.natural_mix, %s base
         WHERE st_intersects(base.geom, natural_mix.geom);
        """%(view_name, fieldname, base)
        cur.execute(sql)
        conn.commit()

    cur.close()
    conn.close()
    del cur, conn



def marine_120928():
    conn_arg = ConnectionParameter()
    conn = conn_arg.getConn()

    feature_list = ['seagrass', 'corals', 'mangroves']
    baseTab = PgTable('gga_marine', 'meow_abnj', 'province')
    for feature in feature_list:
        catTab = PgTable('gga_marine', feature, 'gid')
        int_table = 'ntable_' + feature + '_int'
        group_table = int_table + '_group'
        intTab = PgTable('gga_marine', int_table, 'gid')
        groupTab = PgTable('gga_marine', group_table, 'gid')

        pi = PgIntersectView(conn_arg, baseTab, catTab, intTab)
        pi.run(flag='table')

        # not sure if this helps...
        print "intersection complete. Groupings will start in 3 seconds"
        time.sleep(4)

##        pi2 = PgGroupView(conn_arg, intTab, groupTab)
##        pi2.run()

def marine_130326_marine_features():
##    conn_arg = ConnectionParameter()
##    conn = conn_arg.getConn()
    conn_arg = ConnectionParameter()
    conn = conn_arg.getConn()

    feature_list = ['seagrass', 'corals', 'usgs_mangroves2', 'seamounts']

    # for intersect
    for base in ['meow_200m','pelagic']:
        # base layer
        baseTab = PgTable('pl', base, 'province')

        # feature layer, in this case, WH data
        for feature in feature_list:
            catTab = PgTable('gga_marine', feature, 'gid')

            int_table = 'feature_' + base + '_intersect_' + feature
            intTab = PgTable('gga_marine_mar13', int_table, 'gid')

            # intersection
            pi = PgIntersectView(conn_arg, baseTab, catTab, intTab)
            pi.run(flag='table', simple=True)

            # not sure if this helps...
            time.sleep(1)

    # for grouping
    conn_arg = ConnectionParameter()
    conn = conn_arg.getConn()
    for base in ['meow_200m', 'pelagic']:
        # group
        for feature in ['seagrass', 'corals', 'usgs_mangroves2', 'seamounts']:
            int_table = 'feature_' + base + '_intersect_' + feature
            group_table = int_table + '_group'

            intTab = PgTable('gga_marine_mar13', int_table, 'gid')
            groupTab = PgTable('gga_marine_mar13', group_table, 'gid')

            pi2 = PgGroupView(conn_arg, intTab, groupTab)
            pi2.run(simple=True)


def marine_130326():
##    conn_arg = ConnectionParameter()
##    conn = conn_arg.getConn()
    conn_arg = ConnectionParameter()
    conn = conn_arg.getConn()

    # for intersect
##    for province in ['meow_200m', 'pelagic']:
    for province in ['meow_200m','pelagic']:
        # base layer
        baseTab = PgTable('pl', province, 'province')

        # feature layer, in this case, WH data
##        catTab = PgTable('gga_marine_mar13', 'wh_marine', 'wdpaid')
        catTab = PgTable('whs', 'view_whs_current_marine', 'wdpaid')

        int_table = 'unesco46_'+ province + '_intersect'
        intTab = PgTable('gga_marine_mar13', int_table, 'gid')

        # intersection
        pi = PgIntersectView(conn_arg, baseTab, catTab, intTab)
        pi.run(flag='table')

        # not sure if this helps...
        print "intersection complete. Groupings will start in 3 seconds"
        time.sleep(4)

    # for grouping
    conn_arg = ConnectionParameter()
    conn = conn_arg.getConn()
    for province in ['meow_200m', 'pelagic']:
        # group
        int_table = 'unesco46_' + province + '_intersect'
        group_table = int_table + '_group'

        intTab = PgTable('gga_marine_mar13', int_table, 'gid')
        groupTab = PgTable('gga_marine_mar13', group_table, 'gid')

        pi2 = PgGroupView(conn_arg, intTab, groupTab)
        pi2.run(simple = True)




def test_simple():
    conn_arg = ConnectionParameter()
    conn = conn_arg.getConn()

    baseTab = PgTable('pl', 'meow_200m', 'province')

    catTab = PgTable('whs','view_whs_current_marine', 'wdpaid')
    int_table = 'meow_intersect_mwhs_simple'
##        group_table = int_table + '_group'
    intTab = PgTable('gga_marine_mar13', int_table, 'gid')
##        groupTab = PgTable('gga_marine', group_table, 'province')
##    pi = PgIntersectView(conn_arg, baseTab, catTab, intTab)
##    pi.run(flag='table', simple = True)

    # test group
    conn_arg = ConnectionParameter()
    conn = conn_arg.getConn()

    group_table = int_table + '_group'
    groupTab = PgTable('gga_marine_mar13', group_table, 'province')
    pi2 = PgGroupView(conn_arg, intTab, groupTab)
    pi2.run(simple = True)

def jamie_function(conn_arg, full_table, taxon, field1):
    # this function returns the number of counts
    conn = conn_arg.getConn()
    cur = conn.cursor()

    sql = """SELECT count(*)
    FROM %s
    WHERE "Threatened"='Y'
    AND "%s" = 'H'
    AND "Taxon_group" = '%s'
    """%(full_table, field1, taxon)

    cur.execute(sql)
    result = cur.fetchall()

    cur.close()
    conn.close()

    return result[0][0]

def jamie_simple_count(conn_arg, full_table, taxon, field1):
    # this function returns the number of counts
    conn = conn_arg.getConn()
    cur = conn.cursor()

    sql = """SELECT count(*)
    FROM %s
    WHERE
    "%s" = 'H'
    AND "Taxon_group" = '%s'
    """%(full_table, field1, taxon)

    cur.execute(sql)
    result = cur.fetchall()

    cur.close()
    conn.close()

    return result[0][0]

def jamie():
    field_list = ["SENSITIVITY_Optimistic",
  "SENSITIVITY_Pessimistic",
  "ADAPTABILITY_Optimistic",
  "ADAPTABILITY_Pessimistic",
  "SensXLadaptability_Optimistic",
  "SensXLadaptability_Pessimistic",
  "Final_exposure_akyjy_2025_optimistic",
  "Final_Score_akyjy_2025_optimistic",
  "Final_exposure_akyjy_2025_pessimistic",
  "Final_Score_akyjy_2025_pessimistic",
  "Final_exposure_akyjy_2055_Optimistic",
  "Final Score_akyjy_2055_Optimistic",
  "Final_exposure_akyjy_2055Pessimistic",
  "Final_score_akyjy_2055Pessimistic",
  "Final_exposure_akyjy_2085_Optimistic",
  "Final_score_akyjy_2085_Optimistic",
  "Final_exposure_akyjy_2085Pessimistic",
  "Final_Score_akyjy_2085Pessimistic",
  "Final_exposure_akyuy_2025_Optimistic",
  "Final_score_akyuy_2025_Optimistic",
  "Final_exposure_akyuy_2025Pessimistic",
  "Final_Score_akyuy_2025Pessimistic",
  "Final_exposure_akyuy_2055_Optimistic",
  "Final_Score_akyuy_2055_Optimistic",
  "Final_Exposure_akyuy_2055Pessimistic",
  "Final_Score_akyuy_2055Pessimistic",
  "Final_exposure_akyuy_2085_Optimistic",
  "Final_Score_akyuy_2085_Optimistic",
  "Final_exposure_akyuy_2085Pessimistic",
  "Final_Score_akyuy_2085Pessimistic",
  "Final_exposure_akzcy_2025_Optimistic",
  "Final_Score_akzcy_2025_Optimistic",
  "Final_exposure_akzcy_2025Pessimistic",
  "Final_Score_akzcy_2025Pessimistic",
  "Final_exposure_akzcy_2055_Optimistic",
  "Final_Score_akzcy_2055_Optimistic",
  "Final_exposure_akzcy_2055Pessimistic",
  "Final_score_akzcy_2055Pessimistic",
  "Final_exposure_akzcy_2085_Optimistic",
  "Final_Score_akzcy_2085_Optimistic",
  "Final_exposure_akzcy_2085Pessimistic",
  "Final_Score_akzcy_2085Pessimistic",
  "Final_exposure_akzja_2025_Optimistic",
  "Final_score_akzja_2025_Optimistic",
  "Final_exposure_akzja_2025Pessimistic",
  "Final_score_akzja_2025Pessimistic",
  "Final_exposure_akzja_2055_Optimistic",
  "Final_score_akzja_2055_Optimistic",
  "Final_exposure_akzja_2055Pessimistic",
  "Final_Score_akzja_2055Pessimistic",
  "Final_exposure_akzja_2085_Optimistic",
  "Final_Score_akzja_2085_Optimistic",
  "Final_exposure_akzja_2085Pessimistic",
  "Final_score_akzja_2085Pessimistic",
  "Final_exposure_akzjb_2025_Optimistic",
  "Final_score_akzjb_2025_Optimistic",
  "Final_exposure_akzjb_2025Pessimistic",
  "Final_Score_akzjb_2025Pessimistic",
  "Final_exposure__akzjb_2055_Optimistic",
  "Final_Score_akzjb_2055_Optimistic",
  "Final_exposure_akzjb_2055Pessimistic",
  "Final_Score_akzjb_2055Pessimistic",
  "Final_exposure_akzjb_2085_Optimistic",
  "Final_Score_akzjb_2085_Optimistic",
  "Final_exposure_akzjb_2085Pessimistic",
  "Final_Score_akzjb_2085Pessimistic"]
    taxon_list = ["Reptiles","Birds","Amphibians", "Mammals","Fish"]

    conn_arg = ConnectionParameter()
    full_table = "ad_hoc_analysis.jamie"

    f = open(r"D:\Yichuan\Jamie\result.csv", 'w')

    # first row
    f.write('taxon, fieldname, all_count, thr_count\n')

    # for each taxon and field
    for taxon in taxon_list:
        for field in field_list:
            if field.startswith("Final_Score") or field.startswith("Final_score"):
                thr_col = jamie_function(conn_arg, full_table, taxon, field)
            else:
                thr_col = ''

            all_col = jamie_simple_count(conn_arg, full_table, taxon, field)

            row = ','.join([taxon, field, str(all_col), str(thr_col)]) + '\n'
            f.write(row)

    f.close()

##    print jamie_function(conn_arg, full_table, taxon, field)





# default constants
##ca2012()
conn_arg = ConnectionParameter()
conn = conn_arg.getConn()

lookup = (['eba','ebaname'],
          ['feow', 'ecoregion'],
          ['g200_terr', 'g200_regio'],
          ['g200_marine', 'g200_regio'],
          ['g200_fw', 'g200_regio'],
          ['meow_200m', 'province'],
          ['meow_200m', 'ecoregion'],
          ['cpd', 'site_ref'],
          ['udv_dcw', 'provname'],
          ['udv_dcw', 'biome'],
          ['udv_dcw', 'realm'],
          ['wwf_terr_ecos_clean', 'eco_name'],
          ['wwf_terr_ecos_clean', 'biome'],
          ['wwf_terr_ecos_clean', 'realm'],
          ['wwf_terr_ecos_clean', 'realm_biome'],
          ['hotspot_type_core', 'fname'],
          ['wilderness_type_core', 'wa_name'],
          ['aze', 'aze_id'],
          ['iba', 'sitrecid'],
          ['kba', 'sitrecid'])



# debug
##ca2012(lookup)

