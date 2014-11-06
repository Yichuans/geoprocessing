# This script is used for exporting pg tables to
# a desired tabular format in excel
# for comparative analysis 2012
#


import os, sys, psycopg2, time

# for some reason, importing excel module failed
os.chdir(r"C:\Python27\ArcGIS10.1\Lib\xlwt-0.7.5")


from xlwt import *

# style information, headings bold
font = Font()
font.bold = True
alignment = Alignment()
alignment.horz = Alignment.HORZ_CENTER
alignment.vert = Alignment.VERT_CENTER

style_bold = XFStyle()
style_bold.font = font
style_bold.alignment = alignment

style_align = XFStyle()
style_align.alignment = alignment

# default style
style_default = XFStyle()

# dictionaries
biogeo_theme_names = {'Biogeographical province': 'udv_dcw_provname',
               'Terrestrial biome/realm': 'wwf_terr_ecos_clean_realm_biome', ## THIS NEEDS NEW ANALYSES
               'Terrestrial ecoregion': 'wwf_terr_ecos_clean_eco_name',
               'Marine province': 'meow_200m_province',
               'Marine ecoregion': 'meow_200m_ecoregion'} ## THIS NEEDS NEW ANALYESES

priority_theme_names = {
               'Terrestrial biodiversity hotspot': 'hotspot_type_core_fname',
               'High biodiversity wilderness area': 'wilderness_type_core_wa_name', ## THIS NEEDS NEW ANALYSES!!
               'Terrestrial Global 200 priority ecoregion': 'g200_terr_g200_regio',
               'Freshwater Global 200 priority ecoregion': 'g200_fw_g200_regio',
               'Marine Global 200 priority ecoregion': 'g200_marine_g200_regio',
               'Endemic Bird Area': 'eba_ebaname',
               'Centre of Plant Diversity': 'cpd_site_ref'}

id_lookup = {
    'cpd_site_ref': 'pl._lookup_id_cpd',
    'wwf_terr_ecos_clean_realm_biome': 'pl._lookup_id_realm_biome',
    'iba_sitrecid': 'pl._lookup_id_kba_iba',
    'kba_sitrecid': 'pl._lookup_id_kba_iba',
    'aze_aze_id': 'pl._lookup_id_aze'
    }

site_theme_names = {
    'KBA': 'kba_sitrecid',
    'IBA': 'iba_sitrecid',
    'AZE': 'aze_aze_id'
    }
# need to make sure they correspond!

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

##class CaPostgres2Excel(object):
##    # programmtically produce excel from postgres queries
##    def __init__(self, conn_arg):
##        # init
##        pass
##
##    def _check_row_count(self, view):
##        # return row count in a view
##        pass
##
##    def _get_wdpa_name_by_id(self, id):
##        # get name from lookup table
##        pass
##
##    def _get_compare_items(self, id, view):
##        # get items in same
##        pass
# Cheat GET GLOBAL dictionary
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


# get data
def check_row_count(conn, view):
    # conn to be reused
    # count number of rows
    sql = 'SELECT count(*) FROM %s'%view
    cur = conn.cursor()
    cur.execute(sql)
    num = cur.fetchone()[0]
    del cur
    return int(num)

def get_wdpa_name_by_id(conn, wdpaid, REF_TABLE = 'ca_data.nomi_2013'):
    # conn to be reused
    # get the wdpa name
    sql = 'SELECT name FROM %s WHERE wdpaid = %s'%(REF_TABLE, wdpaid)
    cur = conn.cursor()
    cur.execute(sql)
    name = cur.fetchone()
    del cur
    if name:
        # pg returns utf-8 encoded string
        return name[0].decode('utf-8')
    else:
        raise Exception('No name found for the input wdpaid')

def get_all_fields(conn, table):
    # get all fields
    """<table/view> must have qualified names"""
    schema = table.split('.')[0]
    name = table.split('.')[1]
    cur = conn.cursor()

    # container
    fieldlist = []
    sql = """select column_name from
    information_schema.columns where
    table_schema = '%s' and table_name = '%s'"""%(schema, name)
    cur.execute(sql)
    for each in cur.fetchall():
        fieldlist.append(each[0])

    del cur
    return fieldlist

def get_attributes_by_id(conn, id, id_field, ref_table, whereclause = '', *attr):
    """<ref_table>, <id_field>, qualified name, int"""
    # conn to be reused
    # get attributes from ref_table
    cur = conn.cursor()
    for each in attr:
        if each not in get_all_fields(conn, ref_table):
            error_statment = 'input fields "%s" not found in ref_table: %s'%(each, str(get_all_fields(conn, ref_table)))
            print ref_table
            raise Exception(error_statment)

    # main
    fields = ','.join(attr)
    # counter for text
    if type(id) == type('a'):
        id_value = '\''+id+'\''
    else:
        id_value = id
    if whereclause == '':
        sql = 'SELECT DISTINCT %s FROM %s WHERE %s = %s'%(fields, ref_table, id_field, id_value)
    else:
        sql = 'SELECT DISTINCT %s FROM %s WHERE %s = %s AND %s'%(fields, ref_table, id_field, id_value, whereclause)
    # populate attrs field names
    cur.execute(sql)
    result = cur.fetchall()
    del cur
    return result


def get_aggregate_whs_attr(wdpaid_list):
    # only for valid result
    result_list = []
    if len(wdpaid_list)>0:
        for id in wdpaid_list:
            # id: existing whs wdpaid
            # not None
            if id != None:
                # all valid wdpa should have these two fields filled
                name = get_attributes_by_id(conn, id, 'wdpaid','whs.whs', '', 'name')[0][0]
                country = get_attributes_by_id(conn, id, 'wdpaid', 'whs.whs_country_ag_name', '', 'country')[0][0]
                area = get_attributes_by_id(conn, id, 'wdpaid', 'whs.view_whs_gis_area', '', 'areakm2')[0][0]
                crit = get_attributes_by_id(conn, id, 'wdpaid', 'whs.whs', '', 'int_crit')[0][0]
    ##                result_list.append((name, country, area, crit))
                result_list.append('; '.join([name, country, format_number_ha(area), crit]))

    return sorted(result_list)

##def get_aggregate_te_attr(
def get_organise_te_list(get_attr_list):
    # accommodate tuple and clean up
    te_list = []
    for tuple in get_attr_list:
        name = tuple[0]
        country = tuple[1]
        # in case no crit is available
        crit = tuple[2] or '(not specified)'
        te_list.append('; '.join([name, country, crit]))
    return sorted(te_list)


def format_number_ha(number):
    # format whc format -> str in ha
    ha = int(round(100 * float(number)))

    def intWithCommas(x):
        if type(x) not in [type(0), type(0L)]:
            raise TypeError("Parameter must be an integer.")
        if x < 0:
            return '-' + intWithCommas(-x)
        result = ''
        while x >= 1000:
            x, r = divmod(x, 1000)
            result = ",%03d%s" % (r, result)
        return "%d%s" % (x, result)

    return intWithCommas(ha) + ' ha'


# write output
def write_a_column(sheet, start_row, start_column, content_list, style = style_default):
    if len(content_list) > 0:
        col = start_column
        # content index
        index = 0
        for row in range(start_row, start_row + len(content_list)):
            sheet.write(row, col, content_list[index], style)
            index += 1

def write_a_row(sheet, start_row, start_col, heading_list, style = style_default):
    if len(heading_list) > 0:
        index = 0
        for col in range(start_col, start_col + len(heading_list)):
            sheet.write(start_row, col, heading_list[index], style)
            index += 1

def write_theme(sheet, start_row, theme):
    # for each
    max_row_len = theme.get_row_len()

    # first column: udvardy
##    write_a_column(sheet, start_row, 0, [theme.theme_name], style_bold)
    end_row =  start_row + theme.get_row_len() - 1
    sheet.write_merge(start_row, end_row, 0, 0, theme.theme_name, style_bold)

    row = start_row
    # second column: udvardy provinces
    for content in theme.contents:
        # end_row, merge cell in the row headings
        merge_end_row = row + content.get_row_len() - 1
##        merge_end_row = row + content.get_row_len() - 1

        sheet.write_merge(row, merge_end_row, 1, 1, [content.unit_name], style_align)
##        write_a_column(sheet, row, 1, [content.unit_name])
        # third column: existing wh sites
        write_a_column(sheet, row, 2, content.wh_list)

        # fourth column: tentative sites
        write_a_column(sheet, row, 3, content.te_list)

        # auto increment
##        row += content.get_row_len() + 1
        row = merge_end_row + 1

class Content:
    # a block grouped by base unit: each prov in udv
    def __init__(self, base_unit_name):
        self.unit_name = base_unit_name
        self.wh_list = []
        self.te_list = []

    def add_whs(self, wh_list):
        self.wh_list = wh_list
##        # control length
##        if len(self.wh_list) > 30:
##            self.wh_list = ['Many (%s sites)'%len(self.te_list)]
        # if none
        if len(self.wh_list) == 0:
            self.wh_list = ['None']

    def add_tentative(self, te_list):
        self.te_list = te_list
        # control length
        if len(self.te_list) > 30:
            self.te_list = ['Many (%s sites)'%len(self.te_list)]

    def get_row_len(self):
        return max(len(self.wh_list), len(self.te_list), 1)

    def get_max_string_size_wh(self):
        # return the longest string size
        return max(map(len, self.wh_list))

    def get_max_string_size_te(self):
        # return the longest string size
        return max(map(len, self.te_list))

    def get_max_string_size_unit(self):
        return len(self.base_unit_name)

class Theme:
    # a block grouped by theme: udv
    def __init__(self, theme_name):
        self.theme_name = theme_name
        self.contents = []

    def add_content(self, content):
        self.contents.append(content)

    def get_row_len(self):
        # at least size 1
        return sum([content.get_row_len() for content in self.contents]) or 1

    def get_max_string_size_wh(self):
        return max([content.get_max_string_size_wh() for content in self.contents])

    def get_max_string_size_te(self):
        return max([content.get_max_string_size_te() for content in self.contents])

    def get_max_string_size_unit(self):
        return max([content.get_max_string_size_unit() for content in self.contents])

##def testmain():
##    wb = Workbook(encoding = 'utf-8')
##    sheet = wb.add_sheet('testsheet', cell_overwrite_ok = True)
##    ct = Content('Palau Tropical Moist')
##    ct.add_whs(['a','b', 'c'])
##    ct.add_tentative(['ccc', 'ddd', 'eee', 'fff'])
##
##    ct2 = Content('Micronesia')
##    ct2.add_whs(['a','b', 'c', 'd'])
##    ct2.add_tentative(['ccc', 'ddd', 'eee', 'fff', 'ggg'])
##
##    tm = Theme('Terrestrial Ecoregion')
##    tm.add_content(ct)
##    tm.add_content(ct2)
##
##    write_theme(sheet, 0, tm)
##    wb.save(r"C:\testesttest.xls")

def main():
    # for each site/ each theme

    # candidate 1) found in priority 2) not found in priority (NO)
    # existing sites 1) found in priority 2) not found in priority (NA/None)

    candidate_list = range(9991301, 9991306)


##    candidate_list = [9991204]
    for candidate in candidate_list:
        pg2excel_per_site(candidate)

    pass

def pg2excel_per_site(site_id):
    # get site name
    site_name = get_attributes_by_id(conn, site_id, 'wdpaid', 'ca_data.nomi2013', '', 'name')[0][0]
    # for a site
    wb = Workbook(encoding = 'utf-8')
    sheet1 = wb.add_sheet('biogeographic', cell_overwrite_ok = True)
    sheet2 = wb.add_sheet('priority', cell_overwrite_ok = True)
    sheet3 = wb.add_sheet('site-level', cell_overwrite_ok = True)

    # heading
    write_a_row(sheet1, 0, 0, ['Base', 'Nominated property',
                'World heritage sites in the same biogeographic unit',
                'Tentative List sites in the same biogeographic unit'], style_bold)

    write_a_row(sheet2, 0, 0, ['Base', 'Nominated property',
                'World heritage sites in the same priority region',
                'Tentative List sites in the same priority region'], style_bold)

    write_a_row(sheet3, 0, 0, ['Base', 'Nominated property',
                'World heritage sites in the same site-level priorities',
                'Tentative List sites in the same site-level priorities'], style_bold)

    # sheet 1 biogeographic
    start_row = 1
    for theme_name in sorted(biogeo_theme_names.keys()):
        start_row = process_a_theme(sheet1, start_row, site_id, theme_name, biogeo_theme_names[theme_name])

    # sheet 2 priority
    start_row = 1
    for theme_name in sorted(priority_theme_names.keys()):
        start_row = process_a_theme(sheet2, start_row, site_id, theme_name, priority_theme_names[theme_name])

    # kba/iba/aze
    start_row = 1
    for theme_name in sorted(site_theme_names.keys()):
        start_row = process_a_theme(sheet3, start_row, site_id, theme_name, site_theme_names[theme_name])

    # adjust field lengths
    sheet1.col(0).width = 30 * 256
    sheet1.col(1).width = 50 * 256
    sheet1.col(2).width = 70 * 256
    sheet1.col(3).width = 70 * 256

    sheet2.col(0).width = 30 * 256
    sheet2.col(1).width = 50 * 256
    sheet2.col(2).width = 70 * 256
    sheet2.col(3).width = 70 * 256

    sheet3.col(0).width = 30 * 256
    sheet3.col(1).width = 50 * 256
    sheet3.col(2).width = 70 * 256
    sheet3.col(3).width = 70 * 256

    wb.save(r"C:\%s.xls"%site_name)

def process_a_theme(sheet, start_row, site_id, theme_name, theme_base_unit):
    # for a theme, get data and write theme
##    theme_name = 'test'
    theme = Theme(theme_name)

    # find filter_tab
##    base_unit_name = ''
##    filter_tab = 'ca2012_nomi_view.nomi_2012_' + theme_base_unit + '_filter_' + str(site_id) + 'attr'
    filter_tab = 'ca2013_nomi_view.' +  nomi_dict[site_id].lower().replace(',', '') + '_nomi_2013_' + theme_base_unit

##    site_id = '9991210' # candidate id
    te_tab = 'tentative_view.' + theme_base_unit

    # get base units in the filter_tab, e.g. one to many
    base_units = get_attributes_by_id(conn, site_id, 'tid', filter_tab, '',
                                      'bid')

    for base_unit_raw in base_units:
        # raw is a tuple, lookup
        base_unit = base_unit_raw[0]
##        base_unit_descript =

        # look up table here
        try:
            if id_lookup.has_key(theme_base_unit):
                base_name = get_attributes_by_id(conn, base_unit, 'id', id_lookup[theme_base_unit], '',
                                                 'name')[0][0]
            else:
                base_name = base_unit

        except Exception as e:
            print str(e)
            print sheet, start_row, site_id, theme_name, theme_base_unit

        content = Content(base_name)

        # for each base unit, get existing whs sites
        whereclause = 'bid = \'%s\''%base_unit
        whs_id = get_attributes_by_id(conn, base_unit, 'bid', filter_tab, whereclause,
                                          'wdpaid')
        # convert tuple list to a list
        whs_id_list = [id for (id,) in whs_id]
        content.add_whs(get_aggregate_whs_attr(whs_id_list))

        # FOR TENTATIVE
        # tentative list sites
        te_list = get_organise_te_list(get_attributes_by_id(conn, base_unit, 'bid', te_tab, '',
                             'tentative', 't_country', 't_crit'))
        content.add_tentative(te_list)

        # add to theme
        theme.add_content(content)

    # start from the second row, first row reserved for headings
    write_theme(sheet, start_row, theme)

    # return start_row for the next theme
    return start_row + theme.get_row_len() + 1


##    # get whs wdpa id (id,)
##    attrs = get_attributes_by_id(conn, site_id, 'nomi_wdpaid', filter_table, whereclause,
##                                 'wdpaid')
####    # per base unit within theme
##    if check_row_count(conn, filter_tab) == 0:
##        content.add_whs(['No'])
##        content.add_tentative(['N/A'])
##
##    else:
##
##        content.add_whs



conn_arg = ConnectionParameter()
conn = conn_arg.getConn()
nomi_dict = getDictFromDBTable(conn_arg, 'ca_data', 'nomi2013', 'wdpaid', 'country')

# test
##main()
