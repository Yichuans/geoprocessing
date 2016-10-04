# This script is used for exporting pg tables to
# a desired tabular format in excel

import os, sys, psycopg2, time, re

# for some reason, importing excel module failed
from xlwt import *

from YichuanDB import ConnectionParameter, get_sql_result, get_sql_result_as_list, process_sql

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
biogeo_theme_names = {'Biogeographical province': 'udv_dcw_p',
               'Terrestrial biome/realm': 'wwf_rb', ## THIS NEEDS NEW ANALYSES
               'Terrestrial ecoregion': 'wwf_e',
               'Marine province': 'meow_200m_p',
               'Marine ecoregion': 'meow_200m_e'} ## THIS NEEDS NEW ANALYESES

priority_theme_names = {
               'Terrestrial biodiversity hotspot': 'hs',
               'High biodiversity wilderness area': 'ws', ## THIS NEEDS NEW ANALYSES!!
               'Terrestrial Global 200 priority ecoregion': 'g200_terr',
               'Freshwater Global 200 priority ecoregion': 'g200_fw',
               'Marine Global 200 priority ecoregion': 'g200_marine',
               'Endemic Bird Area': 'eba',
               'Centre of Plant Diversity': 'cpd'}


site_theme_names = {
    'KBA': 'kba',
    'IBA': 'iba',
    'AZE': 'aze'
    }

id_lookup = {
    'site_ref': 'pl._lookup_cpd',
    'realm_biome': 'pl._lookup_realm_biome',
    'sitrecid': 'pl._lookup_kba_iba',
    'aze_id': 'pl._lookup_aze'
    }


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
# need to make sure they correspond!


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

def get_wdpa_name_by_id(conn, wdpaid):
    # conn to be reused
    # get the wdpa name
    sql = 'SELECT en_name FROM %s WHERE wdpaid = %s'%(COMBINED_WH_NOMINATION_VIEW, wdpaid)
    cur = conn.cursor()
    cur.execute(sql)
    name = cur.fetchone()
    cur.close()

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
    cur.close()


    return result



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



def pg2excel_per_site(site_id, outputfolder):
    """
    nomination id
    """
    # get site name
    site_name = get_attributes_by_id(conn, site_id, 'wdpaid', OUTPUT_SCHEMA + '.' + COMBINED_WH_NOMINATION_VIEW, '', 'en_name')[0][0]

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

    outputfile = outputfolder + os.sep + re.sub(r'[/\\:*?"<>|]', '', site_name) + '.xls'

    print(outputfile)

    wb.save(outputfile)

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


def process_a_theme(sheet, start_row, site_id, theme_name, themekey):
    # for a theme, get data and write theme
    # theme_base_unit: 'provname, biome etc'
    print(theme_name)
    import string
    theme_base_unit = BASE_LOOKUP[themekey][1]

    # theme_name: 'Biogeographical province'
    theme = Theme(theme_name)

    # find wh_filter_tab
    filter_tab = OUTPUT_SCHEMA + '.' +  get_filtername(site_id, get_intersectname(themekey))
    print(filter_tab)

    # find tentative tab
    te_tab = TLS_SCHEMA + '.' + get_intersectname(themekey)

    # get base units in the filter_tab, e.g. one to many
    base_units = get_attributes_by_id(conn, site_id, 'tid', filter_tab, '',
                                      'bid')

    for base_unit_raw in base_units:
        # raw is a tuple, lookup
        base_unit = string.strip(str(base_unit_raw[0]))

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
            sys.exit(1)

        content = Content(base_name)

        # for each base unit, get existing whs sites
        whereclause = 'bid = \'%s\''%base_unit
        whs_id = get_attributes_by_id(conn, base_unit, 'bid', filter_tab, whereclause,
                                          'tid')
        # convert tuple list to a list
        whs_id_list = [id for (id,) in whs_id]

        # get rid of other nominated properties
        for nomi in NOMI_ID:
            if nomi in whs_id_list:
                whs_id_list.remove(nomi)

        content.add_whs(get_aggregate_whs_attr(whs_id_list))

        # FOR TENTATIVE
        # tentative list sites
        unesid_list = get_attributes_by_id(conn, base_unit, 'bid', te_tab, '', 'tid')
        unesid_list = [id for (id,) in unesid_list]
        te_list = get_aggregate_te_attr(unesid_list)

        content.add_tentative(te_list)

        # add to theme
        theme.add_content(content)

    # start from the second row, first row reserved for headings
    write_theme(sheet, start_row, theme)

    # return start_row for the next theme
    return start_row + theme.get_row_len() + 1


def get_aggregate_whs_attr(wdpaid_list):
    # only for valid result
    result_list = []
    if len(wdpaid_list)>0:
        for wdpaid in wdpaid_list:
            # wdpaid: existing whs wdpaid
            # not None
            if wdpaid != None:
                # all valid wdpa should have these two fields filled
                # debug: print wdpaid
                name = get_attributes_by_id(conn, wdpaid, 'wdpaid', WH_ATTR, '', 'en_name')[0][0]
                country = get_attributes_by_id(conn, wdpaid, 'wdpaid', WH_ATTR, '', 'country_name')[0][0]
                area = get_attributes_by_id(conn, wdpaid, 'wdpaid', WH_ATTR, '', 'gis_area')[0][0]
                crit = get_attributes_by_id(conn, wdpaid, 'wdpaid', WH_ATTR, '', 'crit')[0][0]
    ##                result_list.append((name, country, area, crit))
                result_list.append('; '.join([name, country, format_number_ha(area), crit]))

    return sorted(result_list)


def get_aggregate_te_attr(unesco_list):
    # only for valid result
    result_list = []
    if len(unesco_list)>0:
        for unesid in unesco_list:
            # wdpaid: existing whs wdpaid
            # not None
            if unesid != None:
                # all valid wdpa should have these two fields filled
                # print unesid
                name = get_attributes_by_id(conn, unesid, 'id', TLS_SHAPE, '', 'name')[0][0]
                country = get_attributes_by_id(conn, unesid, 'id', TLS_SHAPE, '', 'country')[0][0]

                crit = get_attributes_by_id(conn, unesid, 'reference', TLS_ORGIN, '', 'critere')

                if crit:
                    crit = crit[0][0] or '(not specified)'
                else:
                    crit = '(not specified)'

                result_list.append('; '.join([name, country, crit]))

    return sorted(result_list)

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



# # 2014 ====================

# COMBINED_WH_NOMINATION_VIEW = 'z_combined_wh_nomination_view'
# WH_ATTR = 'arcgis.v_wh_non_spatial_full'
# TLS_SHAPE = 'tls.tentative'
# TLS_ORGIN = 'tls.origin'

# OUTPUT_SCHEMA = 'ca_2014'
# TLS_SCHEMA = 'ca_tls'

# NOMI_ID = range(9991401, 9991410)

# conn_arg = ConnectionParameter()
# conn = conn_arg.getConn()

# # 2015 ================
# COMBINED_WH_NOMINATION_VIEW = 'z_combined_wh_nomination_view'
# WH_ATTR = 'arcgis.v_wh_non_spatial_full'
# TLS_SHAPE = 'tls.tentative'

# OUTPUT_SCHEMA = 'ca_2015'
# TLS_SCHEMA = 'ca_tls'


# NOMI_ID = range(9991501, 9991505)


# conn_arg = ConnectionParameter(host = 'localhost',
#              db = 'whs_v2',
#              port = '5432',
#              user = 'postgres',
#              password = 'gisintern')

# conn = conn_arg.getConn()

# # 2015 add ===============
# COMBINED_WH_NOMINATION_VIEW = 'z_combined_wh_nomination_view'
# WH_ATTR = 'arcgis.v_wh_non_spatial_full'
# TLS_SHAPE = 'tls.tentative'

# OUTPUT_SCHEMA = 'ca_2015_add'
# TLS_SCHEMA = 'ca_tls'


# NOMI_ID = range(9991505, 9991507)


# conn_arg = ConnectionParameter(host = 'localhost',
#              db = 'whs_v2',
#              port = '5432',
#              user = 'postgres',
#              password = 'gisintern')

# conn = conn_arg.getConn()

# 2016 st.vincent and the grenadines
COMBINED_WH_NOMINATION_VIEW = 'z_combined_wh_nomination_view'
WH_ATTR = 'arcgis.v_wh_non_spatial_full'
TLS_SHAPE = 'tls.tentative'

OUTPUT_SCHEMA = 'ca_2016'
TLS_SCHEMA = 'ca_tls'
TLS_ORGIN = 'tls.origin'

# NOMI_ID = [9991601]
NOMI_ID = range(9991601, 9991610)


conn_arg = ConnectionParameter(host = 'localhost',
             db = 'whs_v2',
             port = '5432',
             user = 'postgres',
             password = 'gisintern')

conn = conn_arg.getConn()


# test
##main()

def main(outputfolder):
    # for each site/ each theme

    # candwdpaidate 1) found in priority 2) not found in priority (NO)
    # existing sites 1) found in priority 2) not found in priority (NA/None)

    for candidate in NOMI_ID:
        pg2excel_per_site(candidate, outputfolder)
    conn.close()

def to_excel_2016(outputfolder = r"E:\Yichuan\Comparative_analysis_2016"):
    main(outputfolder)



# if __name__ == 'main':
#     to_excel_2015()