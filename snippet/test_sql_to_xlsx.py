from sqlalchemy import create_engine
import pandas as pd


# base_id: [lookup_table, base_name]
BASE_ID_LOOKUP = {
    'site_ref': 'pl._lookup_cpd',
    'realm_biome': 'pl._lookup_realm_biome',
    'sitrecid': 'pl._lookup_kba_iba',
    'aze_id': 'pl._lookup_aze'
    }


# base: [base_table, base_id/base_name]
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

# start
engine = create_engine('postgresql://postgres:gisintern@localhost/whs_v2')


def gen_sql_percentage_overlap(base):
    # join tables: wh attrs and pl lookups
    if base not in BASE_LOOKUP:
        raise Exception('base not in the base_lookup')

    base_table = BASE_LOOKUP[base][0]
    base_id = BASE_LOOKUP[base][1]

    # arguments for constructing sql
    wh_lookup_table = OUTPUT_SCHEMA + '.' + COMBINED_WH_NOMINATION_VIEW
    intersect = OUTPUT_SCHEMA + '.intersect_' + base_table + '_' + base_id

    # base_id is a text field with meaningful name
    if not base_id in BASE_ID_LOOKUP:
        pl_lookup_table = 'pl.' + base_table
        pl_lookup_table_id = base_id
        pl_lookup_table_name = base_id

    # need to look up name
    else:
        pl_lookup_table = BASE_ID_LOOKUP[base_id]
        pl_lookup_table_id = 'id'
        pl_lookup_table_name = 'name'

    # sql
    sql = """
    SELECT DISTINCT wdpaid, en_name,
    {2}.{4},
    seg_area as overlap_area,
    theme_area as wh_area,
    base_area,
    seg_area/base_area as per_wh

    FROM
    {0},{1},{2}

    WHERE
    {0}.tid = {1}.wdpaid AND
    {0}.bid = {2}.{3}

    """.format(intersect, wh_lookup_table, 
        pl_lookup_table, pl_lookup_table_id, pl_lookup_table_name)

    print sql

    return pd.read_sql(sql, engine)


COMBINED_WH_NOMINATION_VIEW = 'z_combined_wh_nomination_view'
TLS_SHAPE = 'tls.tentative'

OUTPUT_SCHEMA = 'ca_2016'
TLS_SCHEMA = 'ca_tls'
