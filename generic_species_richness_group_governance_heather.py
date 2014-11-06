#-------------------------------------------------------------------------------
# Name:        Generic species group
# Purpose:
#
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
#
# Created:     2013/08/14
# Copyright:   (c) Yichuan Shi 2013

# DON'T USE THIS AS A TEMPLATE
# DON'T USE THIS AS A TEMPLATE
# DON'T USE THIS AS A TEMPLATE
# DON'T USE THIS AS A TEMPLATE
# DON'T USE THIS AS A TEMPLATE
# DON'T USE THIS AS A TEMPLATE
# DON'T USE THIS AS A TEMPLATE

#-------------------------------------------------------------------------------

##def main():
##    pass

##if __name__ == '__main__':
##    main()
import YichuanDB

RL = ['LC', 'VU', 'NT', 'EN', 'CR', 'EX', 'DD']
CLASS = ["AMPHIBIA", "MAMMALIA", "AVES", "REPTILIA"]
KINGDOM = ["PLANTAE", "ANIMALIA", "FUNGI","PROTISTA"]

KINGDOM_FIELD = 'kingdom'
CLASS_FIELD = 'class'
BINOMIAL_FIELD = 'binomial'
RL_FIELD = 'category'



# constants
# all_species table
all_sp = "governance.species_result"
all_sp_taxonid = 'species_result.species_id' # must not be the same as all_sis_taxonid
all_sp_baseid = 'species_result.base_id'

# all sis table
all_sis = "ca_nomi.sis_2014"
all_sis_taxonid = "sis_2014.id_no"

# WH/nomi name look up table
# NOTE: assuming wdpaid is present!!!!!
wh_nomi_lookup = "governance.pa_name"
wh_nomi_name = "pa_name.name"

# a dirty trick - to get withstatement subquery fieldname
filter_taxonid = 'id_no'
filter_baseid = 'base_id'

def filter_sp(whereclause):
    # this function partitions based on the criteria
    sql = """WITH
    filter_sis AS
    (SELECT *
    FROM %s
    %s
    ),
    filter_sp AS
    (SELECT %s, filter_sis.*
    FROM %s JOIN filter_sis
    ON %s = filter_sis.%s)
    """%(all_sis, whereclause, all_sp_baseid, all_sp, all_sp_taxonid, filter_taxonid)
    return sql


def summarise_sp(with_filter_statement, output_name):
    # per hexagon
    header = """
    CREATE OR REPLACE VIEW %s AS """%(output_name)

    core = """
    SELECT %s:: int as wdpaid, COUNT(DISTINCT %s) as count
    FROM filter_sp
    GROUP BY %s
    ORDER BY %s
    """%(filter_baseid, filter_taxonid, filter_baseid, filter_baseid)

    sql = header + with_filter_statement + core

    process(sql)

def summarise_sp_detail(with_filter_statement, output_name):
    # per hexagon
    header = """
    CREATE OR REPLACE VIEW %s_detail AS """%(output_name,)

    core = """
    SELECT DISTINCT %s::int as wdpaid, %s as site_name, %s::int as taxon_id, filter_sp.%s as class_name, filter_sp.%s as binomial
    FROM filter_sp LEFT JOIN %s on (wdpaid = %s)
    """ %(filter_baseid, wh_nomi_name, filter_taxonid, CLASS_FIELD, BINOMIAL_FIELD,
        wh_nomi_lookup, all_sp_baseid.split('.')[1])

    sql = header + with_filter_statement + core

    process(sql)

def process(sql):
    # run sql
    conn = YichuanDB.ConnectionParameter().getConn()
    cur = conn.cursor()

    print sql

    cur.execute(sql)
    conn.commit()

    conn.close()

    cur.close()

def main(output_schema):

    # input
##    output_schema = ""
    overview_table = output_schema + '.sp_overview'

    # constants
    no_thr = "WHERE " + RL_FIELD + " <> 'EX'"
    thr = "WHERE " + RL_FIELD + " IN ('VU', 'EN', 'CR')"
    bio_classes = ["AMPHIBIA", "MAMMALIA", "AVES", "REPTILIA"]
    kingdoms = ["PLANTAE", "ANIMALIA"]

    # all species
    # overall
    whereclause = no_thr
    filterstatement = filter_sp(whereclause)
    view_all = output_schema +'.sr_all'

    summarise_sp(filterstatement, view_all)
    summarise_sp_detail(filterstatement, output_schema +'.sr_all')

    # threat
    whereclause = thr
    filterstatement = filter_sp(whereclause)
    view_all_thr = output_schema +'.tsr_all'

    summarise_sp(filterstatement, view_all_thr)
    summarise_sp_detail(filterstatement, output_schema + '.tsr_all')

    for bio_class in bio_classes:
        # overall
        whereclause = no_thr + "AND " + CLASS_FIELD + " = '" + bio_class + "'"
        filterstatement = filter_sp(whereclause)

        summarise_sp(filterstatement, output_schema +'.sr_' + bio_class.lower())
        summarise_sp_detail(filterstatement, output_schema + '.sr_' + bio_class.lower())

        # threat
        whereclause = thr + "AND " + CLASS_FIELD + " = '" + bio_class + "'"
        filterstatement = filter_sp(whereclause)
        summarise_sp(filterstatement, output_schema +'.tsr_' + bio_class.lower())
        summarise_sp_detail(filterstatement, output_schema + '.tsr_' + bio_class.lower())

    for kingdom in kingdoms:
        # overall
        whereclause = no_thr + "AND " + KINGDOM_FIELD + " = '" + kingdom + "'"
        filterstatement = filter_sp(whereclause)
        summarise_sp(filterstatement, output_schema + '.sr_' + kingdom.lower())
        summarise_sp_detail(filterstatement, output_schema + '.sr_' + kingdom.lower())

        # threat
        whereclause = thr + "AND " + KINGDOM_FIELD + " = '" + kingdom + "'"
        filterstatement = filter_sp(whereclause)
        summarise_sp(filterstatement, output_schema + '.tsr_' + kingdom.lower())
        summarise_sp_detail(filterstatement, output_schema + '.tsr_' + kingdom.lower())

    # once this is all done - create an overview

    header = """
    CREATE OR REPLACE VIEW %s AS
    WITH unique_base AS
    (SELECT DISTINCT wdpaid::int, %s as site_name FROM %s LEFT JOIN %s ON (wdpaid = %s))
    """%(overview_table, wh_nomi_name, all_sp, wh_nomi_lookup, all_sp_baseid.split('.')[1])

    # concatenate attrs
    attrlist = []
    fromtext = ""
    for each in kingdoms:
        attrlist.append("""CASE WHEN %s.count IS NULL THEN 0 ELSE %s.count END as %s,
        CASE WHEN %s.count IS NULL THEN 0 ELSE %s.count END as %s """%(
        'sr_' + each.lower(),
        'sr_' + each.lower(),
        'all_' + each.lower(),
        'tsr_' + each.lower(),
        'tsr_' + each.lower(),
        'thr_' + each.lower()))

        fromtext += """
        LEFT JOIN %s USING (%s)
        LEFT JOIN %s USING (%s)
        """%(output_schema + '.sr_' + each.lower(), 'wdpaid',
        output_schema + '.tsr_' + each.lower(), 'wdpaid')


    for each in bio_classes:
        attrlist.append("""CASE WHEN %s.count IS NULL THEN 0 ELSE %s.count END as %s,
        CASE WHEN %s.count IS NULL THEN 0 ELSE %s.count END as %s """%('sr_' + each.lower(),
        'sr_' + each.lower(),
        'all_' + each.lower(),
        'tsr_' + each.lower(),
        'tsr_' + each.lower(),
        'thr_' + each.lower()))

        fromtext += """
        LEFT JOIN %s USING (%s)
        LEFT JOIN %s USING (%s)"""%(output_schema + '.sr_' + each.lower(), 'wdpaid',
        output_schema + '.tsr_' + each.lower(), 'wdpaid')

    select_attr = ','.join(attrlist)

    firstline = """SELECT wdpaid, site_name, %s.count as rl_all, %s.count as rl_thr, """%('sr_all', 'tsr_all') + select_attr
    fromline = """FROM unique_base LEFT JOIN %s USING (%s) LEFT JOIN %s USING (%s) """%(
    view_all, 'wdpaid', view_all_thr, 'wdpaid') + fromtext

    orderline = "ORDER BY %s::int"%('wdpaid',)

    sql = header + firstline + fromline + orderline

    process(sql)

main('governance')

##def main2():
##    mammal_whereclause = "WHERE class_name = 'MAMMALIA'"
##    filterstatement = filter_sp(mammal_whereclause)
##    summarise_sp(filterstatement, 'sr_mammal')
##    summarise_sp_detail(filterstatement, 'sr_mammal')

