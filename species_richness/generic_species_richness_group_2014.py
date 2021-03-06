#-------------------------------------------------------------------------------
# Name:        Generic species group
# Purpose:
#
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
#
# Created:     2013/08/14
# Copyright:   (c) Yichuan Shi 2013

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
all_sp = "ca_2014.species"
all_sp_taxonid = 'species.id_no' # must not be the same as all_sis_taxonid
all_sp_baseid = 'species.wdpaid'

# all sis table
all_sis = "ca_nomi.sis_2014"
all_sis_taxonid = "sis_2014.id_no"

# WH/nomi name look up table
# NOTE: assuming wdpaid is present!!!!!
wh_nomi_lookup = "ca_2014.z_combined_wh_nomination_view"
wh_nomi_name = "z_combined_wh_nomination_view.en_name"

# a dirty trick - to get withstatement subquery fieldname
filter_taxonid = 'id_no'
filter_baseid = 'wdpaid'

def filter_sp(whereclause):
    # this function partitions based on the criteria
    sql = """WITH
    filter_sis AS
    (SELECT *
    FROM %s
    %s
    ),
    filter_sp AS
    (SELECT wdpaid, filter_sis.*
    FROM %s JOIN filter_sis
    ON %s = filter_sis.%s)
    """%(all_sis, whereclause, all_sp, all_sp_taxonid, filter_taxonid)
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
    FROM filter_sp LEFT JOIN %s USING (wdpaid)
    """ %(filter_baseid, wh_nomi_name, filter_taxonid, CLASS_FIELD, BINOMIAL_FIELD,
        wh_nomi_lookup)

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
    (SELECT DISTINCT wdpaid::int, %s as site_name FROM %s LEFT JOIN %s USING (wdpaid))
    """%(overview_table, wh_nomi_name, all_sp, wh_nomi_lookup)

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
        """%(output_schema + '.sr_' + each.lower(), filter_baseid,
        output_schema + '.tsr_' + each.lower(), filter_baseid)


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
        LEFT JOIN %s USING (%s)"""%(output_schema + '.sr_' + each.lower(), filter_baseid,
        output_schema + '.tsr_' + each.lower(), filter_baseid)

    select_attr = ','.join(attrlist)

    firstline = """SELECT wdpaid, site_name, %s.count as rl_all, %s.count as rl_thr, """%('sr_all', 'tsr_all') + select_attr
    fromline = """FROM unique_base LEFT JOIN %s USING (%s) LEFT JOIN %s USING (%s) """%(
    view_all, filter_baseid, view_all_thr, filter_baseid) + fromtext

    orderline = "ORDER BY %s::int"%(filter_baseid,)

    sql = header + firstline + fromline + orderline

    process(sql)

main('ca_2014')

##def main2():
##    mammal_whereclause = "WHERE class_name = 'MAMMALIA'"
##    filterstatement = filter_sp(mammal_whereclause)
##    summarise_sp(filterstatement, 'sr_mammal')
##    summarise_sp_detail(filterstatement, 'sr_mammal')

