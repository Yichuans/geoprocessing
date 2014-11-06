#-------------------------------------------------------------------------------
# Name:        GLC and WDPA
# Purpose:
#
# Author:      Yichuans
#
# Created:     21/10/2014
# Copyright:   (c) Yichuans 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import YichuanDB

biogeo = ['realm', 'biome', 'eco_name']
geopol = ['iso3']
years_dict = {2000: 'result_00',
        2010: 'result_10'}

def main():
    pass

def create_view(group_fields, year, conn):
    """
    This function takes a list of fields and creates a view
    """

    field_statement = ','.join(group_fields)

    create_statement = "CREATE OR REPLACE VIEW %s AS \n"%('wdpa_glc.wg_'+ str(year)+ '_' + '_'.join(group_fields),)

    select_statement =  """
    SELECT %s,
      sum(value_0)/1000000 ::double precision as no_data,
      sum(value_10)/1000000 ::double precision as agriculture,
      sum(value_20)/1000000 ::double precision as forest,
      sum(value_30)/1000000 ::double precision as grass,
      sum(value_40)/1000000 ::double precision as bush,
      sum(value_50)/1000000 ::double precision as wetland,
      sum(value_60)/1000000 ::double precision as water_body,
      sum(value_70)/1000000 ::double precision as tundra,
      sum(value_80)/1000000 ::double precision as artificial,
      sum(value_90)/1000000 ::double precision as bareland,
      sum(value_100)/1000000 ::double precision as ice_snow,
      sum(value_255)/1000000 ::double precision as sea
    """%(field_statement,)

    ## print select_statement

    from_statement = """
    FROM wdpa_glc.%s
    GROUP BY %s
    ORDER BY %s
    """%(years_dict[year], field_statement, field_statement)


    sql = create_statement + select_statement + from_statement
    print(sql)

    YichuanDB.process_sql(sql, conn)


def test_run():
    with YichuanDB.getCurrentWH() as conn:
        for each in biogeo:
            create_view([each], 2000, conn)
        create_view(['iso3'], 2000, conn)


if __name__ == '__main__':
    main()
