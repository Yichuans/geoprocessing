#-------------------------------------------------------------------------------
# Name:        UNEP regional report - forest change by Hansen 2013 data -- SQL aggregate
# Purpose:	   To aggregate stats in SQL
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
# Created:     2015/03/16
#-------------------------------------------------------------------------------

from YichuanDB import process_sql, ConnectionParameter
from Yichuan10 import simple_time_tracker

# CONNECTION CONSTANT
USER = 'ad_hoc' # This also determines the target schema where the workspace is, i.e., input and output
PWD = 'ad_hoc'

# get connection parameters
CONN_PARAM = ConnectionParameter(host = 'localhost',
                 db = 'whs_v2',
                 port = '5432',
                 user = USER,
                 password = PWD)

# country level stats
@simple_time_tracker
def create_sql_country_stats(input_table, input_lookup, output_view):
	sql = """
	-- country level aggregates
	CREATE OR REPLACE VIEW %s AS
	WITH a AS (
	SELECT 
	  input_result.patch_id, 
	  input_result.year, 
	  input_result.count_pixel, 
	  input_result.total_area_km2, 
	  iso3,
	  input_lookup.country, 
	  input_lookup.eco_name, 
	  input_lookup.biome, 
	  input_lookup.realm
	FROM 
	  %s as input_result, 
	  %s as input_lookup
	WHERE 
	  input_result.patch_id = input_lookup.patch_id AND year != 0
	)
	SELECT iso3, country, year, sum(total_area_km2) ::double precision as total_area_km2
	FROM a
	GROUP BY iso3, country, year
	ORDER BY iso3, year;

	"""%(output_view, input_table, input_lookup)

	return sql


@simple_time_tracker
def create_sql_loss_year_stats(input_table, output_view):
	sql = """
		CREATE OR REPLACE VIEW %s AS
		WITH a AS (
		SELECT 
		  input_result.patch_id, 
		  input_result.year, 
		  input_result.count_pixel, 
		  input_result.total_area_km2
		FROM 
		  %s as input_result
		WHERE 
		  year != 0
		  )
		SELECT year, sum(total_area_km2) ::double precision as total_area_km2
		FROM a
		GROUP BY year
		ORDER BY year;

		"""%(output_view, input_table)
	return sql


@simple_time_tracker
def create_sql_country_stats_no_year(input_table, input_lookup, output_view):
	sql = """
	-- country level aggregates
	CREATE OR REPLACE VIEW %s AS
	WITH a AS (
	SELECT 
	  input_result.patch_id,
	  input_result.count_pixel, 
	  input_result.total_area_km2, 
	  iso3,
	  input_lookup.country, 
	  input_lookup.eco_name, 
	  input_lookup.biome, 
	  input_lookup.realm
	FROM 
	  %s as input_result, 
	  %s as input_lookup
	WHERE 
	  input_result.patch_id = input_lookup.patch_id
	)
	SELECT iso3, country, sum(total_area_km2) ::double precision as total_area_km2
	FROM a
	GROUP BY iso3, country
	ORDER BY iso3;

	"""%(output_view, input_table, input_lookup)

	return sql



def main():
	# get connection
	conn = CONN_PARAM.getConn()

	# create sql for country loss stats and run
	sql = create_sql_country_stats('ap_result', 'ap_country_lookup', 'ap_loss_country')
	process_sql(sql, conn)

	# create sql for loss year stats
	sql = create_sql_loss_year_stats('ap_result', 'ap_loss_year')
	process_sql(sql, conn)

	# create sql for country base stats and run
	sql = create_sql_country_stats_no_year('ap_base_result', 'ap_country_lookup', 'ap_base_country')
	process_sql(sql, conn)

	# create sql for country loss stats and run
	sql = create_sql_country_stats('wa_result', 'wa_country_lookup', 'wa_loss_country')
	process_sql(sql, conn)

	# create sql for loss year stats
	sql = create_sql_loss_year_stats('wa_result', 'wa_loss_year')
	process_sql(sql, conn)

	# create sql for country base stats and run
	sql = create_sql_country_stats_no_year('wa_base_result', 'wa_country_lookup', 'wa_base_country')
	process_sql(sql, conn)

	# close
	conn.close()


def correct_result_base10():
	# get connection
	conn = CONN_PARAM.getConn()

	# create sql for country base stats and run
	sql = create_sql_country_stats_no_year('ap_base_result_10', 'ap_country_lookup', 'ap_base_country_10')
	process_sql(sql, conn)

	# create sql for country base stats and run
	sql = create_sql_country_stats_no_year('wa_base_result_10', 'wa_country_lookup', 'wa_base_country_10')
	process_sql(sql, conn)

	# close
	conn.close()




# main()

# correct_result_base10()


def lac():
	conn = CONN_PARAM.getConn()

	# create sql for country loss stats and run
	sql = create_sql_country_stats('result_loss', 'lac_teow_v2', 'lac_loss_country')
	process_sql(sql, conn)

	# create sql for loss year stats
	sql = create_sql_loss_year_stats('result_loss', 'lac_loss_year')
	process_sql(sql, conn)

	# create sql for country base stats and run
	sql = create_sql_country_stats_no_year('result_base10', 'lac_teow_v2', 'lac_base_country')
	process_sql(sql, conn)

	# close
	conn.close()	


lac()