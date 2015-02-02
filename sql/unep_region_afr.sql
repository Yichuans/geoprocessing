WITH a AS (
SELECT 
  afr.patch_id, 
  afr.year, 
  afr.count_pixel, 
  afr.total_area_km2, 
  iso3_code,
  country_ecoregion.terr_name, 
  country_ecoregion.eco_name, 
  country_ecoregion.biome, 
  country_ecoregion.realm
FROM 
  ad_hoc.afr, 
  ad_hoc.country_ecoregion
WHERE 
  afr.patch_id = country_ecoregion.patch_id
)
SELECT iso3_code, terr_name, year, sum(total_area_km2) as total_area_km2
FROM a
WHERE year != 0 AND iso3_code not in ('ESP', ' ', 'ISR', 'PSE', 'XXX')
GROUP BY iso3_code, terr_name, year
ORDER BY iso3_code, year
  
