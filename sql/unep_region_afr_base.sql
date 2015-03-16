-- country level aggregates
CREATE OR REPLACE VIEW ad_hoc.result_baseline_country_25 AS
WITH a AS (
SELECT 
  afr_baseline.patch_id,  
  afr_baseline.count_pixel, 
  afr_baseline.total_area_km2, 
  iso3_code,
  country_ecoregion.terr_name, 
  country_ecoregion.eco_name, 
  country_ecoregion.biome, 
  country_ecoregion.realm
FROM 
  ad_hoc.result_base_25 as afr_baseline, 
  ad_hoc.country_ecoregion
WHERE 
  afr_baseline.patch_id = country_ecoregion.patch_id AND iso3_code not in ('ESP', ' ', 'ISR', 'PSE', 'XXX')
)
SELECT iso3_code, terr_name, sum(total_area_km2) ::double precision as total_area_km2
FROM a
GROUP BY iso3_code, terr_name
ORDER BY iso3_code;
  
-- ecoregion level aggregates across continents
CREATE OR REPLACE VIEW ad_hoc.result_baseline_ecoregion_25 AS
WITH a AS (
SELECT 
  afr_baseline.patch_id,  
  afr_baseline.count_pixel, 
  afr_baseline.total_area_km2, 
  iso3_code,
  country_ecoregion.terr_name, 
  country_ecoregion.eco_name, 
  country_ecoregion.biome, 
  country_ecoregion.realm
FROM 
  ad_hoc.result_base_25 as afr_baseline, 
  ad_hoc.country_ecoregion
WHERE 
  afr_baseline.patch_id = country_ecoregion.patch_id AND iso3_code not in ('ESP', ' ', 'ISR', 'PSE', 'XXX')
)
SELECT eco_name, biome, realm, sum(total_area_km2)::double precision as total_area_km2
FROM a
GROUP BY eco_name, biome, realm
ORDER BY eco_name, biome, realm;

-- biome level aggregates across continents
CREATE OR REPLACE VIEW ad_hoc.result_baseline_biome_25 AS
WITH a AS (
SELECT 
  afr_baseline.patch_id,  
  afr_baseline.count_pixel, 
  afr_baseline.total_area_km2, 
  iso3_code,
  country_ecoregion.terr_name, 
  country_ecoregion.eco_name, 
  country_ecoregion.biome,
  _lookup_wwf_terr_biome.name as biome_name,
  country_ecoregion.realm
FROM 
  ad_hoc.result_base_25 as afr_baseline, 
  ad_hoc.country_ecoregion,
  ad_hoc._lookup_wwf_terr_biome
WHERE 
 bioid = biome AND
  afr_baseline.patch_id = country_ecoregion.patch_id AND iso3_code not in ('ESP', ' ', 'ISR', 'PSE', 'XXX')
)
SELECT biome, biome_name, sum(total_area_km2)::double precision as total_area_km2
FROM a
GROUP BY biome, biome_name
ORDER BY biome;


ALTER TABLE ad_hoc.result_baseline_country_25
OWNER TO ad_hoc;
ALTER TABLE ad_hoc.result_baseline_ecoregion_25
OWNER TO ad_hoc;
ALTER TABLE ad_hoc.result_baseline_biome_25
OWNER TO ad_hoc;
