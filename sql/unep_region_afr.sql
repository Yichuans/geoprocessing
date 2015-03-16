-- country level aggregates
CREATE OR REPLACE VIEW ad_hoc.result_loss_year_country AS
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
  ad_hoc.result_loss as afr, 
  ad_hoc.country_ecoregion
WHERE 
  afr.patch_id = country_ecoregion.patch_id AND year != 0 AND iso3_code not in ('ESP', ' ', 'ISR', 'PSE', 'XXX')
)
SELECT iso3_code, terr_name, year, sum(total_area_km2) ::double precision as total_area_km2
FROM a
GROUP BY iso3_code, terr_name, year
ORDER BY iso3_code, year;

ALTER TABLE ad_hoc.unep_afr_baseline_country
  OWNER TO ad_hoc;

-- ecoregion level aggregates across continents
CREATE OR REPLACE VIEW ad_hoc.result_loss_year_ecoregion AS
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
  ad_hoc.result_loss as afr, 
  ad_hoc.country_ecoregion
WHERE 
  afr.patch_id = country_ecoregion.patch_id AND year != 0 AND iso3_code not in ('ESP', ' ', 'ISR', 'PSE', 'XXX')
)
SELECT eco_name, biome, realm, year, sum(total_area_km2)::double precision as total_area_km2
FROM a
GROUP BY eco_name, biome, realm, year
ORDER BY eco_name, biome, realm, year;

-- biome level aggregates across continents
CREATE OR REPLACE VIEW ad_hoc.result_loss_year_biome AS
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
  _lookup_wwf_terr_biome.name as biome_name,
  country_ecoregion.realm
FROM 
  ad_hoc.result_loss as afr, 
  ad_hoc.country_ecoregion,
  ad_hoc._lookup_wwf_terr_biome
WHERE 
 bioid = biome AND
  afr.patch_id = country_ecoregion.patch_id AND year != 0 AND iso3_code not in ('ESP', ' ', 'ISR', 'PSE', 'XXX')
)
SELECT biome, biome_name, year, sum(total_area_km2)::double precision as total_area_km2
FROM a
GROUP BY biome, biome_name, year
ORDER BY biome, year;


-- continent level aggregates
CREATE OR REPLACE VIEW ad_hoc.result_loss_year AS
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
  ad_hoc.result_loss as afr, 
  ad_hoc.country_ecoregion
WHERE 
  afr.patch_id = country_ecoregion.patch_id AND year != 0 AND iso3_code not in ('ESP', ' ', 'ISR', 'PSE', 'XXX')
)
SELECT year, sum(total_area_km2) ::double precision as total_area_km2
FROM a
GROUP BY year
ORDER BY year;


ALTER TABLE ad_hoc.result_loss_year_country
OWNER TO ad_hoc;
ALTER TABLE ad_hoc.result_loss_year_ecoregion
OWNER TO ad_hoc;
ALTER TABLE ad_hoc.result_loss_year_biome
OWNER TO ad_hoc;
ALTER TABLE ad_hoc.result_loss_year
OWNER TO ad_hoc;
  