CREATE OR REPLACE VIEW ad_hoc.landcover_result AS
with lc as (
SELECT 
  landcover.wdpaid, 
  landcover.year, 
  landcover.lc_class, 
  sum(landcover.pixel) as pixel
FROM 
  ad_hoc.landcover
GROUP BY
  landcover.wdpaid, 
  landcover.year, 
  landcover.lc_class
ORDER BY
  landcover.wdpaid, 
  landcover.year, 
  landcover.lc_class), lc_result as (
SELECT 
  landcover.wdpaid, 
  split_part(landcover.lc_class, '-', 1) s_lc,
  split_part(landcover.lc_class, '-', 2) t_lc,
  landcover.pixel as pixel,
  landcover.pixel * 0.03 * 0.03 as sqkm
FROM 
  lc as landcover
WHERE year like '%-%'
)
SELECT lc.*, llc.lc_name source, llc2.lc_name target, lc.sqkm as areakm2
FROM lc_result lc LEFT JOIN ad_hoc._lookup_landcover llc ON (lc.s_lc = llc.lc_class)
LEFT JOIN ad_hoc._lookup_landcover llc2 ON (lc.t_lc = llc2.lc_class)

