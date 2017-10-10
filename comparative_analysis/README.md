# Spatial comparative analysis for the evaluation of nominations under biodiversity criteria

The detail methodology of the analysis is fully documented in the [Comparative Analysis for World Heritage nominations under biodiversity criteria](https://www.unep-wcmc.org/resources-and-data/comparative-analysis-methodology-for-world-heritage-nominations-under-biodiversity-criteria). This section aims not to repeat what has already been but to focus on the technical details of undertaking the spatial component of the analysis

## Comparative analysis of biogeographical coverage, broad conservation priorities and site level priorities

The analysis has evolved from a manual process that involves preparing individual dataset, loading in ArcGIS and gathering result in a piecemeal way to a semi-automatic process whereby the analysis will now execute automatically and in parallel, in a Postgres/postgis environment by Python. The amount of time required has reduced considerably from months to a matter of weeks or days (depending on the time for data preparation)

### Software and tools
PostgreSQL 9.1 + PostGIS 2.0
Python libraries: Yichuan10 + YichuanDB + conda + pandas + sqlalchemy + psycopg2 + openpyxl

### Data preparation

Data preparation involves the compilation of boundaries for new nominations but also includes checking and updating existing datasets in the postgres database.

The single most time consuming step is to compile boundaries (with a WDPA schema) from nominations dossier. As is not requested in the operational guidelines under the Convention, States Parties are not incentivised and do not usually submit GIS data, which have to be reverse-engineered from maps. Depending on the quality of the submitted maps and the level of detail required, this step could take days or weeks. While it is expected the quality of these digitised boundaries are reasonably comparable to the scale and resolution of maps in the nominations, the accuracy will inherently be inferior to the original GIS data from which the maps have been produced. Despite the shortcoming, the quality of boundaries itself do not impact the quality of the comparative analysis due to the scale and accuracy of other datasets. once the boundaries have been compiled, they are imported to the postgres database using the `PG_GEOMETRY` option - all other datasets are also in native postgis geometries.

`ca_nomi` hosts all boundaries of nominations. NB, the management of these datasets **must** be undertaken within the ArcGIS environment, i.e., do not try and alter them in postgres otherwise one may risk corrupting the integrity of the database.

The update of the other datasets, such as WWF's ecoregion or Birdlife's KBA data, is done using the `shp2pgsql` and `psql` command line utilities. This is due to a historical implementation that *does not* require any ArcGIS dependency. To ensure analyses do not run into errors due to inconsistency and problematic geometries, it is advised to repair geometry in **both** ArcGIS and PostGIS, after import.

### Methodology - spatial overlay

The central idea behind the analysis is a spatial overlap between two datasets: 1. the combined data of existing natural World Heritage sites and nomination data of a particular year, 2. any of the datasets to be overlaid with, for example, ecoregions or KBAs. The spatial analysis works out the percentage overlap between the two and store the results in a table. Then a view is created with filtered rows to associate results of nominated sites and results of existing WH sites that share the same ecoregion or KBA etc. To reduce potential commission error due to reasons such as accuracy, inconsistent boundary or map scales, an empirical threshold of 5% is applied to any result, i.e., any overlap of less than 5% will *not* be considered as 'true' overlaps and thus ignored.

To increase performance, the above sequential analyses are carried in parallel by feeding multiple database calls simultaneously.

A separate analysis is performed to create a set of database views similar to the above (usually once unless there is an update to the tentative list sites data or additions to the other datasets to overlay with). The purpose is to create context for comparison of sites on the tentative list, in addition to World Heritage sites.

More details can be found in the `comparative_analysis.py` script.

### Methodology - output results to excels

Once the analyses are complete, a separate script `comparative_analysis_to_excel_xx.py` is then employed to pull together the results across multiple tables (including look-up tables where needed for attributes and contextual information) and sort the output into the three components: biogeographic classifications, regional priorities and site level priorities. Additionally, full results including all actual percentage overlap information is generated using the script `comparative_analysis_to_excel_fullresults_xx.py`.