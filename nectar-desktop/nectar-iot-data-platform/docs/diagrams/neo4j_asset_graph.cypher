// ---------------------------------------------------------------
// Nectar asset topology - Neo4j schema, bulk load and query pack
// ---------------------------------------------------------------
// Run once. Constraints double as indexes on the lookup keys.
CREATE CONSTRAINT site_id   IF NOT EXISTS FOR (s:Site)     REQUIRE s.site_id IS UNIQUE;
CREATE CONSTRAINT bldg_id   IF NOT EXISTS FOR (b:Building) REQUIRE b.building_id IS UNIQUE;
CREATE CONSTRAINT asset_id  IF NOT EXISTS FOR (a:Asset)    REQUIRE a.asset_id IS UNIQUE;
CREATE INDEX asset_type_idx IF NOT EXISTS FOR (a:Asset)    ON (a.asset_type);

// Bulk load from the gold layer exported as CSV.
LOAD CSV WITH HEADERS FROM 'file:///sites.csv' AS row
MERGE (s:Site {site_id: row.site_id})
  SET s.site_name = row.site_name, s.city = row.city, s.country = row.country;

LOAD CSV WITH HEADERS FROM 'file:///buildings.csv' AS row
MERGE (b:Building {building_id: row.building_id})
  SET b.building_name = row.building_name, b.floor_area_sqm = toFloat(row.floor_area_sqm)
WITH row, b MATCH (s:Site {site_id: row.site_id})
MERGE (s)-[:CONTAINS]->(b);

LOAD CSV WITH HEADERS FROM 'file:///assets.csv' AS row
MERGE (a:Asset {asset_id: row.asset_id})
  SET a.asset_name = row.asset_name, a.asset_type = row.asset_type,
      a.manufacturer = row.manufacturer, a.rated_power_kw = toFloat(row.rated_power_kw);

// Containment for top-level assets, FEEDS for the rest.
LOAD CSV WITH HEADERS FROM 'file:///assets.csv' AS row
MATCH (a:Asset {asset_id: row.asset_id})
FOREACH (_ IN CASE WHEN row.parent_asset_id IS NULL THEN [1] ELSE [] END |
  MERGE (b:Building {building_id: row.building_id}) MERGE (b)-[:CONTAINS]->(a));

LOAD CSV WITH HEADERS FROM 'file:///assets.csv' AS row
WITH row WHERE row.parent_asset_id IS NOT NULL
MATCH (p:Asset {asset_id: row.parent_asset_id}), (c:Asset {asset_id: row.asset_id})
MERGE (p)-[:FEEDS]->(c);

// ------------------------- queries ---------------------------
// assets_under_site
MATCH (s:Site {site_id: $site_id})-[:CONTAINS|FEEDS*1..10]->(a:Asset)
RETURN DISTINCT a.asset_id AS asset_id, a.asset_type AS asset_type
ORDER BY asset_id;

// parent_and_children
MATCH (a:Asset {asset_id: $asset_id})
OPTIONAL MATCH (p:Asset)-[:FEEDS]->(a)
OPTIONAL MATCH (a)-[:FEEDS]->(c:Asset)
RETURN a.asset_id AS asset_id,
       collect(DISTINCT p.asset_id) AS parents,
       collect(DISTINCT c.asset_id) AS children;

// downstream_impacted
MATCH path = (a:Asset {asset_id: $asset_id})-[:FEEDS*1..10]->(d:Asset)
RETURN DISTINCT d.asset_id AS impacted_asset, min(length(path)) AS hops
ORDER BY hops, impacted_asset;

// orphan_assets
// Requires the dangling reference to be kept as a property on load.
MATCH (a:Asset) WHERE a.parent_asset_id IS NOT NULL
  AND NOT EXISTS { MATCH (p:Asset {asset_id: a.parent_asset_id}) }
RETURN a.asset_id AS orphan_asset_id;

// disconnected_assets
MATCH (a:Asset) WHERE NOT (a)--() RETURN a.asset_id AS disconnected_asset_id;

// critical_assets
MATCH (a:Asset)
OPTIONAL MATCH (a)-[:FEEDS*1..10]->(d:Asset)
RETURN a.asset_id AS asset_id, count(DISTINCT d) AS blast_radius
ORDER BY blast_radius DESC LIMIT 10;
