-- ============================================================================
-- Task 4 - Multi-asset hierarchy queries (relational / closure-table model)
-- ============================================================================
-- Backing tables (built by src/nectar/hierarchy/closure_table.py):
--
--   asset_closure(ancestor_id, descendant_id, depth, path)
--       transitive closure of parent_asset_id, including the depth-0 self pair
--   dim_asset_hierarchy(asset_id, parent_asset_id, root_asset_id, level,
--       hierarchy_path, child_count, descendant_count, is_leaf, is_root,
--       is_orphan, is_disconnected, connectivity_status)
--       one denormalised row per asset for tree views
--
-- Why a closure table and not a recursive CTE: see the module docstring. The
-- recursive-CTE equivalent is given at the bottom of this file for engines that
-- support it, so the design is portable either way.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- 1. Retrieve all assets under a site
-- ---------------------------------------------------------------------------
SELECT
    h.site_id,
    h.building_id,
    h.asset_id,
    h.asset_name,
    h.asset_type,
    h.level,
    h.hierarchy_path,
    h.child_count,
    h.descendant_count,
    h.connectivity_status
FROM dim_asset_hierarchy h
WHERE h.site_id = 'SITE-CBE'
ORDER BY h.building_id, h.hierarchy_path;


-- ---------------------------------------------------------------------------
-- 2. Retrieve parent and child assets of a given asset
-- ---------------------------------------------------------------------------
-- One pass over the closure at depth 1, in both directions. `relationship`
-- makes the result directly renderable in a UI.
SELECT
    'parent' AS relationship,
    c.ancestor_id  AS related_asset_id,
    a.asset_name,
    a.asset_type,
    a.level
FROM asset_closure c
JOIN dim_asset_hierarchy a ON a.asset_id = c.ancestor_id
WHERE c.descendant_id = :asset_id
  AND c.depth = 1

UNION ALL

SELECT
    'child' AS relationship,
    c.descendant_id AS related_asset_id,
    a.asset_name,
    a.asset_type,
    a.level
FROM asset_closure c
JOIN dim_asset_hierarchy a ON a.asset_id = c.descendant_id
WHERE c.ancestor_id = :asset_id
  AND c.depth = 1
ORDER BY relationship DESC, related_asset_id;


-- ---------------------------------------------------------------------------
-- 3. Find downstream impacted assets (blast radius of a failure)
-- ---------------------------------------------------------------------------
-- Everything the asset feeds, transitively, with hop distance so an operator
-- can triage: depth 1 stops within minutes, depth 3 may only drift.
-- Health context is joined in so the on-call engineer sees which of the
-- impacted assets are already unhealthy.
SELECT
    c.depth                              AS hops,
    c.descendant_id                      AS impacted_asset_id,
    h.asset_name,
    h.asset_type,
    h.building_id,
    c.path                               AS impact_path,
    f.faults,
    f.health_score,
    f.risk_band
FROM asset_closure c
JOIN dim_asset_hierarchy h        ON h.asset_id = c.descendant_id
LEFT JOIN curated_fault_statistics f ON f.asset_id = c.descendant_id
WHERE c.ancestor_id = :asset_id
  AND c.depth >= 1
ORDER BY c.depth, c.descendant_id;


-- ---------------------------------------------------------------------------
-- 3b. Which assets are the most critical? (rank by blast radius)
-- ---------------------------------------------------------------------------
-- The maintenance-prioritisation question the hierarchy exists to answer.
SELECT
    c.ancestor_id                         AS asset_id,
    h.asset_name,
    h.asset_type,
    h.site_id,
    COUNT(*)                              AS blast_radius,
    MAX(c.depth)                          AS max_depth,
    SUM(COALESCE(f.faults, 0))            AS downstream_faults
FROM asset_closure c
JOIN dim_asset_hierarchy h            ON h.asset_id = c.ancestor_id
LEFT JOIN curated_fault_statistics f  ON f.asset_id = c.descendant_id
WHERE c.depth >= 1
GROUP BY c.ancestor_id, h.asset_name, h.asset_type, h.site_id
ORDER BY blast_radius DESC, downstream_faults DESC
LIMIT 10;


-- ---------------------------------------------------------------------------
-- 4. Identify orphan assets
-- ---------------------------------------------------------------------------
-- An orphan has a parent_asset_id that does not resolve to a real asset. Note
-- this MUST be evaluated against the raw register: once the dangling pointer is
-- nulled during conforming, the evidence is gone. The silver layer therefore
-- preserves the verdict in `is_orphan`, and the raw check is kept below for
-- direct auditing of the source table.
SELECT
    h.asset_id,
    h.asset_name,
    h.asset_type,
    h.site_id,
    h.building_id,
    h.connectivity_status
FROM dim_asset_hierarchy h
WHERE h.is_orphan
ORDER BY h.site_id, h.asset_id;

-- Equivalent check straight against the register (anti-join form):
-- SELECT a.asset_id, a.parent_asset_id AS dangling_parent_reference
-- FROM   raw_assets a
-- LEFT JOIN raw_assets p ON p.asset_id = a.parent_asset_id
-- WHERE  a.parent_asset_id IS NOT NULL AND p.asset_id IS NULL;


-- ---------------------------------------------------------------------------
-- 5. Identify disconnected assets
-- ---------------------------------------------------------------------------
-- Isolated nodes in the asset graph: no parent, no children. Isolation alone is
-- not automatically a defect - a standalone boiler legitimately has no asset
-- parent - so `connectivity_status` separates the genuine problems
-- (ORPHANED, UNASSIGNED) from legitimate standalone equipment (STANDALONE).
SELECT
    h.connectivity_status,
    COUNT(*) AS assets,
    STRING_AGG(h.asset_id, ', ' ORDER BY h.asset_id) AS asset_ids
FROM dim_asset_hierarchy h
WHERE h.is_disconnected OR h.is_orphan OR h.building_id IS NULL
GROUP BY h.connectivity_status
ORDER BY assets DESC;


-- ---------------------------------------------------------------------------
-- 6. Roll a metric up the hierarchy (why the closure earns its keep)
-- ---------------------------------------------------------------------------
-- Energy attributed to every asset *including everything it feeds*. With an
-- adjacency list this needs a recursive traversal per row; with the closure it
-- is one join.
SELECT
    c.ancestor_id                     AS asset_id,
    h.asset_name,
    h.asset_type,
    ROUND(SUM(e.energy_kwh), 2)       AS subtree_energy_kwh,
    COUNT(DISTINCT c.descendant_id)   AS assets_in_subtree
FROM asset_closure c
JOIN dim_asset_hierarchy h ON h.asset_id = c.ancestor_id
JOIN fact_energy_hourly e  ON e.asset_id = c.descendant_id
GROUP BY c.ancestor_id, h.asset_name, h.asset_type
ORDER BY subtree_energy_kwh DESC
LIMIT 15;


-- ===========================================================================
-- Appendix: the same traversal as a recursive CTE
-- ===========================================================================
-- Supported by PostgreSQL, Snowflake, BigQuery and DuckDB (not by Spark SQL
-- 3.5, which is one of the reasons the closure table is materialised).
--
-- WITH RECURSIVE subtree AS (
--     SELECT asset_id, parent_asset_id, 0 AS depth,
--            CAST(asset_id AS VARCHAR) AS path
--     FROM   dim_asset
--     WHERE  asset_id = :asset_id
--     UNION ALL
--     SELECT a.asset_id, a.parent_asset_id, s.depth + 1,
--            s.path || ' > ' || a.asset_id
--     FROM   dim_asset a
--     JOIN   subtree s ON a.parent_asset_id = s.asset_id
--     WHERE  s.depth < 12                     -- cycle guard
-- )
-- SELECT * FROM subtree WHERE depth >= 1 ORDER BY depth, asset_id;
