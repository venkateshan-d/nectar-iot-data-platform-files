"""Task 4 bonus - the same hierarchy as a property graph (NetworkX / Neo4j).

Why keep a graph model alongside the closure table
--------------------------------------------------
The closure table is the right answer for the analytics platform: it is a plain
table, joins to the facts, and every BI tool understands it. It stops being the
right answer when

* relationships gain **types** beyond containment - "Chiller-01 *feeds*
  AHU-02", "AHU-02 *is monitored by* Sensor-14", "Pump-01 *shares a circuit
  with* Pump-02" - so the structure is a mesh, not a tree;
* queries become genuinely path-shaped ("shortest chilled-water path from the
  plant room to this VAV box");
* the topology changes often enough that rebuilding a closure is wasteful.

That is exactly the shape building-services topology takes as a platform
matures, so this module implements the graph view and the Cypher/Neo4j loading
statements needed to move to a graph database when the time comes.

NetworkX is used here because it runs in-process with no extra infrastructure -
appropriate for tens of thousands of assets, which is where Nectar is today.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import networkx as nx
except ImportError:  # pragma: no cover
    nx = None

LOG = logging.getLogger("nectar.graph")

CONTAINS = "CONTAINS"     # site -> building -> asset
FEEDS = "FEEDS"           # parent asset -> child asset (physical/logical supply)


def _require_networkx():
    if nx is None:  # pragma: no cover
        raise RuntimeError("networkx is required: pip install networkx")


# ---------------------------------------------------------------------------
def build_graph(assets: Iterable[dict], sites: Optional[Iterable[dict]] = None,
                buildings: Optional[Iterable[dict]] = None) -> "nx.DiGraph":
    """Build a typed directed graph of the estate.

    Nodes carry a ``kind`` (Site / Building / Asset) and their attributes; edges
    carry a ``rel`` type. Direction is *downstream*: an edge points from the
    supplier to the thing it supplies, so "what does this failure take out" is a
    forward reachability query and "what feeds this" is a reverse one.
    """
    _require_networkx()
    g = nx.DiGraph()

    for site in sites or []:
        g.add_node(site["site_id"], kind="Site", **{k: v for k, v in site.items() if k != "site_id"})
    for b in buildings or []:
        g.add_node(b["building_id"], kind="Building", **{k: v for k, v in b.items() if k != "building_id"})
        if b.get("site_id"):
            g.add_edge(b["site_id"], b["building_id"], rel=CONTAINS)

    for a in assets:
        aid = a["asset_id"]
        g.add_node(aid, kind="Asset", **{k: v for k, v in a.items() if k != "asset_id"})

    # Edges are added in a second pass so a child that appears before its parent
    # in the input does not create a phantom node.
    for a in assets:
        aid = a["asset_id"]
        parent = a.get("parent_asset_id")
        if parent and g.has_node(parent):
            g.add_edge(parent, aid, rel=FEEDS)
        elif a.get("building_id") and g.has_node(a["building_id"]):
            g.add_edge(a["building_id"], aid, rel=CONTAINS)
    return g


# ---------------------------------------------------------------------------
# the five challenge queries, graph edition
# ---------------------------------------------------------------------------
def assets_under_site(g: "nx.DiGraph", site_id: str) -> List[str]:
    """Q1 - everything reachable from a site node that is an Asset."""
    _require_networkx()
    if site_id not in g:
        return []
    return sorted(n for n in nx.descendants(g, site_id) if g.nodes[n].get("kind") == "Asset")


def parent_and_children(g: "nx.DiGraph", asset_id: str) -> Dict[str, List[str]]:
    """Q2 - direct predecessors and successors."""
    _require_networkx()
    if asset_id not in g:
        return {"parents": [], "children": []}
    return {
        "parents": sorted(p for p in g.predecessors(asset_id) if g.nodes[p].get("kind") == "Asset"),
        "children": sorted(c for c in g.successors(asset_id) if g.nodes[c].get("kind") == "Asset"),
    }


def downstream_impacted(g: "nx.DiGraph", asset_id: str, with_depth: bool = True):
    """Q3 - blast radius of a failure, with hop distance.

    Distance matters operationally: a directly-fed AHU stops within minutes, a
    third-hop sensor may only drift.
    """
    _require_networkx()
    if asset_id not in g:
        return {} if with_depth else []
    if not with_depth:
        return sorted(nx.descendants(g, asset_id))
    lengths = nx.single_source_shortest_path_length(g, asset_id)
    return {n: d for n, d in sorted(lengths.items(), key=lambda kv: (kv[1], kv[0])) if d > 0}


def orphan_assets(assets: Iterable[dict]) -> List[str]:
    """Q4 - ``parent_asset_id`` set but pointing at a node that does not exist.

    This one is deliberately answered from the raw records rather than the
    graph: by the time the graph is built the dangling edge has already been
    dropped, so the graph cannot see it. Referential breaks have to be caught
    where the reference is still visible.
    """
    known = {a["asset_id"] for a in assets}
    return sorted(
        a["asset_id"] for a in assets
        if a.get("parent_asset_id") and a["parent_asset_id"] not in known
    )


def disconnected_assets(g: "nx.DiGraph") -> List[str]:
    """Q5 - isolated asset nodes: no incoming and no outgoing edges."""
    _require_networkx()
    return sorted(
        n for n, d in g.nodes(data=True)
        if d.get("kind") == "Asset" and g.in_degree(n) == 0 and g.out_degree(n) == 0
    )


# ---------------------------------------------------------------------------
# structural diagnostics
# ---------------------------------------------------------------------------
def topology_report(g: "nx.DiGraph") -> dict:
    """Structural health of the estate graph.

    Cycles are the interesting one: ``A feeds B feeds A`` is physically
    impossible in a supply hierarchy and always means bad master data. Detecting
    it here stops the closure-table build from silently truncating at max_depth.
    """
    _require_networkx()
    assets = [n for n, d in g.nodes(data=True) if d.get("kind") == "Asset"]
    sub = g.subgraph(assets)
    try:
        cycles = [c for c in nx.simple_cycles(sub)][:20]
    except Exception:  # pragma: no cover
        cycles = []
    components = list(nx.weakly_connected_components(sub))
    depths = []
    roots = [n for n in sub.nodes if sub.in_degree(n) == 0]
    for r in roots:
        lengths = nx.single_source_shortest_path_length(sub, r)
        depths.append(max(lengths.values()) if lengths else 0)

    return {
        "asset_nodes": len(assets),
        "edges": sub.number_of_edges(),
        "roots": len(roots),
        "leaves": sum(1 for n in sub.nodes if sub.out_degree(n) == 0),
        "weakly_connected_components": len(components),
        "largest_component_size": max((len(c) for c in components), default=0),
        "max_depth": max(depths, default=0),
        # A containment hierarchy is well-formed when it has no cycles and no
        # asset has two parents. `nx.is_forest` alone is not enough: it works on
        # the undirected projection, where a 2-cycle A->B->A collapses to a
        # single edge and looks perfectly tree-shaped.
        "is_forest": (not cycles) and all(sub.in_degree(n) <= 1 for n in sub.nodes),
        "multi_parent_assets": sorted(n for n in sub.nodes if sub.in_degree(n) > 1),
        "cycles_detected": len(cycles),
        "example_cycles": cycles[:5],
        "isolated_assets": len(disconnected_assets(g)),
    }


def critical_assets(g: "nx.DiGraph", top_n: int = 10) -> List[Tuple[str, int]]:
    """Rank assets by blast radius - how many assets go down with them.

    This is the maintenance-prioritisation query the platform exists to answer:
    fix the chiller that takes out 14 downstream units before the sensor that
    takes out nothing.
    """
    _require_networkx()
    assets = [n for n, d in g.nodes(data=True) if d.get("kind") == "Asset"]
    sub = g.subgraph(assets)
    scored = [(n, len(nx.descendants(sub, n))) for n in sub.nodes]
    return sorted(scored, key=lambda kv: (-kv[1], kv[0]))[:top_n]


# ---------------------------------------------------------------------------
# Neo4j
# ---------------------------------------------------------------------------
CYPHER_SCHEMA = """
// Run once. Constraints double as indexes on the lookup keys.
CREATE CONSTRAINT site_id   IF NOT EXISTS FOR (s:Site)     REQUIRE s.site_id IS UNIQUE;
CREATE CONSTRAINT bldg_id   IF NOT EXISTS FOR (b:Building) REQUIRE b.building_id IS UNIQUE;
CREATE CONSTRAINT asset_id  IF NOT EXISTS FOR (a:Asset)    REQUIRE a.asset_id IS UNIQUE;
CREATE INDEX asset_type_idx IF NOT EXISTS FOR (a:Asset)    ON (a.asset_type);
""".strip()

CYPHER_LOAD = """
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
""".strip()

#: The five challenge queries in Cypher - the direct counterpart of the SQL in
#: ``sql/hierarchy/``.
CYPHER_QUERIES = {
    "assets_under_site": """
MATCH (s:Site {site_id: $site_id})-[:CONTAINS|FEEDS*1..10]->(a:Asset)
RETURN DISTINCT a.asset_id AS asset_id, a.asset_type AS asset_type
ORDER BY asset_id;""".strip(),
    "parent_and_children": """
MATCH (a:Asset {asset_id: $asset_id})
OPTIONAL MATCH (p:Asset)-[:FEEDS]->(a)
OPTIONAL MATCH (a)-[:FEEDS]->(c:Asset)
RETURN a.asset_id AS asset_id,
       collect(DISTINCT p.asset_id) AS parents,
       collect(DISTINCT c.asset_id) AS children;""".strip(),
    "downstream_impacted": """
MATCH path = (a:Asset {asset_id: $asset_id})-[:FEEDS*1..10]->(d:Asset)
RETURN DISTINCT d.asset_id AS impacted_asset, min(length(path)) AS hops
ORDER BY hops, impacted_asset;""".strip(),
    "orphan_assets": """
// Requires the dangling reference to be kept as a property on load.
MATCH (a:Asset) WHERE a.parent_asset_id IS NOT NULL
  AND NOT EXISTS { MATCH (p:Asset {asset_id: a.parent_asset_id}) }
RETURN a.asset_id AS orphan_asset_id;""".strip(),
    "disconnected_assets": """
MATCH (a:Asset) WHERE NOT (a)--() RETURN a.asset_id AS disconnected_asset_id;""".strip(),
    "critical_assets": """
MATCH (a:Asset)
OPTIONAL MATCH (a)-[:FEEDS*1..10]->(d:Asset)
RETURN a.asset_id AS asset_id, count(DISTINCT d) AS blast_radius
ORDER BY blast_radius DESC LIMIT 10;""".strip(),
}


def export_cypher(out_dir: str | Path) -> Path:
    """Write the Neo4j schema, loader and query pack to disk."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    target = out / "neo4j_asset_graph.cypher"
    blocks = [
        "// ---------------------------------------------------------------",
        "// Nectar asset topology - Neo4j schema, bulk load and query pack",
        "// ---------------------------------------------------------------",
        CYPHER_SCHEMA, "", CYPHER_LOAD, "",
        "// ------------------------- queries ---------------------------",
    ]
    for name, query in CYPHER_QUERIES.items():
        blocks += [f"// {name}", query, ""]
    target.write_text("\n".join(blocks), encoding="utf-8")
    return target


def export_graphml(g: "nx.DiGraph", path: str | Path) -> Path:
    """GraphML for Gephi / yEd inspection."""
    _require_networkx()
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    clean = g.copy()
    for _, data in clean.nodes(data=True):
        for k, v in list(data.items()):
            if v is None:
                data[k] = ""
            elif not isinstance(v, (str, int, float, bool)):
                data[k] = str(v)
    nx.write_graphml(clean, p)
    return p


def demo(assets: Sequence[dict], sites: Sequence[dict], buildings: Sequence[dict],
         focus_asset: Optional[str] = None) -> dict:
    """Run all five queries plus diagnostics - used by the notebook and tests."""
    g = build_graph(assets, sites, buildings)
    focus = focus_asset or (critical_assets(g, 1)[0][0] if g.number_of_nodes() else None)
    return {
        "topology": topology_report(g),
        "critical_assets": critical_assets(g),
        "focus_asset": focus,
        "focus_parents_children": parent_and_children(g, focus) if focus else {},
        "focus_downstream": downstream_impacted(g, focus) if focus else {},
        "orphans": orphan_assets(assets),
        "disconnected": disconnected_assets(g),
        "assets_under_first_site": assets_under_site(g, sites[0]["site_id"])[:15] if sites else [],
    }


if __name__ == "__main__":  # pragma: no cover
    import argparse

    from ..config import load_config

    parser = argparse.ArgumentParser(description="Asset topology graph utilities")
    parser.add_argument("--export-cypher", default="docs/diagrams")
    args = parser.parse_args()
    print(json.dumps({"cypher": str(export_cypher(args.export_cypher))}, indent=2))
