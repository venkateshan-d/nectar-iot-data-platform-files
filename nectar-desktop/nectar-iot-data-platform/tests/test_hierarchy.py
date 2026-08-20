"""Tests for the asset hierarchy - closure table and graph model.

Both implementations answer the same five questions, so the tests assert that
they agree. A divergence between them is a real bug: it means one of the two
models the platform ships would give an operator a different blast radius.

Topology under test::

    SITE-A / BLD-1
        CHILLER-1
            AHU-1
                SENSOR-1
            AHU-2
        PUMP-1
            FLOW-1
        BOILER-1            (standalone: no parent, no children)
        GHOST-1             (orphan: parent points at MISSING-99)
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from nectar.hierarchy import graph_model as gm
from nectar.hierarchy.closure_table import (
    build_asset_hierarchy,
    build_closure,
    disconnected_assets,
    downstream_impacted,
    orphan_assets,
    parent_and_children,
)

ASSETS = [
    dict(asset_id="CHILLER-1", asset_name="Chiller 1", asset_type="Chiller",
         site_id="SITE-A", building_id="BLD-1", parent_asset_id=None),
    dict(asset_id="AHU-1", asset_name="AHU 1", asset_type="AHU",
         site_id="SITE-A", building_id="BLD-1", parent_asset_id="CHILLER-1"),
    dict(asset_id="AHU-2", asset_name="AHU 2", asset_type="AHU",
         site_id="SITE-A", building_id="BLD-1", parent_asset_id="CHILLER-1"),
    dict(asset_id="SENSOR-1", asset_name="Sensor 1", asset_type="Temp Sensor",
         site_id="SITE-A", building_id="BLD-1", parent_asset_id="AHU-1"),
    dict(asset_id="PUMP-1", asset_name="Pump 1", asset_type="Pump",
         site_id="SITE-A", building_id="BLD-1", parent_asset_id=None),
    dict(asset_id="FLOW-1", asset_name="Flow 1", asset_type="Flow Sensor",
         site_id="SITE-A", building_id="BLD-1", parent_asset_id="PUMP-1"),
    dict(asset_id="BOILER-1", asset_name="Boiler 1", asset_type="Boiler",
         site_id="SITE-A", building_id="BLD-1", parent_asset_id=None),
    dict(asset_id="GHOST-1", asset_name="Ghost 1", asset_type="UPS",
         site_id="SITE-A", building_id="BLD-1", parent_asset_id="MISSING-99"),
]

SITES = [dict(site_id="SITE-A", site_name="Site A")]
BUILDINGS = [dict(building_id="BLD-1", building_name="Building 1", site_id="SITE-A")]


@pytest.fixture(scope="module")
def assets_df(spark):
    """The silver-conformed register: dangling parents nulled, flags set."""
    known = {a["asset_id"] for a in ASSETS}
    rows = []
    for a in ASSETS:
        parent = a["parent_asset_id"]
        is_orphan = bool(parent) and parent not in known
        rows.append({**a,
                     "parent_asset_id": None if is_orphan else parent,
                     "is_orphan": is_orphan,
                     "is_root": parent is None})
    return spark.createDataFrame(rows)


@pytest.fixture(scope="module")
def closure(assets_df):
    return build_closure(assets_df).cache()


@pytest.fixture(scope="module")
def hierarchy(assets_df, closure):
    return build_asset_hierarchy(assets_df, closure).cache()


@pytest.fixture(scope="module")
def graph():
    return gm.build_graph(ASSETS, SITES, BUILDINGS)


# ---------------------------------------------------------------------------
# closure table structure
# ---------------------------------------------------------------------------
def test_closure_contains_the_self_pair_for_every_asset(closure, assets_df):
    self_pairs = closure.filter(F.col("depth") == 0).count()
    assert self_pairs == assets_df.count()


def test_closure_is_transitive(closure):
    """CHILLER-1 must reach SENSOR-1 at depth 2, not only AHU-1 at depth 1."""
    pairs = {(r["ancestor_id"], r["descendant_id"]): r["depth"]
             for r in closure.collect()}
    assert pairs[("CHILLER-1", "AHU-1")] == 1
    assert pairs[("CHILLER-1", "SENSOR-1")] == 2
    assert pairs[("AHU-1", "SENSOR-1")] == 1
    assert ("AHU-2", "SENSOR-1") not in pairs      # different branch


def test_closure_has_no_strict_self_ancestry(closure):
    bad = closure.filter((F.col("depth") > 0)
                         & (F.col("ancestor_id") == F.col("descendant_id")))
    assert bad.count() == 0


def test_levels_and_paths(hierarchy):
    rows = {r["asset_id"]: r for r in hierarchy.collect()}
    assert rows["CHILLER-1"]["level"] == 0
    assert rows["AHU-1"]["level"] == 1
    assert rows["SENSOR-1"]["level"] == 2
    assert rows["SENSOR-1"]["root_asset_id"] == "CHILLER-1"
    assert rows["SENSOR-1"]["hierarchy_path"] == "CHILLER-1 > AHU-1 > SENSOR-1"
    assert rows["CHILLER-1"]["descendant_count"] == 3
    assert rows["CHILLER-1"]["child_count"] == 2


# ---------------------------------------------------------------------------
# the five challenge queries
# ---------------------------------------------------------------------------
def test_assets_under_site(hierarchy, graph):
    sql_ids = {r["asset_id"] for r in hierarchy.filter(F.col("site_id") == "SITE-A").collect()}
    graph_ids = set(gm.assets_under_site(graph, "SITE-A"))
    assert sql_ids == {a["asset_id"] for a in ASSETS}
    assert graph_ids == sql_ids


def test_parent_and_children_agree_between_models(hierarchy, closure, graph):
    rel = parent_and_children(hierarchy, closure, "AHU-1")
    assert [r["asset_id"] for r in rel["parent"].collect()] == ["CHILLER-1"]
    assert [r["asset_id"] for r in rel["children"].collect()] == ["SENSOR-1"]

    g = gm.parent_and_children(graph, "AHU-1")
    assert g["parents"] == ["CHILLER-1"]
    assert g["children"] == ["SENSOR-1"]


def test_downstream_impact_agrees_between_models(hierarchy, closure, graph):
    sql_rows = {r["asset_id"]: r["depth"]
                for r in downstream_impacted(hierarchy, closure, "CHILLER-1").collect()}
    graph_rows = gm.downstream_impacted(graph, "CHILLER-1")
    assert sql_rows == {"AHU-1": 1, "AHU-2": 1, "SENSOR-1": 2}
    assert graph_rows == sql_rows


def test_leaf_has_no_downstream_impact(hierarchy, closure, graph):
    assert downstream_impacted(hierarchy, closure, "SENSOR-1").count() == 0
    assert gm.downstream_impacted(graph, "SENSOR-1") == {}


def test_orphans_agree_between_models(hierarchy, graph):
    assert [r["asset_id"] for r in orphan_assets(hierarchy).collect()] == ["GHOST-1"]
    assert gm.orphan_assets(ASSETS) == ["GHOST-1"]


def test_disconnected_and_connectivity_status(hierarchy, graph):
    rows = {r["asset_id"]: r for r in hierarchy.collect()}
    # BOILER-1 is isolated in the asset graph but legitimately so.
    assert rows["BOILER-1"]["is_disconnected"] is True
    assert rows["BOILER-1"]["connectivity_status"] == "STANDALONE"
    # GHOST-1's parent was dangling; that is a defect, not a standalone asset.
    assert rows["GHOST-1"]["connectivity_status"] == "ORPHANED"
    assert rows["AHU-1"]["connectivity_status"] == "CONNECTED"

    sql_disconnected = {r["asset_id"] for r in disconnected_assets(hierarchy).collect()}
    assert sql_disconnected == {"BOILER-1", "GHOST-1"}


# ---------------------------------------------------------------------------
# graph diagnostics
# ---------------------------------------------------------------------------
def test_topology_report(graph):
    report = gm.topology_report(graph)
    assert report["asset_nodes"] == len(ASSETS)
    assert report["cycles_detected"] == 0
    assert report["is_forest"] is True
    assert report["max_depth"] == 2


def test_critical_assets_ranks_by_blast_radius(graph):
    ranked = dict(gm.critical_assets(graph))
    assert ranked["CHILLER-1"] == 3
    assert ranked["PUMP-1"] == 1
    assert ranked["BOILER-1"] == 0


def test_a_cycle_is_detected_and_does_not_hang_the_closure(spark):
    """Bad master data (A feeds B feeds A) must be caught, not looped on."""
    rows = [
        dict(asset_id="A", asset_name="A", asset_type="Pump", site_id="S", building_id="B",
             parent_asset_id="B", is_orphan=False, is_root=False),
        dict(asset_id="B", asset_name="B", asset_type="Pump", site_id="S", building_id="B",
             parent_asset_id="A", is_orphan=False, is_root=False),
    ]
    df = spark.createDataFrame(rows)
    result = build_closure(df, max_depth=4)          # bounded: terminates
    assert result.filter(F.col("ancestor_id") == F.col("descendant_id"))\
                 .filter(F.col("depth") > 0).count() == 0

    report = gm.topology_report(gm.build_graph(rows))
    assert report["cycles_detected"] >= 1
    assert report["is_forest"] is False


def test_cypher_export_is_complete(tmp_path):
    path = gm.export_cypher(tmp_path)
    text = path.read_text()
    for name in gm.CYPHER_QUERIES:
        assert name in text
    assert "CREATE CONSTRAINT" in text and "MERGE (p)-[:FEEDS]->(c)" in text
