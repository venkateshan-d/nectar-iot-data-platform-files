"""Task 4 - multi-asset hierarchy as a **closure table**.

The problem
-----------
``assets.parent_asset_id`` is an adjacency list. It is compact and cheap to
update, but every interesting question about it ("everything under Site A",
"what breaks if Chiller-01 fails") is a recursive traversal. Spark SQL has no
``WITH RECURSIVE``, and even on a warehouse that does, a recursive CTE per
dashboard query is the wrong cost profile for a read-heavy analytics platform.

The design
----------
Materialise the transitive closure once per batch:

``asset_closure(ancestor_id, descendant_id, depth, path)``

one row for every ancestor/descendant pair, including the self-pair at depth 0.
Then every hierarchy query becomes a single indexed join:

* subtree of X              -> ``WHERE ancestor_id = X``
* ancestors of X            -> ``WHERE descendant_id = X``
* direct children/parent    -> ``AND depth = 1``
* downstream impact of X    -> ``WHERE ancestor_id = X AND depth >= 1``

Trade-off: the closure is O(nodes x average depth) rows and must be rebuilt when
the topology changes. For building automation - tens of thousands of assets,
depth 3-5, topology changing on commissioning rather than continuously - that is
a few hundred thousand rows rebuilt nightly. Cheap, and it turns every read into
a hash join. A graph database (see ``graph_model.py``) is the better answer when
the topology becomes a dense mesh with cycles or when relationships gain many
types; the two models are complementary and both are provided.

Cycle safety
------------
The expansion is bounded by ``max_depth`` and drops any pair where ancestor ==
descendant beyond depth 0, so a mis-entered ``A -> B -> A`` loop produces a
logged warning instead of an infinite loop.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import Config
from ..io_layer import read_table, write_table
from ..logging_utils import RunContext

LOG = logging.getLogger("nectar.hierarchy")

MAX_DEPTH = 12


def _fmt(cfg: Config) -> str:
    return cfg.get("_resolved_format", cfg.table_format)


# ---------------------------------------------------------------------------
def build_closure(assets: DataFrame, max_depth: int = MAX_DEPTH) -> DataFrame:
    """Iteratively expand the adjacency list into a closure table.

    Level 0 is the self-pair (which makes "subtree including the node itself" a
    single predicate instead of a UNION). Each iteration joins the frontier onto
    the direct-edge set; the loop stops when a level adds nothing.
    """
    edges = (
        assets.filter(F.col("parent_asset_id").isNotNull())
        .select(F.col("parent_asset_id").alias("parent"), F.col("asset_id").alias("child"))
        .distinct()
        .cache()
    )

    closure = assets.select(
        F.col("asset_id").alias("ancestor_id"),
        F.col("asset_id").alias("descendant_id"),
        F.lit(0).alias("depth"),
        F.col("asset_id").alias("path"),
    )

    frontier = closure
    for depth in range(1, max_depth + 1):
        nxt = (
            frontier.alias("f")
            .join(edges.alias("e"), F.col("f.descendant_id") == F.col("e.parent"), "inner")
            .select(
                F.col("f.ancestor_id").alias("ancestor_id"),
                F.col("e.child").alias("descendant_id"),
                F.lit(depth).alias("depth"),
                F.concat_ws(" > ", F.col("f.path"), F.col("e.child")).alias("path"),
            )
            # Guard against a topology loop: a node can never be its own
            # strict ancestor.
            .filter(F.col("ancestor_id") != F.col("descendant_id"))
        )
        if not nxt.take(1):
            LOG.info("closure converged at depth %d", depth - 1)
            break
        closure = closure.unionByName(nxt)
        frontier = nxt.cache()
    else:  # pragma: no cover - only on pathological input
        LOG.warning("closure hit max_depth=%d; a cycle in parent_asset_id is likely", max_depth)

    edges.unpersist()
    return closure.dropDuplicates(["ancestor_id", "descendant_id", "depth"])


def build_asset_hierarchy(assets: DataFrame, closure: DataFrame) -> DataFrame:
    """Denormalised per-asset view: level, root, full path, subtree size.

    This is what a dashboard tree-view binds to - one row per asset, no joins.
    """
    # Each helper is renamed to a join key that cannot collide with an asset
    # column, so the joins below stay unambiguous.
    depth_to_root = (
        closure.filter(F.col("depth") > 0)
        .groupBy(F.col("descendant_id").alias("_h_asset_id"))
        .agg(F.max("depth").alias("level"))
    )
    root_ids = assets.filter(F.col("parent_asset_id").isNull()).select(
        F.col("asset_id").alias("_root_id"))
    root = (
        closure.join(F.broadcast(root_ids), closure.ancestor_id == F.col("_root_id"), "inner")
        .select(F.col("descendant_id").alias("_h_asset_id"),
                F.col("ancestor_id").alias("root_asset_id"),
                F.col("path").alias("hierarchy_path"))
    )
    subtree = (
        closure.filter(F.col("depth") > 0)
        .groupBy(F.col("ancestor_id").alias("_h_asset_id"))
        .agg(F.countDistinct("descendant_id").alias("descendant_count"))
    )
    children = (
        assets.filter(F.col("parent_asset_id").isNotNull())
        .groupBy(F.col("parent_asset_id").alias("_h_asset_id"))
        .agg(F.count(F.lit(1)).alias("child_count"))
    )

    out = assets
    for helper in (depth_to_root, root, subtree, children):
        out = out.join(F.broadcast(helper), out.asset_id == helper["_h_asset_id"], "left").drop("_h_asset_id")

    return (
        out
        .withColumn("level", F.coalesce(F.col("level"), F.lit(0)))
        .withColumn("descendant_count", F.coalesce(F.col("descendant_count"), F.lit(0)))
        .withColumn("child_count", F.coalesce(F.col("child_count"), F.lit(0)))
        .withColumn("root_asset_id", F.coalesce(F.col("root_asset_id"), F.col("asset_id")))
        .withColumn("hierarchy_path", F.coalesce(F.col("hierarchy_path"), F.col("asset_id")))
        .withColumn("is_leaf", F.col("child_count") == 0)
        # "Disconnected" = an isolated node in the *asset* graph: it is neither
        # fed by anything nor feeds anything.
        .withColumn("is_disconnected",
                    (F.col("child_count") == 0) & F.col("parent_asset_id").isNull())
        # Isolation alone is not a defect - a standalone boiler legitimately has
        # no asset-level parent. What matters is whether it is still anchored to
        # a building, and whether its parent pointer was dangling. This column
        # separates the three cases so the operations team can triage.
        .withColumn(
            "connectivity_status",
            F.when(F.col("is_orphan"), F.lit("ORPHANED"))
            .when(F.col("building_id").isNull(), F.lit("UNASSIGNED"))
            .when(F.col("is_disconnected"), F.lit("STANDALONE"))
            .otherwise(F.lit("CONNECTED")),
        )
        .select(
            "asset_id", "asset_name", "asset_type", "site_id", "building_id",
            "parent_asset_id", "root_asset_id", "level", "hierarchy_path",
            "child_count", "descendant_count", "is_leaf", "is_root", "is_orphan",
            "is_disconnected", "connectivity_status",
        )
    )


# ---------------------------------------------------------------------------
# The five queries the challenge asks for, expressed against the closure table
# ---------------------------------------------------------------------------
def assets_under_site(hierarchy: DataFrame, site_id: str) -> DataFrame:
    """Q1: every asset at a site, ordered as a tree."""
    return (hierarchy.filter(F.col("site_id") == site_id)
            .orderBy("building_id", "hierarchy_path"))


def parent_and_children(hierarchy: DataFrame, closure: DataFrame, asset_id: str) -> Dict[str, DataFrame]:
    """Q2: immediate parent and immediate children of an asset."""
    parent = closure.filter((F.col("descendant_id") == asset_id) & (F.col("depth") == 1)) \
                    .select(F.col("ancestor_id").alias("asset_id"))
    children = closure.filter((F.col("ancestor_id") == asset_id) & (F.col("depth") == 1)) \
                      .select(F.col("descendant_id").alias("asset_id"))
    return {
        "parent": parent.join(hierarchy, "asset_id", "left"),
        "children": children.join(hierarchy, "asset_id", "left"),
    }


def downstream_impacted(hierarchy: DataFrame, closure: DataFrame, asset_id: str) -> DataFrame:
    """Q3: everything that fails with *asset_id* - its whole subtree.

    ``depth`` is returned so an operator can triage: depth 1 is directly fed,
    depth 3 is three removes away and may have local buffering.
    """
    return (
        closure.filter((F.col("ancestor_id") == asset_id) & (F.col("depth") >= 1))
        .select(F.col("descendant_id").alias("asset_id"), "depth", "path")
        .join(hierarchy.drop("hierarchy_path"), "asset_id", "left")
        .orderBy("depth", "asset_id")
    )


def orphan_assets(hierarchy: DataFrame) -> DataFrame:
    """Q4: assets whose ``parent_asset_id`` points at something that does not exist."""
    return hierarchy.filter(F.col("is_orphan"))


def disconnected_assets(hierarchy: DataFrame) -> DataFrame:
    """Q5: assets with no parent and no children - isolated nodes."""
    return hierarchy.filter(F.col("is_disconnected"))


# ---------------------------------------------------------------------------
def build_hierarchy_tables(spark: SparkSession, cfg: Config, ctx: RunContext,
                           assets: Optional[DataFrame] = None) -> Dict[str, DataFrame]:
    fmt = _fmt(cfg)
    assets = assets if assets is not None else read_table(spark, cfg.table_path("silver", "assets"), fmt)

    closure = build_closure(assets)
    write_table(closure, cfg.table_path("gold", "asset_closure"), fmt=fmt, mode="overwrite")

    hierarchy = build_asset_hierarchy(assets, closure)
    write_table(hierarchy, cfg.table_path("gold", "dim_asset_hierarchy"), fmt=fmt, mode="overwrite")

    n_closure, n_orphan, n_disc = (
        closure.count(),
        hierarchy.filter("is_orphan").count(),
        hierarchy.filter("is_disconnected").count(),
    )
    ctx.record("hierarchy.closure_rows", n_closure)
    ctx.record("hierarchy.orphans", n_orphan)
    ctx.record("hierarchy.disconnected", n_disc)
    LOG.info("hierarchy: %d closure rows, max level %s, %d orphans, %d disconnected",
             n_closure, hierarchy.agg(F.max("level")).collect()[0][0], n_orphan, n_disc)
    return {"asset_closure": closure, "dim_asset_hierarchy": hierarchy}
