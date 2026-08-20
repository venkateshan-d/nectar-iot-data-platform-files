# Databricks notebook source
# MAGIC %md
# MAGIC # Asset hierarchy - transitive closure
# MAGIC
# MAGIC A job task rather than a pipeline table. The closure is built by iterative
# MAGIC expansion, and a bounded loop with an early exit is not expressible as a
# MAGIC declarative table definition. Knowing where the declarative model stops
# MAGIC being the right tool is part of using it well.
# MAGIC
# MAGIC Cheap and idempotent - full overwrite each run, sub-second at this size.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "nectar")
CATALOG = dbutils.widgets.get("catalog")
MAX_DEPTH = 12

assets = (
    spark.table(f"{CATALOG}.gold.dim_asset")
    .filter("__END_AT IS NULL")          # SCD2: current version only
    .select("asset_id", "asset_name", "asset_type", "site_id",
            "building_id", "parent_asset_id")
)

# A parent pointer that resolves to nothing is an orphan. This MUST be computed
# before the pointer is nulled, otherwise the evidence is gone - the same reason
# the portable pipeline records it at conform time.
valid_parents = assets.select(F.col("asset_id").alias("_p_id"))
assets = (
    assets.join(F.broadcast(valid_parents), assets.parent_asset_id == F.col("_p_id"), "left")
    .withColumn("is_orphan", F.col("parent_asset_id").isNotNull() & F.col("_p_id").isNull())
    .withColumn("is_root", F.col("parent_asset_id").isNull())
    .withColumn("parent_asset_id",
                F.when(F.col("_p_id").isNull(), None).otherwise(F.col("parent_asset_id")))
    .drop("_p_id")
    .cache()
)

# COMMAND ----------

edges = (assets.filter(F.col("parent_asset_id").isNotNull())
         .select(F.col("parent_asset_id").alias("parent"), F.col("asset_id").alias("child"))
         .distinct().cache())

# Depth 0 is the self-pair, which makes "subtree including the node itself" a
# single predicate rather than a UNION.
closure = assets.select(
    F.col("asset_id").alias("ancestor_id"),
    F.col("asset_id").alias("descendant_id"),
    F.lit(0).alias("depth"),
    F.col("asset_id").alias("path"),
)
frontier = closure

for depth in range(1, MAX_DEPTH + 1):
    nxt = (
        frontier.alias("f")
        .join(edges.alias("e"), F.col("f.descendant_id") == F.col("e.parent"))
        .select(F.col("f.ancestor_id").alias("ancestor_id"),
                F.col("e.child").alias("descendant_id"),
                F.lit(depth).alias("depth"),
                F.concat_ws(" > ", F.col("f.path"), F.col("e.child")).alias("path"))
        # Cycle guard: a node can never be its own strict ancestor. Bad master
        # data logs a warning instead of looping forever.
        .filter(F.col("ancestor_id") != F.col("descendant_id"))
    )
    if not nxt.take(1):
        print(f"closure converged at depth {depth - 1}")
        break
    closure = closure.unionByName(nxt)
    frontier = nxt.cache()
else:
    print(f"WARNING: hit max_depth={MAX_DEPTH}; a cycle in parent_asset_id is likely")

closure = closure.dropDuplicates(["ancestor_id", "descendant_id", "depth"])
(closure.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.asset_closure"))

# COMMAND ----------

level = (closure.filter("depth > 0").groupBy(F.col("descendant_id").alias("_id"))
         .agg(F.max("depth").alias("level")))
roots = assets.filter("parent_asset_id IS NULL").select(F.col("asset_id").alias("_root"))
root = (closure.join(F.broadcast(roots), closure.ancestor_id == F.col("_root"))
        .select(F.col("descendant_id").alias("_id"),
                F.col("ancestor_id").alias("root_asset_id"),
                F.col("path").alias("hierarchy_path")))
subtree = (closure.filter("depth > 0").groupBy(F.col("ancestor_id").alias("_id"))
           .agg(F.countDistinct("descendant_id").alias("descendant_count")))
children = (assets.filter("parent_asset_id IS NOT NULL")
            .groupBy(F.col("parent_asset_id").alias("_id"))
            .agg(F.count(F.lit(1)).alias("child_count")))

hierarchy = assets
for helper in (level, root, subtree, children):
    hierarchy = hierarchy.join(F.broadcast(helper), hierarchy.asset_id == helper["_id"], "left").drop("_id")

hierarchy = (
    hierarchy
    .fillna({"level": 0, "descendant_count": 0, "child_count": 0})
    .withColumn("root_asset_id", F.coalesce("root_asset_id", F.col("asset_id")))
    .withColumn("hierarchy_path", F.coalesce("hierarchy_path", F.col("asset_id")))
    .withColumn("is_leaf", F.col("child_count") == 0)
    .withColumn("is_disconnected",
                (F.col("child_count") == 0) & F.col("parent_asset_id").isNull())
    # Isolation alone is not a defect - a standalone boiler legitimately has no
    # asset-level parent. This column separates the real problems from that.
    .withColumn("connectivity_status",
                F.when(F.col("is_orphan"), "ORPHANED")
                .when(F.col("building_id").isNull(), "UNASSIGNED")
                .when(F.col("is_disconnected"), "STANDALONE")
                .otherwise("CONNECTED"))
)
(hierarchy.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.dim_asset_hierarchy"))

print(f"closure rows: {closure.count()} | max level: {hierarchy.agg(F.max('level')).first()[0]}")
display(hierarchy.groupBy("connectivity_status").count().orderBy("count", ascending=False))
