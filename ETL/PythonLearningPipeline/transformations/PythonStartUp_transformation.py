from pyspark import pipelines as dp
from pyspark.sql.functions import col

# ── 1. DEFINE THE STREAMING TABLE (like CREATE OR REFRESH in SQL) ──────────
dp.create_streaming_table(
  name             = "orders_streaming",
  comment          = "Streaming table ingesting real-time orders",
  schema           = """
    order_id      BIGINT      NOT NULL,
    customer_id   BIGINT,
    order_status  STRING,
    order_date    DATE,
    region        STRING,
    total_amount  DECIMAL(10, 2),
    created_at    TIMESTAMP
  """,
  partition_cols   = ["order_date"],
  table_properties = {
    "delta.autoOptimize.optimizeWrite" : "true",
    "delta.autoOptimize.autoCompact"   : "true"
  },
  expect_all_or_drop = {
    "order_id is not null"  : "Drop rows missing order ID",
    "total_amount >= 0"     : "Drop rows with negative amount"
  }
)



# ── 2. SELECT DATA FROM SOURCE & WRITE TO THE TABLE (like the AS SELECT in SQL) ──
@dp.append_flow(target = "orders_streaming")
def ingest_orders():
  return (
    spark.readStream
      .format("cloudFiles")            # Auto Loader (source)
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "/checkpoints/orders_schema")
      .load("abfss://raw@storageaccount.dfs.core.windows.net/orders/")  # source path
      .select(
        col("order_id").cast("BIGINT"),
        col("customer_id").cast("BIGINT"),
        col("order_status"),
        col("order_date").cast("DATE"),
        col("region"),
        col("total_amount").cast("DECIMAL(10,2)"),
        col("created_at").cast("TIMESTAMP")
      )
      .filter("order_status != 'TEST'")
  )

