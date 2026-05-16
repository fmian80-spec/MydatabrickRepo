from pyspark import pipelines as dp

dp.create_streaming_table(
  name    = "workspace.default.orders_streaming"

)


@dp.append_flow(target = "workspace.default.orders_streaming")
def load_orders():
  return (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .load("/Volumes/mycatalog/raw/testv/")
  )