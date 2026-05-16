from pyspark import pipelines as dp

dp.create_streaming_table(
  name    = "mycatalog.raw.orders_streaming",

)