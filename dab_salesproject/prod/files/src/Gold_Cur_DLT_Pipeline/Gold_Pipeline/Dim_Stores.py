import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
expectations = {
    "rule 1" : "store_id IS NOT NULL"
}
@dlt.table(
    name = "dimstores_stg",
    comment="Streaming stores staging table from Silver layer"
)

def dimstoress_stg():
  df = spark.readStream.table("sales_project.silver.dim_stores_enr")
  return df


dlt.create_streaming_table(
    name= "dimstores",
    comment="SCD Type 2 Store Dimension",
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "store_id"},
    expect_all_or_fail= expectations
)

dlt.create_auto_cdc_flow(
    target = "dimstores",
    source = "dimstores_stg",
    keys = ["store_id"],
  sequence_by = "last_updated",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  once = False
)