import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
expectations = {
    "rule 1" : "product_id IS NOT NULL"
}
@dlt.table(
    name = "dimproducts_stg",
    comment="Streaming products staging table from Silver layer"
)

def dimproducts_stg():
  df = spark.readStream.table("sales_project.silver.dim_products_enr")
  return df


dlt.create_streaming_table(
    name= "dimproducts",
    comment="SCD Type 2 Product Dimension",
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "product_id"},
    expect_all_or_fail= expectations
)

dlt.create_auto_cdc_flow(
    target = "dimproducts",
    source = "dimproducts_stg",
    keys = ["product_id"],
  sequence_by = "last_updated",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  once = False
)