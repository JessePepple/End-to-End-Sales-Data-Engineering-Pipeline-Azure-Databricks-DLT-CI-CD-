import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
expectations = {
    "rule 1" : "sales_id IS NOT NULL"
}
@dlt.table(
    name = "factsales_stg",
    comment="Streaming of our Fact Table staging table from Silver layer"
)

def dimfacts_stg():
  df = spark.readStream.table("sales_project.silver.fact_sales_enr")
  return df


dlt.create_streaming_table(
    name= "factsales",
    comment="SCD Type 2 Facts Dimension",
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "sales_id"},
    expect_all_or_fail= expectations
)

dlt.create_auto_cdc_flow(
    target = "factsales",
    source = "factsales_stg",
    keys = ["sales_id"],
  sequence_by = "last_updated",
  stored_as_scd_type = "1",
  track_history_except_column_list = None,
  once = False
)