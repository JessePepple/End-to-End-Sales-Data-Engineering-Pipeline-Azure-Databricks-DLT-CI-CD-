import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
expectations = {
    "rule 1" : "customer_id IS NOT NULL"
}
@dlt.table(
    name = "dimcustomers_stg",
    comment="Streaming staging table from Silver layer"
)

def dimcustomers_stg():
  df = spark.readStream.table("sales_project.silver.dim_customer_enr")
  return df


dlt.create_streaming_table(
    name= "dimcustomer",
    comment="SCD Type 2 Customer Dimension",
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "customer_id"},
    expect_all_or_fail= expectations
)

dlt.create_auto_cdc_flow(
    target = "dimcustomer",
    source = "dimcustomers_stg",
    keys = ["customer_id"],
  sequence_by = "last_updated",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  once = False
)