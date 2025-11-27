import dlt
from pyspark.sql.functions import *
expectations = {
    "rule 1" : "date_sk IS NOT NULL",
    "rule 2" : "date IS NOT NULL"
}
@dlt.table(
    name= "dimdate_stg"
)
def dimdate_stg():
  df = spark.readStream.table("sales_project.silver.dim_date_enr")
  return df

dlt.create_streaming_table(
    name= "dimdate",
    comment= "curated gold table",
    table_properties={
        "pipelines.autoOptimize.zOrdebyCols": "date_sk"
    },
    expect_all_or_fail= expectations
)

dlt.create_auto_cdc_flow(
    source= "dimdate_stg",
    target= "dimdate",
    keys= ["date_sk"],
    sequence_by = "last_updated",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  once = False

)