import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *



sys.path.append(
    "/Workspace/Users/jessepepple36@gmail.com/End-to-End-Sales-Data-Engineering-Pipeline-Azure-Databricks-DLT-CI-CD-/dab_salesproject/prod/files/utils/"
)

from Silver_Transformations import Transformations  


# ---------------------------
# Spark fixture
# ---------------------------
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark


# ---------------------------
# drop_cols
# ---------------------------
def test_drop_cols(spark):
    data = [("Alice", 1, "UK"), ("Bob", 2, "US")]
    df = spark.createDataFrame(data, ["name", "age", "country"])

    transformer = Transformations(df)
    result = transformer.drop_cols("age", "country")

    assert "age" not in result.columns
    assert "country" not in result.columns
    assert "name" in result.columns


# ---------------------------
# drop_data
# ---------------------------
def test_drop_data(spark):
    data = [("Alice", 1, "UK")]
    df = spark.createDataFrame(data, ["name", "age", "country"])

    transformer = Transformations(df)
    result = transformer.drop_data("age")

    assert "age" not in result.columns
    assert "name" in result.columns
    assert "country" in result.columns


# ---------------------------
# rename_cols
# ---------------------------
def test_rename_cols(spark):
    data = [("Alice", 1)]
    df = spark.createDataFrame(data, ["name", "age"])

    transformer = Transformations(df)
    result = transformer.rename_cols("age", "years")

    assert "age" not in result.columns
    assert "years" in result.columns
    assert "name" in result.columns


# ---------------------------
# drop_duplicates
# ---------------------------
def test_drop_duplicates(spark):
    data = [("Alice", 1), ("Alice", 1), ("Bob", 2)]
    df = spark.createDataFrame(data, ["name", "id"])

    transformer = Transformations(df)
    result = transformer.drop_duplicates(["name"])

    assert result.count() == 2
    assert "name" in result.columns
    assert "id" in result.columns


# ---------------------------
# cast_Int
# ---------------------------
def test_cast_Int(spark):
    data = [("Alice", "10"), ("Bob", "20")]
    df = spark.createDataFrame(data, ["name", "age"])

    transformer = Transformations(df)
    result = transformer.cast_Int(["age"])

    assert isinstance(result.schema["age"].dataType, IntegerType)
    assert result.filter(col("age") == 10).count() == 1
    assert result.filter(col("age") == 20).count() == 1


# ---------------------------
# add_updatedtimestamp
# ---------------------------
def test_add_updatedtimestamp(spark):
    data = [("Alice", 1)]
    df = spark.createDataFrame(data, ["name", "age"])

    transformer = Transformations(df)
    result = transformer.add_updatedtimestamp("updated_ts")

    assert "updated_ts" in result.columns
    assert result.filter(col("updated_ts").isNull()).count() == 0
