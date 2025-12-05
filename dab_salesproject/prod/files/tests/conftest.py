import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from transformations import Transformations
from pyspark.sql.functions import col

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("unit-tests") \
        .master("local[*]") \
        .getOrCreate()

@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField("customer_code", StringType(), True),
        StructField("age", StringType(), True),
        StructField("city", StringType(), True)
    ])

    data = [
        ("C001", "25", "London"),
        ("C002", "30", "London"),
        ("C002", "30", "London"),  # duplicate
    ]

    return spark.createDataFrame(data, schema)


# TEST: drop_cols()

def test_drop_cols(sample_df):
    trans = Transformations(sample_df)
    result = trans.drop_cols("city")

    assert "city" not in result.columns
    assert len(result.columns) == 2



# TEST: rename_cols()

def test_rename_cols(sample_df):
    trans = Transformations(sample_df)
    result = trans.rename_cols("customer_code", "customer_id")

    assert "customer_id" in result.columns
    assert "customer_code" not in result.columns



# TEST: drop_duplicates()

def test_drop_duplicates(sample_df):
    trans = Transformations(sample_df)
    result = trans.drop_duplicates(["customer_code", "age", "city"])

    assert result.count() == 2  # one duplicate removed


# TEST: cast_Int()

def test_cast_int(sample_df):
    trans = Transformations(sample_df)
    result = trans.cast_Int(["age"])

    assert dict(result.dtypes)["age"] == "int"



# TEST: add_updatedtimestamp()

def test_add_updatedtimestamp(sample_df):
    from pyspark.sql.functions import current_timestamp

    trans = Transformations(sample_df)
    result = trans.add_updatedtimestamp("last_updated")

    assert "last_updated" in result.columns
    assert dict(result.dtypes)["last_updated"] == "timestamp"



# MOCK TEST: read_Streams()

def test_read_streams_mock(monkeypatch, spark):

    def fake_stream_reader(*args, **kwargs):
        # return a simple DF instead of a streaming DF
        return spark.createDataFrame(
            [("C01", "Lagos")],
            ["id", "city"]
        )

    trans = Transformations()

    monkeypatch.setattr(
        trans,
        "read_Streams",
        lambda data_name, folder: fake_stream_reader()
    )

    df = trans.read_Streams("dummy", "dummy")

    assert df.count() == 1
    assert "city" in df.columns



# MOCK TEST: writeStream()

def test_write_stream_mock(monkeypatch, sample_df):

    trans = Transformations(sample_df)

    def fake_writer(*args, **kwargs):
        return sample_df  # pretend write succeeded

    monkeypatch.setattr(
        trans,
        "writeStream",
        lambda *args, **kwargs: sample_df
    )

    result = trans.writeStream("dummy", "folder")

    assert result.count() == sample_df.count()
