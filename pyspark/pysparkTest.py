import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType 

import pyspark.sql.functions as F

def clean_and_filter_new_products(df):
  df_transformed = (
    df
    .dropna(subset=['Current_Price_USD'])
    .filter(F.col('Condition') == 'New')
  )
  return df_transformed

@pytest.fixture(scope="session")
def spark():
  return SparkSession.builder.appName("TesteUnitarios").getOrCreate()

def test_clean_and_filter_new_products(spark):
  fake_data = [
    (None,"New"),
    (2000.0,"Renew"),
    (5000.0,"New")
  ]

  custom_schema = StructType([
    StructField("Current_Price_USD", DoubleType(), True),
    StructField("Condition", StringType(), True)
  ])

  df_in = spark.createDataFrame(fake_data,custom_schema)

  df_out = clean_and_filter_new_products(df_in)
  df_result = df_out.collect()

  assert len(df_result) == 1
  assert df_result[0]["Current_Price_USD"] == 5000
  assert df_result[0]["Condition"] == "New"