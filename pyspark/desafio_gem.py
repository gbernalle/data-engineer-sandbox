from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType,DateType
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Treinamento PySpark").getOrCreate()

custom_schema = StructType([
    StructField("Date",DateType(),True),
    StructField("Platform",StringType(),True),
    StructField("Product_Category",StringType(),True),
    StructField("Model_Name",StringType(),True),
    StructField("Condition",StringType(),True),
    StructField("Launch_Price_USD",IntegerType(),True),
    StructField("Launch_Price_INR",IntegerType(),True),
    StructField("Current_Price_USD",DoubleType(),True),
    StructField("Current_Price_INR",DoubleType(),True),
    StructField("Discount_Pct",DoubleType(),True)
  ])

df_apple_products = spark.read.csv(
    "./database/apple_products_pricing_2020_2026.csv",
    schema=custom_schema,
    header=True
  )

df_sales_transactions = spark.read.csv(
  "./database/apple_sales_transactions.csv",
  inferSchema=True,
  header=True
) 

# Bloco 1 de testes
def clean_and_filter_new_products(df):
  df_transformed = (
    df
    .dropna(subset=['Current_Price_USD'])
    .filter(F.col('Condition') == 'New')
  )
  return df_transformed
  
# clean_and_filter_new_products(df_apple_products).show()

def dicount_pct_greater_than_15(df):
  df_transformed = (
    df
    .filter(F.col("Discount_Pct") > 15.0)
  )
  return df_transformed

# dicount_pct_greater_then_15(df_apple_products).show()

# Bloco 2 de testes
def combine_dataframes(df_pricing,df_sales):
  df_combined = df_pricing.join(
    df_sales,
    on=["Model_Name"],
    how="left"
  )
  return df_combined

df_combined = combine_dataframes(df_pricing=df_apple_products,df_sales=df_sales_transactions)
# combine_dataframes(df_pricing=df_apple_products,df_sales=df_sales_transactions).show()

def average_price_per_product(df):
  df_average = (
    df
    .groupBy("Product_Category")
    .agg(
      F.round(F.avg("Current_Price_USD"),2).alias("Avg_Price_USD")
    )   
  )
  return df_average

# average_price_per_product(df_apple_products).show()

def revenue_per_store_USD(df):
  df_sales = (
    df
    .groupBy("Store_ID")
    .agg(
      F.round(
        F.sum(
          F.col("Current_Price_USD") * F.col("Units_Sold")
        ),2
      ).alias("Total_Revenue_USD")
    )
  )
  return df_sales

#revenue_per_store_USD(df_combined).show()

# Bloco 3

def most_cheaper_product(df):
  df_ranked = (
    df
    .withColumn(
      "Ranking",
      F.dense_rank().over(
        Window
        .partitionBy("Product_Category")
        .orderBy(F.col("Current_Price_USD").desc())
      )
    )
  )
  return df_ranked

# most_cheaper_product(df_apple_products).show()

def units_sold_per_time(df):
  df_filtered = (
    df
    .withColumn(
      "Cumulative_Units_Sold",
      F.sum("Units_Sold").over(
        Window
        .partitionBy("Model_Name")
        .orderBy("Date")
      )
    )
  )
  return df_filtered

#units_sold_per_time(df_sales_transactions).show()

def diference_cheaper_per_expensive(df):
  df_filtered = (
    df
    .withColumn(
      "Diff_Price_Launch",
      F.lead("Launch_Price_USD")
      .over(
        Window
        .partitionBy("Product_Category")
        .orderBy(F.col("Launch_Price_USD"))
      ) - F.col("Launch_Price_USD")
    )
  )

  return df_filtered

# diference_cheaper_per_expensive(df_apple_products).show()

def qt_sales_per_product_model(df):
  df_transformed = (
    df
    .withColumn(
      "Qt_Sales",
      F.lag("Units_Sold")
      .over(
        Window
        .partitionBy("Model_Name")
        .orderBy("Transaction_ID")
      )    
    )
  )
  return df_transformed

# qt_sales_per_product_model(df_sales_transactions).show()

def models_not_found(df_price, def_sales):
  df_transformed = (
    def_sales
    .join(
      df_price,
      on=["Model_Name"],
      how="left_anti"
    )
    .select("Model_Name")
    .distinct()
  )
  return df_transformed


#models_not_found(df_price=df_apple_products,def_sales=df_sales_transactions).show()

def get_top_selling_model_per_category(df_pricing, df_sales):
  df_grouped = (
    df_pricing
    .join(
      df_sales,
      on=["Model_Name"],
      how="inner"
    )
    .groupBy("Product_Category", "Model_Name")
    .agg(
        F.sum("Units_Sold").alias("Total_Units_Sold")
    )
  )

  df_final = (
        df_grouped
        .withColumn(
          "Ranking", 
          F.dense_rank().over(
            Window
            .partitionBy("Product_Category")
            .orderBy(
              F.col("Total_Units_Sold")
              .desc()
            )
          )
        )
        .filter(F.col("Ranking") == 1)
        .select("Product_Category", "Model_Name", "Total_Units_Sold")
    )

  return df_final

#get_top_selling_model_per_category(df_pricing=df_apple_products, df_sales=df_sales_transactions).show()