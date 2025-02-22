from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, expr
from pyspark.sql.window import Window

# Initialize Spark

spark = SparkSession.builder \
    .appName("Nifty50") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0") \
    .getOrCreate()


gcs_input_path = "gs://nsestock/raw/nifty50data_*.parquet"
df = spark.read.parquet(gcs_input_path)
# Create Date Dimension Table
dim_date = df.select("date").distinct()
dim_date = dim_date.withColumn("month_name", date_format(col("date"), "MMMM"))
dim_date = dim_date.withColumn("day", date_format(col("date"), "d").cast("int"))
dim_date = dim_date.withColumn("month", date_format(col("date"), "M").cast("int"))
dim_date = dim_date.withColumn("year", date_format(col("date"), "yyyy").cast("int"))

# Create Fact Table
fact_table = df.select("symbol", "date", "previousClose", "open", "close", "buyQty", "sellQty")
fact_table = fact_table.withColumn("trade_volume", expr("buyQty + sellQty"))

# Create Company Dimension Table
dim_company = df.select("symbol", "companyName", "industry").distinct()

# Configure BigQuery Output
bq_dataset = "nsestockanalysis.stock_dataset"

fact_table.write \
    .format("bigquery") \
    .option("table", f"{bq_dataset}.stock_data_staging") \
    .option("temporaryGcsBucket", "nsestock")\
    .mode("overwrite") \
    .save()
dim_company.write \
    .format("bigquery") \
    .option("table", f"{bq_dataset}.company_staging") \
    .option("temporaryGcsBucket", "nsestock")\
    .mode("overwrite") \
    .save()

dim_date.write \
    .format("bigquery") \
    .option("table", f"{bq_dataset}.date_staging") \
    .option("temporaryGcsBucket", "nsestock")\
    .mode("overwrite") \
    .save()

spark.stop()
