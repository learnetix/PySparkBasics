from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

spark = SparkSession.builder \
.appName("SimpleETLPipeline") \
.getOrCreate()


schema = StructType([
StructField("customer_id", IntegerType(), True),
StructField("first_name", StringType(), True),
StructField("last_name", StringType(), True),
StructField("order_date", DateType(), True),
StructField("order_total", DoubleType(), True)
])


df = spark.read.option("header", True).schema(schema).csv("customer_orders.csv")

df_transformed = df.filter(df["order_total"] > 100) \
.withColumn("full_name", concat_ws(" ", df.first_name, df.last_name)) \
.select("customer_id", "full_name", "order_total") \
.fillna(0, subset=["order_total"])

df_transformed.show()

df_transformed.write.mode("overwrite").parquet("clean_customer_data")

spark.stop()
