from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import current_date

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

df = (
    spark.read
    .format("json")
    .load("s3://raw-bucket/events/")
)

df = df.withColumn(
    "ingestion_date",
    current_date()
)

df.write \
  .format("delta") \
  .mode("append") \
  .partitionBy("ingestion_date") \
  .save("s3://lake/bronze/events/")
