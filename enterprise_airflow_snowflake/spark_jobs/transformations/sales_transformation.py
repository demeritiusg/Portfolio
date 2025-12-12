from pyspark.sql import SparkSession, functions as F

def main():
    spark = SparkSession.builder.appName("EMRSalesTransform").getOrCreate()

    input_path = "s3://your-bucket/processed/sales/"
    output_path = "s3://your-bucket/analytics/sales_aggregates/"

    df = spark.read.parquet(input_path)

    agg_df = (
        df.groupBy("region")
        .agg(F.sum("sales_amount").alias("total_sales"))
        .orderBy(F.desc("total_sales"))
    )

    agg_df.write.mode("overwrite").parquet(output_path)
    spark.stop()

if __name__ == "__main__":
    main()
