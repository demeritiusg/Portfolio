from pyspark.sql import SparkSession
import os

def main():
    spark = {
        SparkSession.builder.appName("S3CustomerIngest")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.aws.auth.*********")
        .getOrCreate()
    }
    
    input_path = "my s3 bucket path"
    output_path = "my output path"
    
    df = spark.read.option("header", "true").csv(input_path)
    df_clean = df.dropna(subset=["customer_id"])
    
    df_clean.write.mode("overwrite").parquet(output_path)
    spark.stop()
    
if __name__ == "__main__":
    main()