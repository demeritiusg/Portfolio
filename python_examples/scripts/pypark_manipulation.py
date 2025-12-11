from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Start Spark Session
spark = SparkSession.builder.appName('FlightAnalysis').getOrCreate()

# Define Schema
flight_schema = StructType([
	StructField("YEAR", IntegerType(), True),
	StructField("MONTH", IntegerType(), True),
	StructField("DAY", IntegerType(), True),
	StructField("DAY_OF_WEEK", IntegerType(), True),
	StructField("AIRFLINE", StringType(), True),
	StructField("FLIGHT_NUMBER", StringType(), True),
	StructField("ORIGIN_AIRPORT", StringType(), True),
	StructField("DESTINATION_AIRPORT", StringType(), True),
	StructField("DEPATURE_DELAY", IntegerType(), True),
	StructField("ARRIVAL_DELAY", IntegerType(), True),
])

flight_data_path = "data/flights.csv"
airline_lookup_path = "data/airlines.csv"

# Load Data
df = spark.read.csv(flight_data_path, header=True, schema=flight_schema)
airlines_df = spark.read.csv(airline_lookup_path, header=True, inferschema=True)

# Data Cleaning and Transformation

df_clean = (
	df.dropna(subset["DEPARTURE_DELAY", "ARRIVAL_DELAY"]) #remove missing delays
		.withColumnRenamed("ORIGIN_AIRPORT", "Origin")
		.withColumnRenamed("DESTINATION_AIRPORT", "Distination")
		.withColumnRenamed("DEPATURE_DELAY", "DepDelay")
		.withColumnRenamed("ARRIVAL_DELAY", "ArrDelay")
        .withColumn("TotalDelay", F.col("DepDelay") + F.col("ArrDelay"))
)

# Join with airline data
df_enriched = df_clean.join(airlines_df, on="AIRLINE", how="left")

# Aggregation, Average delay
df_avg_delay = (
	df_enriched.groupby("AIRLINE", "AIRLINE_NAME")
		.agg(F.avg("TotalDelay").alias("AvgDelay"))
)

# Rank Flights per Airfline
window_spec = Window.partitionBy("AIRLINE").orderBy(F.desc("TotalDelay"))
df_ranked = df_enriched.withColumn("Rank", F.rank().over(window_spec))

# Performance Optimization
df_ranked = df_ranked.repartition("AIRLINE").cache()

# Sort Results
df_avg_delay_sorted = df_avg_delay.orderBy("AvgDelay", ascending=False)

# Persist Results
df_avg_delay_sorted.write.mode(overwrite).parquet("output/avg_delay")
df_ranked.filter(F.col("Rank") <= 3) \
	.write.mode("overwrite").parquet("output/top_delayed_flights")

# Show Sample Output

print("Airlines with Highest Average Delay: ")
df_avg_delay_sorted.show(5, truncate=False)

print("Top 3 Delayed Flights per Airlines: ")
df_ranked.filter(F.col("Rank") <= 3).show(10, truncate=False)