
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth,
    when, lit
)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_PATH", "CURATED_PATH"]
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read raw JSON events from S3
raw_df = spark.read.json(args["RAW_PATH"])

# Clean and transform events
df = (
    raw_df
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("temperature", col("temperature").cast("double"))
    .filter(
        col("event_id").isNotNull()
        & col("sensor_id").isNotNull()
        & col("truck_id").isNotNull()
        & col("event_time").isNotNull()
        & col("temperature").isNotNull()
    )
    .dropDuplicates(["event_id"])
    .withColumn(
        "temperature_breach",
        when(col("temperature") > 5.0, lit(True)).otherwise(lit(False))
    )
    .withColumn("year", year(col("event_time")))
    .withColumn("month", month(col("event_time")))
    .withColumn("day", dayofmonth(col("event_time")))
)

# Write compressed Parquet files partitioned by date
(
    df.write
    .mode("append")
    .partitionBy("year", "month", "day")
    .option("compression", "snappy")
    .parquet(args["CURATED_PATH"])
)

job.commit()
