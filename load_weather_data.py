from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as psf
from pyspark.sql.types import (
    BooleanType,
    DateType,
    StringType,
    StructField,
    StructType,
)
import time
from pathlib import Path


S3_PATH = "s3://dataminded-academy-capstone-resources/raw/open_aq/"
LOCAL_PATH = "data_part_1.json"

def read_data_s3(path: Path):
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2").config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain").getOrCreate()
    return spark.read.option("multiline","true").json(
        str(path)
    )

def read_data_local(path: Path):
    spark = SparkSession.builder.getOrCreate()
    return spark.read.option("multiline","true").json(
        str(path)
    )


if __name__ == "__main__":
    path_to_exercises = Path(__file__).parents[0]
    resources_dir = path_to_exercises / "resources"
    # df = read_data_s3(S3_PATH)
    df = read_data_local(resources_dir / LOCAL_PATH)
    df.printSchema()
    df = df.withColumn("lat", psf.col("coordinates.latitude")).withColumn("lon", psf.col("coordinates.longitude"))
    df = df.withColumn("localdate_as_string", psf.col("date.local")).withColumn("utc", psf.col("date.utc"))
    df = df.withColumn("localdate", psf.to_timestamp("localdate_as_string"))
    df = df.drop("coordinates", "date", "localdate_as_string")
    df.printSchema()
    df.show()