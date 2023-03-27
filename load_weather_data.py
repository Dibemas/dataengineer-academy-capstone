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
import botocore 
import botocore.session 
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 
import json


S3_PATH = "s3a://dataminded-academy-capstone-resources/raw/open_aq/"
LOCAL_PATH =  "raw" # "data_part_1.json"
SNOWFLAKE_KEY = "snowflake/capstone/login"
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

def read_data_s3():
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.2").config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain").getOrCreate()
    return spark.read.option("multiline","true").json(S3_PATH)

def read_data_local(path: Path):
    print("reading from " + str(path))
    spark = SparkSession.builder.getOrCreate()
    return spark.read.json(
        str(path)
    )


def read_data_raw_from_s3():
    path_to_exercises = Path(__file__).parents[0]
    target_dir = path_to_exercises / "resources" / "raw"
    df = read_data_s3()
    df.write.json(
        path=str(target_dir),
        mode="overwrite"
    )
  

def get_snowflake_credentials():
    client = botocore.session.get_session().create_client('secretsmanager')
    cache_config = SecretCacheConfig()
    cache = SecretCache( config = cache_config, client = client)
    return json.loads(cache.get_secret_string(SNOWFLAKE_KEY))


def retrieve_and_clean_data():
    path_to_exercises = Path(__file__).parents[0]
    resources_dir = path_to_exercises / "resources"
    df = read_data_local(resources_dir / LOCAL_PATH)
    df.printSchema()
    df.show()
    df = df.withColumn("lat", psf.col("coordinates.latitude")).withColumn("lon", psf.col("coordinates.longitude"))
    df = df.withColumn("localdate_as_string", psf.col("date.local")).withColumn("utc", psf.col("date.utc"))
    df = df.withColumn("localdate", psf.to_timestamp("localdate_as_string"))
    df = df.drop("coordinates", "date", "localdate_as_string")
    df.printSchema()
    df.show()

    snowflake_credentials = get_snowflake_credentials()
    
    sfOptions = {'sfURL': "https://" + snowflake_credentials['URL'],
        "sfUser": snowflake_credentials['USER_NAME'],
        "sfPassword": snowflake_credentials['PASSWORD'],
        "sfDatabase": snowflake_credentials['DATABASE'],
        "sfSchema": "NELE@PXL",
        "sfWarehouse": snowflake_credentials['WAREHOUSE'],
        "parallelism": "64"
    }

    df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "nele_weather_data").mode("overwrite").save()

if __name__ == "__main__":
    read_data_raw_from_s3()
    retrieve_and_clean_data()
    # print(get_snowflake_credentials())

   