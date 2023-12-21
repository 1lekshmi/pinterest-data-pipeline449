# Databricks notebook source
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

from pyspark.sql.functions import *
import urllib

# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0a9b5b8a2ae5-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/user-0a9b5b8a2ae5-bucket"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# Checking if S3 bucket was mounted successfully

# COMMAND ----------

display(dbutils.fs.ls("/mnt/user-0a9b5b8a2ae5-bucket/topics/"))

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/user-0a9b5b8a2ae5-bucket/topics/0a9b5b8a2ae5.{}/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type).option("inferSchema", infer_schema).load(file_location.format("pin"))
df_geo = spark.read.format(file_type).option("inferSchema", infer_schema).load(file_location.format("geo"))
df_user = spark.read.format(file_type).option("inferSchema", infer_schema).load(file_location.format("user"))

display(df_pin)
display(df_geo)
display(df_user)


# COMMAND ----------


