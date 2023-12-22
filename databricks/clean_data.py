# Databricks notebook source
# MAGIC %md
# MAGIC ## Cleaning Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframes

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

# MAGIC %md
# MAGIC ### Cleaning df_pin dataframe

# COMMAND ----------

# remove duplicates 
df_pin = df_pin.drop_duplicates()

# replacing empty entries and entries with no relevant data with None
df_pin = df_pin.replace({"No description available Story format" : None}, subset=["description"])
df_pin = df_pin.replace({"User Info Error" : None}, subset=["follower_count"])
df_pin = df_pin.replace({"Image src error." : None}, subset=["image_src"])
df_pin = df_pin.replace({"User Info Error" : None}, subset=["poster_name"])
df_pin = df_pin.replace({"N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e" : None}, subset=["tag_list"])
df_pin = df_pin.replace({"No Title Data Available" : None}, subset=["title"])

# transforming the follower_count column to make sure every entry is a number
df_pin = df_pin.withColumn("follower_count",regexp_replace("follower_count","k","000"))
df_pin = df_pin.withColumn("follower_count",regexp_replace("follower_count","M","000000"))

# casting follower_count column to int
df_pin = df_pin.withColumn("follower_count", df_pin["follower_count"].cast("int"))

# change the save_location column to only have the path
df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

# rename the index column
df_pin = df_pin.withColumnRenamed("index","ind")

# reordering the columns
df_pin = df_pin.select("ind","unique_id","title","description","follower_count","poster_name","tag_list","is_image_or_video","image_src","save_location","category")

display(df_pin)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning df_geo dataframe

# COMMAND ----------

# remove duplicates
df_geo = df_geo.drop_duplicates()

# coordinates column
df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))

# dropping latitude and logitude columns
df_geo = df_geo.drop("latitude","longitude")

# converting timestamp from a string to a timestamp
df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))

# reordering the columns
df_geo = df_geo.select("ind","country","coordinates","timestamp")

display(df_geo)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning df_user dataframe

# COMMAND ----------

# removing duplicates
df_user = df_user.drop_duplicates()

# a new user_name column
df_user = df_user.withColumn("user_name", concat("first_name", lit(" "), "last_name"))

# dropping first_name and last_name
df_user = df_user.drop("first_name", "last_name")

# converting date_joined to a timestamp
df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))

# reordering the columns
df_user = df_user.select("ind", "user_name", "age", "date_joined")

display(df_user)


# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Query

# COMMAND ----------

# creating temporary views
df_pin.createOrReplaceTempView("pin")
df_geo.createOrReplaceTempView("geo")
df_user.createOrReplaceTempView("user")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the most popular category in each country

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT country, category, category_count
# MAGIC FROM(
# MAGIC     SELECT country, category, COUNT(category) AS category_count,
# MAGIC     ROW_NUMBER() OVER(PARTITION BY country ORDER BY COUNT(category) DESC) AS row_number
# MAGIC     FROM geo
# MAGIC     JOIN pin ON pin.ind = geo.ind
# MAGIC     GROUP BY country, category
# MAGIC )
# MAGIC WHERE row_number = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find which was the most popular category each year

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT post_year, category, category_count FROM (
# MAGIC   SELECT year(timestamp) AS post_year, category, COUNT(category) AS category_count,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY year(timestamp) ORDER BY COUNT(category) DESC) AS row_number
# MAGIC   FROM geo 
# MAGIC   JOIN pin on pin.ind = geo.ind
# MAGIC   GROUP BY year(timestamp), category
# MAGIC )
# MAGIC WHERE 2018 <= post_year AND post_year <= 2022 AND row_number = 1;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the user with most followers in each country

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT country, poster_name, max_follower_count FROM (
# MAGIC   SELECT country, poster_name, MAX(follower_count) AS max_follower_count,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY country ORDER BY MAX(follower_count) DESC) as row_number
# MAGIC   FROM pin 
# MAGIC   JOIN geo ON geo.ind = pin.ind
# MAGIC   GROUP BY country,poster_name
# MAGIC )
# MAGIC WHERE row_number = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT country, MAX(follower_count) AS max_follower_count
# MAGIC FROM pin
# MAGIC JOIN geo ON geo.ind = pin.ind
# MAGIC GROUP BY country
# MAGIC ORDER BY MAX(follower_count) DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the most popular category for different age groups

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH age_table AS (
# MAGIC   SELECT age, pin.category AS category
# MAGIC   FROM user
# MAGIC   JOIN pin ON pin.ind = user.ind
# MAGIC ), age_group_table AS (
# MAGIC   SELECT CASE 
# MAGIC     WHEN 18 <= age AND age <= 24 THEN '18-24'
# MAGIC     WHEN 25 <= age AND age <= 35 THEN '25-35'
# MAGIC     WHEN 36 <= age AND age <= 50 THEN '36-50'
# MAGIC     WHEN 50 < age THEN '50+'
# MAGIC   END AS age_group,
# MAGIC   category, COUNT(category) AS category_count
# MAGIC   FROM age_table
# MAGIC   GROUP BY age_group, category
# MAGIC )
# MAGIC SELECT age_group, category, category_count
# MAGIC FROM (
# MAGIC   SELECT  age_group, 
# MAGIC           category, 
# MAGIC           category_count,
# MAGIC           ROW_NUMBER() OVER (PARTITION BY age_group ORDER BY category_count DESC) AS row_number
# MAGIC   FROM 
# MAGIC     age_group_table
# MAGIC )
# MAGIC WHERE row_number = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pin;
# MAGIC -- SELECT * FROM geo;
# MAGIC -- SELECT * FROM user;
