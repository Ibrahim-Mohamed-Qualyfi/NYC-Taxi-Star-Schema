# Databricks notebook source
from pyspark.sql.functions import input_file_name, current_timestamp, col

spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net", "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

landing_data_path_2010 = "abfss://landing-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2010-*/"
landing_data_path_2014 = "abfss://landing-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-*/"
landing_data_path_2019 = "abfss://landing-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-*/"

df_2010 = spark.read.csv(landing_data_path_2010, header=True, inferSchema=True)
df_2014 = spark.read.csv(landing_data_path_2014, header=True, inferSchema=True)
df_2019 = spark.read.csv(landing_data_path_2019, header=True, inferSchema=True)

df_2010 = df_2010.withColumn("file_name", input_file_name()) \
    .withColumn("created_on", current_timestamp())

df_2014 = df_2014.withColumn("file_name", input_file_name()) \
    .withColumn("created_on", current_timestamp())

df_2019 = df_2019.withColumn("file_name", input_file_name()) \
    .withColumn("created_on", current_timestamp())

df_2014 = df_2014.withColumnRenamed(" pickup_datetime", "pickup_datetime").withColumnRenamed(" dropoff_datetime", "dropoff_datetime").withColumnRenamed(" passenger_count", "passenger_count").withColumnRenamed(" trip_distance", "trip_distance").withColumnRenamed(" pickup_longitude", "pickup_longitude").withColumnRenamed(" pickup_latitude", "pickup_latitude").withColumnRenamed(" rate_code", "rate_code").withColumnRenamed(" store_and_fwd_flag", "store_and_fwd_flag").withColumnRenamed(" dropoff_longitude", "dropoff_longitude").withColumnRenamed(" dropoff_latitude", "dropoff_latitude").withColumnRenamed(" payment_type", "payment_type").withColumnRenamed(" fare_amount", "fare_amount").withColumnRenamed(" surcharge", "surcharge").withColumnRenamed(" mta_tax", "mta_tax").withColumnRenamed(" tip_amount", "tip_amount").withColumnRenamed(" tolls_amount", "tolls_amount").withColumnRenamed(" total_amount", "total_amount")

df_2019 = df_2019.withColumnRenamed("VendorID", "vendor_id")

bronze_data_path = "abfss://bronze-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/"

df_2010.write.format("delta").partitionBy("vendor_id").mode("overwrite").save(bronze_data_path + "yellow_tripdata_2010")
df_2014.write.format("delta").partitionBy("vendor_id").mode("overwrite").save(bronze_data_path + "yellow_tripdata_2014")
df_2019.write.format("delta").partitionBy("vendor_id").mode("overwrite").save(bronze_data_path + "yellow_tripdata_2019")

