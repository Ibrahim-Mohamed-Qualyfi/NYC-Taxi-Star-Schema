# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

silver_schema = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("rate_code", IntegerType(), True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("file_name", StringType(), True),
    StructField("created_on", TimestampType(), True)
])

# COMMAND ----------

# MAGIC %pip install geopandas

# COMMAND ----------

import geopandas as gpd
from shapely.geometry import Point

zones = gpd.read_file('file:///Workspace/Users/ibrahim.mohamed@qualyfi.co.uk/location_data.geojson')

point = Point(-73.982101999999998,40.736289999999997)

for index, zone in zones.iterrows():
    if zone['geometry'].contains(point):
        print(f'The point is within {zone["borough"]} borough.')
        print(f'Location ID: {zone["location_i"]}')
        print(f'Location ID: {zone["zone"]}')



# COMMAND ----------

from pyspark.sql.functions import udf, col, lit, when, lower, expr, coalesce
from pyspark.sql.types import IntegerType
import geopandas as gpd
from shapely.geometry import Point

spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net", "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

zones = gpd.read_file('file:///Workspace/Users/ibrahim.mohamed@qualyfi.co.uk/location_data.geojson')

@udf(IntegerType())
def map_location_udf(longitude, latitude):
    point = Point(longitude, latitude)
    for index, zone in zones.iterrows():
        if zone['geometry'].contains(point):
            return int(zone['location_i']) 

    return None

bronze_data_path = "abfss://bronze-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/"

silver_df_2010 = spark.read.format("delta").load(bronze_data_path + "yellow_tripdata_2010/")
silver_df_2014 = spark.read.format("delta").load(bronze_data_path + "yellow_tripdata_2014")
silver_df_2019 = spark.read.format("delta").load(bronze_data_path + "yellow_tripdata_2019/")

silver_df_2010 = silver_df_2010.withColumn("pickup_location_id", map_location_udf(col("pickup_longitude"), col("pickup_latitude")))
silver_df_2010 = silver_df_2010.withColumn("dropoff_location_id", map_location_udf(col("dropoff_longitude"), col("dropoff_latitude")))
silver_df_2010 = silver_df_2010.filter(col("total_amount").isNotNull() & (col("total_amount") > 0)) \
    .filter(col("trip_distance") > 0)
silver_df_2010 = silver_df_2010.drop("pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "store_and_fwd_flag")
silver_df_2010 = silver_df_2010.withColumn("extra", lit(0))
silver_df_2010 = silver_df_2010.withColumn("fare_amount", col("fare_amount").cast("double"))
silver_df_2010 = silver_df_2010.withColumn("extra", col("extra").cast("double"))
silver_df_2010 = silver_df_2010.withColumn(
    "vendor_id",
    expr(
        "CASE " +
        "WHEN vendor_id = 'CMT' THEN 1 " +
        "WHEN vendor_id = 'VTS' THEN 2 " +
        "WHEN vendor_id = 'DDS' THEN 4 " +
        "ELSE vendor_id END"
    ).cast(IntegerType())
)
silver_df_2010 = silver_df_2010.withColumn(
    "payment_type",
    expr(
        "CASE " +
        "WHEN lower(payment_type) IN ('credit', 'cre', 'crd', '1') THEN 1 " +
        "WHEN lower(payment_type) IN ('cash', 'csh', 'cas', '2') THEN 2 " +
        "WHEN lower(payment_type) IN ('no', 'no charge', 'noc', '3') THEN 3 " +
        "WHEN lower(payment_type) IN ('disput', 'dis', '4') THEN 4 " +
        "WHEN lower(payment_type) IN ('unknown', 'unk', '5') THEN 5 " +
        "WHEN lower(payment_type) IN ('voided trip', 'voi', 'voided', '6') THEN 6 " +
        "ELSE payment_type END"
    ).cast(IntegerType())
)

silver_df_2014 = silver_df_2014.withColumn("pickup_location_id", map_location_udf(col("pickup_longitude"), col("pickup_latitude")))
silver_df_2014 = silver_df_2014.withColumn("dropoff_location_id", map_location_udf(col("dropoff_longitude"), col("dropoff_latitude")))
silver_df_2014 = silver_df_2014.filter(col("total_amount").isNotNull() & (col("total_amount") > 0)) \
    .filter(col("trip_distance") > 0)
silver_df_2014 = silver_df_2014.drop("pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "store_and_fwd_flag")
silver_df_2014 = silver_df_2014.withColumn("extra", lit(0))
silver_df_2014 = silver_df_2014.withColumn("extra", col("extra").cast("double"))
silver_df_2014 = silver_df_2014.withColumn(
    "vendor_id",
    expr(
        "CASE " +
        "WHEN vendor_id = 'CMT' THEN 1 " +
        "WHEN vendor_id = 'VTS' THEN 2 " +
        "WHEN vendor_id = 'DDS' THEN 4 " +
        "ELSE vendor_id END"
    ).cast(IntegerType())
)
silver_df_2014 = silver_df_2014.withColumn(
    "payment_type",
    expr(
        "CASE " +
        "WHEN lower(payment_type) IN ('credit', 'cre', 'crd', '1') THEN 1 " +
        "WHEN lower(payment_type) IN ('cash', 'csh', 'cas', '2') THEN 2 " +
        "WHEN lower(payment_type) IN ('no', 'no charge', 'noc', '3') THEN 3 " +
        "WHEN lower(payment_type) IN ('disput', 'dis', '4') THEN 4 " +
        "WHEN lower(payment_type) IN ('unknown', 'unk', '5') THEN 5 " +
        "WHEN lower(payment_type) IN ('voided trip', 'voi', 'voided', '6') THEN 6 " +
        "ELSE payment_type END"
    ).cast(IntegerType())
)


silver_df_2019 = silver_df_2019.withColumn("surcharge", coalesce(col("congestion_surcharge"), lit(0)) + coalesce(col("improvement_surcharge"), lit(0)))
silver_df_2019 = silver_df_2019.drop("congestion_surcharge", "improvement_surcharge", "store_and_fwd_flag")
silver_df_2019 = silver_df_2019.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime").withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime").withColumnRenamed("RatecodeID", "rate_code").withColumnRenamed("PULocationID", "pickup_location_id").withColumnRenamed("DOLocationID", "dropoff_location_id")


# COMMAND ----------

silver_df_2010 = silver_df_2010.select(*[col(field.name) for field in silver_schema.fields])
silver_df_2014 = silver_df_2014.select(*[col(field.name) for field in silver_schema.fields])
silver_df_2019 = silver_df_2019.select(*[col(field.name) for field in silver_schema.fields])

# COMMAND ----------

sampled_df_2010 = silver_df_2010.sample(0.001)
sampled_df_2014 = silver_df_2014.sample(0.001)

# COMMAND ----------

spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net", "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

silver_data_path = "abfss://silver-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/"

silver_df = sampled_df_2010.union(sampled_df_2014)

silver_df.write.format("delta").partitionBy("pickup_location_id").mode("overwrite").save(silver_data_path)

# COMMAND ----------

spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net", "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

silver_data_path = "abfss://silver-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/"

silver_df_2019.write.format("delta").partitionBy("pickup_location_id").mode("append").save(silver_data_path)
