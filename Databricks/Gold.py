# Databricks notebook source
from pyspark.sql.functions import expr, monotonically_increasing_id
from pyspark.sql.types import StringType

spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net", "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

silver_data_path = "abfss://silver-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/"
gold_data_path = "abfss://gold-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/"

silver_df = spark.read.format("delta").load(silver_data_path)

dim_time = silver_df.select(
    "pickup_datetime",
    "dropoff_datetime"
)

dim_payment_type = silver_df.select(
    "payment_type"
)

dim_payment_type = dim_payment_type.withColumn(
    "payment_type",
    expr(
        "CASE " +
        "WHEN lower(payment_type) IN ('credit', 'cre', 'crd', '1') THEN 'Credit card' " +
        "WHEN lower(payment_type) IN ('cash', 'csh', 'cas', '2') THEN 'Cash' " +
        "WHEN lower(payment_type) IN ('no', 'no charge', 'noc', '3') THEN 'No charge' " +
        "WHEN lower(payment_type) IN ('disput', 'dis', '4') THEN 'Dispute' " +
        "WHEN lower(payment_type) IN ('unknown', 'unk', '5') THEN 'Unknown' " +
        "WHEN lower(payment_type) IN ('voided trip', 'voi', 'voided', '6') THEN 'Voided Trip' " +
        "ELSE NULL END"
    ).cast(StringType())
)

dim_vendors = silver_df.select(
    "vendor_id"
)

dim_vendors = dim_vendors.withColumn(
    "vendor",
    expr(
        "CASE " +
        "WHEN vendor_id = 1 THEN 'CMT' " +
        "WHEN vendor_id = 2 THEN 'VTS' " +
        "WHEN vendor_id = 4 THEN 'DDS' " +
        "ELSE vendor_id END"
    ).cast(StringType())
)

dim_rate_code = silver_df.select(
    "rate_code"
)

dim_rate_code = dim_rate_code.withColumn(
    "rate_code_description",
    expr(
        "CASE " +
        "WHEN rate_code = 1 THEN 'Standard rate' " +
        "WHEN rate_code = 2 THEN 'JFK' " +
        "WHEN rate_code = 3 THEN 'Newark' " +
        "WHEN rate_code = 4 THEN 'Nassau or Westchester' " +
        "WHEN rate_code = 5 THEN 'Negotiated fare' " +
        "WHEN rate_code = 6 THEN 'Group ride' " +
        "ELSE NULL END"
    ).cast(StringType())
)

fact_trips = silver_df.select(
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "file_name",
    "created_on"
)

fact_trips = fact_trips.withColumn("trip_id", monotonically_increasing_id())


# COMMAND ----------

spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net", "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

dim_time.write.format("delta").mode("overwrite").save(gold_data_path + "dim_time")
dim_payment_type.write.format("delta").mode("overwrite").save(gold_data_path + "dim_payment_type")
dim_vendors.write.format("delta").mode("overwrite").save(gold_data_path + "dim_vendors")
dim_rate_code.write.format("delta").mode("overwrite").save(gold_data_path + "dim_rate_code")
fact_trips.write.format("delta").mode("overwrite").save(gold_data_path + "fact_trips")
