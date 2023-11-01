# Databricks notebook source
spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net", "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

dbutils.fs.rm("abfss://landing-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/", True)
dbutils.fs.rm("abfss://bronze-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/", True)
dbutils.fs.rm("abfss://silver-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/", True)
dbutils.fs.rm("abfss://gold-ibrahim@datacohortworkspacelabs.dfs.core.windows.net/", True)
