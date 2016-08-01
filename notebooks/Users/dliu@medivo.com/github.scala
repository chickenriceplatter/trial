// Databricks notebook source exported at Mon, 1 Aug 2016 18:44:36 UTC
val list = List((1, "one"),(2, "two"),(3,"three"),(4,"four"),(5,"five"))

val rdd = sc.parallelize(list)

val df = rdd.toDF("index", "number")

val new_df = df.withColumn("another", $"index" + 1)

// COMMAND ----------

new_df.show()

// COMMAND ----------

import org.apache.spark.sql.functions._

val stats = new_df.agg(max("index"), min("another"))

// COMMAND ----------

stats.show()

// COMMAND ----------

