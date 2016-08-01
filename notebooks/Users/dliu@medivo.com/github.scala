// Databricks notebook source exported at Mon, 1 Aug 2016 18:19:01 UTC
val data_location = "http://s3.amazonaws.com/assets.datacamp.com/course/Kaggle/train.csv"

val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load(data_location)

// COMMAND ----------

sqlContext

// COMMAND ----------

