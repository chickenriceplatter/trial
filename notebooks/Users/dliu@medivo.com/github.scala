// Databricks notebook source exported at Fri, 29 Jul 2016 22:12:29 UTC

val list = List((1, "one"),(2, "two"),(3,"three"),(4,"four"),(5,"five"))

val rdd = sc.parallelize(list)

val df = rdd.toDF("index", "number")

val new_df = df.withColumn("another", $"index" + 1)
