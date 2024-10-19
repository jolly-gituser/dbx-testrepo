# Databricks notebook source
from pyspark.sql import SparkSession

# Create a simple DataFrame
spark = SparkSession.builder.appName("Test").getOrCreate()
data = [("Alice", 29), ("Bob", 31), ("Cathy", 23)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show DataFrame
df.show()

# Save DataFrame as a table
df.write.saveAsTable("people")
