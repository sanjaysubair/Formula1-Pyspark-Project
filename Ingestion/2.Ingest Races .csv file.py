# Databricks notebook source
# MAGIC %md
# MAGIC Read the races file
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

# COMMAND ----------

races_schema = StructType(
    fields = 
    [
    StructField("raceId", IntegerType()),
    StructField("year", IntegerType()),
    StructField("round", IntegerType()),
    StructField("circuitId", IntegerType()),
    StructField("name", StringType()),
    StructField("date", StringType()),
    StructField("time", StringType())
    ]  
)

# COMMAND ----------

races_df = spark.read.schema(races_schema).options(Header=True).csv("abfss://raw@sanjayformula1dl2.dfs.core.windows.net/races.csv")
display(races_df)

# COMMAND ----------

races_renamed_df = races_df.withColumnsRenamed({"raceId":"race_id","year":"race_year", "circuitId":"circuit_id"})

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, col, lit, current_timestamp

# COMMAND ----------

races_addcolumn_df = races_renamed_df.withColumn("race_timestamp",to_timestamp(concat(col("date"), lit(" "),col("time")),'yyyy-MM-dd HH:mm:ss'))\
.withColumn("Ingestion_Date", current_timestamp())

# COMMAND ----------

races_final_df = races_addcolumn_df.select("race_id","race_year", "round","circuit_id","name","race_timestamp","Ingestion_Date")

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").parquet("abfss://processed@sanjayformula1dl2.dfs.core.windows.net/races")

# COMMAND ----------

display(spark.read.parquet("abfss://processed@sanjayformula1dl2.dfs.core.windows.net/races"))

# COMMAND ----------


