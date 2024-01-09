# Databricks notebook source
# MAGIC %md
# MAGIC Create Schema and Read multiple csv files from the folder

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(
    fields = [
             StructField("raceId", IntegerType(), False),
             StructField("driverID", IntegerType(), True),
             StructField("lap", IntegerType(), True),
             StructField("position", IntegerType(), True),
             StructField("time", StringType(), True),
             StructField("milliseconds", IntegerType(), True)
             ]
    
)

# COMMAND ----------

lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv("abfss://raw@sanjayformula1dl2.dfs.core.windows.net/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC Rename columns and add ingestion date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumn("Ingested_Time", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC Write to processed container

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet("abfss://processed@sanjayformula1dl2.dfs.core.windows.net/lap_times")

# COMMAND ----------


