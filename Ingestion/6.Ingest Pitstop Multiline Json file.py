# Databricks notebook source
# MAGIC %md
# MAGIC Define Schema and Read the json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pitstop_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), True),
        StructField("stop", StringType(), True),
        StructField("lap", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("milliseconds", IntegerType(), True)
    ]
)

# COMMAND ----------

pitstop_df = spark.read\
.schema(pitstop_schema)\
.option("multiLine",True)\
.json("abfss://raw@sanjayformula1dl2.dfs.core.windows.net/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Rename columns and add ingestion date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pitstop_final_df = pitstop_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumn("Ingested_Time", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC Write to processed container

# COMMAND ----------

pitstop_final_df.write.mode("overwrite").parquet("abfss://processed@sanjayformula1dl2.dfs.core.windows.net/pit_stops")

# COMMAND ----------


