# Databricks notebook source
# MAGIC %md
# MAGIC 1. Creat Schema and Read results file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(
    fields = [
        StructField("resultId", IntegerType(), False),
        StructField("raceId", IntegerType(), True),
        StructField("driverId", IntegerType(), True),
        StructField("constructorId", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("grid", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("positionText", StringType(), True),
        StructField("positionOrder", IntegerType(), True),
        StructField("points", FloatType(), True),
        StructField("laps", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
        StructField("fastestLap", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("fastestLapSpeed", StringType(), True),
        StructField("statusId", IntegerType(), True) 
    ]
)

# COMMAND ----------

results_df = spark.read.schema(results_schema).json("abfss://raw@sanjayformula1dl2.dfs.core.windows.net/results.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId","result_id")\
                               .withColumnRenamed("raceId","race_id")\
                               .withColumnRenamed("driverId","driver_id")\
                               .withColumnRenamed("constructorId","constructor_id")\
                               .withColumnRenamed("positionText","position_text")\
                               .withColumnRenamed("positionOrder","position_order")\
                               .withColumnRenamed("fastestLap","fastest_lap")\
                               .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                               .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                               .drop("statusId")\
                               .withColumn("Ingestion_time", current_timestamp()) 


# COMMAND ----------

# MAGIC %md
# MAGIC Write dataframe to processed folder

# COMMAND ----------

results_renamed_df.write.mode("overwrite").parquet("abfss://processed@sanjayformula1dl2.dfs.core.windows.net/results")

# COMMAND ----------


