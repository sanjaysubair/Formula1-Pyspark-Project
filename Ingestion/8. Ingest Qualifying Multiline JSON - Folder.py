# Databricks notebook source
# MAGIC %md
# MAGIC Create Schema and read multiline JSON files from a folder

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(
    fields= [
        StructField("qualifyId", IntegerType(), False),
        StructField("raceId", IntegerType(), True),
        StructField("driverId", IntegerType(), True),
        StructField("constructorId", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("q1", StringType(), True),
        StructField("q2", StringType(), True),
        StructField("q3", StringType(), True),
    
        
    ]
)

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiLine", True)\
.json("abfss://raw@sanjayformula1dl2.dfs.core.windows.net/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC Rename fields and add ingestion time

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_qualifying_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumn("Ingestion_time", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC Write to processed folder

# COMMAND ----------

final_qualifying_df.write.mode("overwrite").parquet("abfss://processed@sanjayformula1dl2.dfs.core.windows.net/qualifying")

# COMMAND ----------


