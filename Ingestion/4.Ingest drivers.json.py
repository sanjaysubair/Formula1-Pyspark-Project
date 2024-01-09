# Databricks notebook source
# MAGIC %md
# MAGIC ###1.Read the drivers json
# MAGIC ####Create two schemas , one for the nested field, and one for the entire json

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(
    fields = [
            StructField("forename", StringType(),False),
            StructField("surname", StringType(), False)
            ]
)

# COMMAND ----------

drivers_schema = StructType(
    fields = [StructField("driverId", IntegerType(), False),
            StructField("driverRef", StringType(), True),
            StructField("number", IntegerType(), True),
            StructField("code", StringType(), True),
            StructField("name", name_schema),
            StructField("dob", DateType(), True),
            StructField("nationality", StringType(), True),
            StructField("url", StringType(), True)]
            
)

# COMMAND ----------

drivers_df = spark.read.\
schema(drivers_schema).\
json("abfss://raw@sanjayformula1dl2.dfs.core.windows.net/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Rename Columns, create name column and add ingestion timestamp

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

# COMMAND ----------

drivers_final_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                                  .withColumnRenamed("driverRef", "driver_ref")\
                                  .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
                                  .withColumn("Ingestion_date", current_timestamp())\
                                  .drop("url")


# COMMAND ----------

# MAGIC %md
# MAGIC Write to processed folder

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("abfss://processed@sanjayformula1dl2.dfs.core.windows.net/drivers")

# COMMAND ----------


