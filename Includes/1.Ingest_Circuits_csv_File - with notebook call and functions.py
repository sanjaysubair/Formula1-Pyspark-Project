# Databricks notebook source
# MAGIC %md
# MAGIC #1. Ingest Circuits .csv file
# MAGIC

# COMMAND ----------
# calling configuration notebook
# MAGIC %run "../Includes/Configuration"

# COMMAND ----------
#calling common_functions notebook
# MAGIC %run "../Includes/Common_Functions"

# COMMAND ----------
# Replace the raw conatiner url using the raw_folder_path variable with fstring method
circuits_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw_folder_path}/circuits.csv")


# MAGIC %md
# MAGIC Create own schema for ingestion

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

Circuits_Schema = StructType(fields= [
    StructField("circuitId",IntegerType(), False),
    StructField("circuitRef",StringType(), True),
    StructField("name",StringType(), True),
    StructField("location",StringType(), True),
    StructField("country",StringType(), True),
    StructField("lat",DoubleType(), True),
    StructField("lng",DoubleType(), True),
    StructField("alt",IntegerType(), False),
    StructField("url",StringType(), False)
    ]
)

# COMMAND ----------

circuits_df1 = spark.read.schema(Circuits_Schema).option("Header", True).csv(f"{raw_folder_path}/circuits.csv")

# MAGIC %md
# MAGIC Select only required columns

# COMMAND ----------

#circuits_selected_df = circuits_df1.select("circuitId", "circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

#circuits_selected_df = circuits_df1.select(circuits_df1.circuitId, circuits_df1.circuitRef,circuits_df1.name,circuits_df1.location,circuits_df1.country,circuits_df1.lat,circuits_df1.lng,circuits_df1.alt)

# COMMAND ----------

#circuits_selected_df = circuits_df1.select(circuits_df1["circuitId"], circuits_df1["circuitRef"],circuits_df1["name"],circuits_df1["location"],circuits_df1["country"],circuits_df1["lat"],circuits_df1["lng"],circuits_df1["alt"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df1.select(col("circuitId"), col("circuitRef"),col("name"),col("location").alias("Race Location"),col("country"),col("lat"),col("lng"),col("alt"))


# MAGIC %md
# MAGIC Rename columns
# MAGIC

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuits_id")\
.withColumnRenamed("circuitRef","circuits_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC Add timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------
#call function defined in the common_functions notebook
circuits_final_df = add_ingestion_date(circuits_renamed_df)\
.withColumn("env",lit("Production"))

# COMMAND ----------

# MAGIC %md
# MAGIC Save as parquet file
# MAGIC

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("abfss://processed@sanjayformula1dl2.dfs.core.windows.net/circuits")

# COMMAND ----------

dbutils.fs.ls("abfss://processed@sanjayformula1dl2.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.parquet("abfss://processed@sanjayformula1dl2.dfs.core.windows.net/circuits/"))

# COMMAND ----------


