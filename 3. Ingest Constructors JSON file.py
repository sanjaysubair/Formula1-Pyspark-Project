# Databricks notebook source
# MAGIC %md
# MAGIC Read Constructors file

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read\
.schema(constructors_schema)\
.json("abfss://raw@sanjayformula1dl2.dfs.core.windows.net/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Drop columns  

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(constructors_df['url'])

# COMMAND ----------

# MAGIC %md 
# MAGIC Rename column and add a column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId","constructor_id")\
                                               .withColumnRenamed("constructorRef","constructor_ref")\
                                               .withColumn("Ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC Write parquet file

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet("abfss://processed@sanjayformula1dl2.dfs.core.windows.net/constructors.json")

# COMMAND ----------


