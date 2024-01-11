# Databricks notebook source
# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import sum, count, col, when
constructor_standing_df = race_results_df\
    .groupBy("race_year","team")\
    .agg(sum("points").alias("total_points"),\
        count(when(col("position")==1, True)).alias("wins"))
    

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_constructor_df = constructor_standing_df\
    .withColumn("rank", rank().over(window_spec))

# COMMAND ----------

display(final_constructor_df)

# COMMAND ----------

final_constructor_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standing")

# COMMAND ----------


