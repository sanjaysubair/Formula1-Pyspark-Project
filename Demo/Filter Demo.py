# Databricks notebook source
# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# using SQL method to filter the file
races_filtered_df = races_df.filter("race_year = 2019")

# COMMAND ----------

# using python method to filter the file
#races_filtered_df1 = races_df.filter(races_df.race_year == 2019)
races_filtered_df1 = races_df.filter(races_df["race_year"] == 2019)

# COMMAND ----------

# using multiple filter conditions
# races_filtered_df2 = races_df.filter("race_year = 2019 and round <=5")
races_filtered_df3 = races_df.\
     filter((races_df["race_year"] == 2019) & (races_df["round"]<= 5))

# COMMAND ----------

display(races_filtered_df3)

# COMMAND ----------


