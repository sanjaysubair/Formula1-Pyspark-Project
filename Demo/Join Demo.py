# Databricks notebook source
# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("name", "circuit_name")\
    .filter("circuits_id <= 69")
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Inner Join

# COMMAND ----------

race_circuits_df = circuits_df\
    .join(races_df,circuits_df.circuits_id == races_df.circuit_id, "inner")\
    .select(circuits_df.circuits_id, circuits_df.circuit_name, races_df.name, races_df.race_id )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Outer Join

# COMMAND ----------

# left outer join
race_circuits_df = circuits_df\
    .join(races_df,circuits_df.circuits_id == races_df.circuit_id, "left")\
    .select(circuits_df.circuits_id, circuits_df.circuit_name, races_df.name, races_df.race_id )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# right outer join
race_circuits_df = circuits_df\
    .join(races_df,circuits_df.circuits_id == races_df.circuit_id, "right")\
    .select(circuits_df.circuits_id, circuits_df.circuit_name, races_df.name, races_df.race_id )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# full outer join
race_circuits_df = circuits_df\
    .join(races_df,circuits_df.circuits_id == races_df.circuit_id, "full")\
    .select(circuits_df.circuits_id, circuits_df.circuit_name, races_df.name, races_df.race_id )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Semi Join \
# MAGIC Returns only matching rows from the left table. Basically an inner join with columns from the left table

# COMMAND ----------

race_circuits_df = circuits_df\
    .join(races_df,circuits_df.circuits_id == races_df.circuit_id, "semi")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Anti Join \
# MAGIC Returns every row from the left df not found in the right df

# COMMAND ----------

race_circuits_df = circuits_df\
    .join(races_df,circuits_df.circuits_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cross Join\
# MAGIC Cartesion product

# COMMAND ----------

race_circuits_df = circuits_df\
    .crossJoin(races_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------


