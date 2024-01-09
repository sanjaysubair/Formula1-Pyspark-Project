# Databricks notebook source
#Created a notebook with a function, which inputs a dataframe, and returns a dataframe with the ingestion_time column added.
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("Ingestion_Time", current_timestamp())
    return output_df
