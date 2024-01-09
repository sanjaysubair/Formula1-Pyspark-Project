# Databricks notebook source
#Created a notebook with variables for raw, processed and presentation container URL, 
#and replaced the hardcoded urls using f-string method in the Ingest_circuits_csv file.
raw_folder_path = "abfss://raw@sanjayformula1dl2.dfs.core.windows.net"
processed_folder_path = "abfss://processed@sanjayformula1dl2.dfs.core.windows.net"
presentation_folder_path = "abfss://presentation@sanjayformula1dl2.dfs.core.windows.net"
