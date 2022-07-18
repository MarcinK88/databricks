# Databricks notebook source
# MAGIC %run ./_dbacademy_helper

# COMMAND ----------

# MAGIC %run ./Classroom-Setup

# COMMAND ----------

DA = DBAcademyHelper(catalog=catalog)
DA.cleanup()            # Remove the existing database and files
DA.init(create_db=True) # True is the default

# Execute any special scripts we need for this lesson
# create_magic_table() or whatever setup function you want
# init_mlflow_as_job()

DA.conclude_setup()
