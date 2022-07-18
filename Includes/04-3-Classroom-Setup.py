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

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_managed
# MAGIC (
# MAGIC   device_id  INT,
# MAGIC   mrn        STRING,
# MAGIC   name       STRING,
# MAGIC   time       TIMESTAMP,
# MAGIC   heartrate  DOUBLE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver_managed VALUES
# MAGIC   (23,'40580129','Nicholas Spears','2020-02-01T00:01:58.000+0000',54.0122153343),
# MAGIC   (17,'52804177','Lynn Russell','2020-02-01T00:02:55.000+0000',92.5136468131),
# MAGIC   (37,'65300842','Samuel Hughes','2020-02-01T00:08:58.000+0000',52.1354807863),
# MAGIC   (23,'40580129','Nicholas Spears','2020-02-01T00:16:51.000+0000',54.6477014191),
# MAGIC   (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',95.033344842),
# MAGIC   (37,'65300842','Samuel Hughes','2020-02-01T00:23:58.000+0000',57.3391541312),
# MAGIC   (23,'40580129','Nicholas Spears','2020-02-01T00:31:58.000+0000',56.6165053697),
# MAGIC   (17,'52804177','Lynn Russell','2020-02-01T00:32:56.000+0000',94.8134313932),
# MAGIC   (37,'65300842','Samuel Hughes','2020-02-01T00:38:54.000+0000',56.2469995332),
# MAGIC   (23,'40580129','Nicholas Spears','2020-02-01T00:46:57.000+0000',54.8372685558)