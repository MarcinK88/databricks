# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Resets the environment removing any directories and or tables created during course execution

# COMMAND ----------

# MAGIC %run ./_dbacademy_helper

# COMMAND ----------

# MAGIC %run ./Classroom-Setup

# COMMAND ----------

import re

DA = DBAcademyHelper(catalog=catalog)

course = "ncouc"

username = spark.sql("select current_user()").first()[0]
clean_username = re.sub(r"[^a-zA-Z0-9]", "_", username)

for catalog in DA.catalogs:
    print(f"Processing the catalog {catalog}")
    spark.sql(f"USE CATALOG {catalog}")
    rows = spark.sql("SHOW DATABASES").collect()
    for row in rows:
        db_name = row[0]
        if db_name.startswith(DA.db_name_prefix):
            print(f" - dropping database {catalog}.{db_name}")
            spark.sql(f"DROP DATABASE IF EXISTS {catalog}.{db_name} CASCADE")

result = dbutils.fs.rm(DA.working_dir_prefix, True)
print(f"\nDeleted {DA.working_dir_prefix}: {result}")

print("\nCourse environment succesfully reset")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>