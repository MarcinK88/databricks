-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create and Share Tables in Unity Catalog
-- MAGIC 
-- MAGIC In this notebook you will learn how to:
-- MAGIC * Create databases and tables
-- MAGIC * Control access to databases and tables
-- MAGIC * Explore grants on various objects in Unity Catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Up
-- MAGIC 
-- MAGIC Run the following cells to perform some setup. In order to avoid conflicts in a shared training environment, this will generate a unique database name exclusively for your use. In your own environment you are free to choose your own database name, but if performing this exercise in a shared training environment, please use the generated one as provided.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note: this notebook assumes a catalog named *main* in your Unity Catalog metastore. If you need to target a different catalog, edit the following notebook, **Classroom-Setup**, before proceeding.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %run ./Includes/04-2-Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Unity Catalog three-level namespace
-- MAGIC 
-- MAGIC Most SQL developers will be familiar with using a two-level namespace to unambiguously address tables within a database/schema as follows:
-- MAGIC 
-- MAGIC     SELECT * FROM database.table;
-- MAGIC 
-- MAGIC Unity Catalog introduces the concept of a *catalog* that resides above the database in the object hierarchy. Metastores can host any number of catalogs, which in turn can host any number of database/schemas. To deal with this additional level, complete table references in Unity Catalog use a three-level namespace. The following statement exemplifies this:
-- MAGIC 
-- MAGIC     SELECT * FROM catalog.database.table;
-- MAGIC     
-- MAGIC SQL developers will probably also be familiar with the **`USE`** statement to select a default database, to avoid having to always specify a database when referencing tables. Unity Catalog augments this with the **`USE CATALOG`** statement, which similarly selects a default catalog.
-- MAGIC 
-- MAGIC In the cell below, we select the catalog from our Unity Catalog metastore as our default. Following execution of this statement, any database/schema references will be assumed to be in this catalog unless explicitely overridden by a three-level reference.

-- COMMAND ----------

USE CATALOG `${da.catalog}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create and use a new database
-- MAGIC 
-- MAGIC Let's create a new database exclusively for our use in this exercise, then set this as the default so we can reference tables by name only.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS `${da.db_name}`

-- COMMAND ----------

USE `${da.db_name}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Delta architecture
-- MAGIC 
-- MAGIC Let's create and populate a simple collection of tables persuant to the Delta architecture:
-- MAGIC * A silver table containing patient heart rate data as read from a medical device
-- MAGIC * A gold table that averages heart rate data per patient on a daily basis
-- MAGIC 
-- MAGIC For now, there will be no bronze table in this simple example.
-- MAGIC 
-- MAGIC Note that we need ony specify the table name below, since we have set a default catalog and database above.

-- COMMAND ----------

CREATE TABLE silver
(
  device_id  INT,
  mrn        STRING,
  name       STRING,
  time       TIMESTAMP,
  heartrate  DOUBLE
)

-- COMMAND ----------

INSERT INTO silver VALUES
  (23,'40580129','Nicholas Spears','2020-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177','Lynn Russell','2020-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842','Samuel Hughes','2020-02-01T00:08:58.000+0000',52.1354807863),
  (23,'40580129','Nicholas Spears','2020-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',95.033344842),
  (37,'65300842','Samuel Hughes','2020-02-01T00:23:58.000+0000',57.3391541312),
  (23,'40580129','Nicholas Spears','2020-02-01T00:31:58.000+0000',56.6165053697),
  (17,'52804177','Lynn Russell','2020-02-01T00:32:56.000+0000',94.8134313932),
  (37,'65300842','Samuel Hughes','2020-02-01T00:38:54.000+0000',56.2469995332),
  (23,'40580129','Nicholas Spears','2020-02-01T00:46:57.000+0000',54.8372685558)

-- COMMAND ----------

CREATE TABLE gold AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM silver
  GROUP BY mrn, name, DATE_TRUNC("DD", time))

-- COMMAND ----------

SELECT * FROM gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Grant access to gold table [optional]
-- MAGIC 
-- MAGIC Now let's allow users in the **analysts** group to read from the **gold** table.
-- MAGIC 
-- MAGIC 
-- MAGIC Note that you can only perform this section if you followed along with the *Manage users and groups* exercise and created a Unity Catalog group named **analysts**.
-- MAGIC 
-- MAGIC Perform this section by uncommenting the code cells and running them in sequence. You will also be prompted to run some queries as a secondary user. To do this:
-- MAGIC 
-- MAGIC 1. Open a separate private browsing session and log in to Databricks SQL using the user id you created when performing *Manage users and groups*.
-- MAGIC 1. Create a SQL endpoint following the instructions in *Create SQL Endpoint in Unity Catalog*.
-- MAGIC 1. Prepare to enter queries as instructed below in that environment.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's grant **SELECT** privilege on the **gold** table.

-- COMMAND ----------

-- GRANT SELECT ON TABLE gold to `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query table as user
-- MAGIC 
-- MAGIC With a **SELECT** grant in place, attempt to query the table in the Databricks SQL environment of your secondary user.
-- MAGIC 
-- MAGIC Run the following cell to output a query statement that reads from the **gold** table. Copy and paste the output into a new query within the SQL environment of your secondary user, and run the query.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog}.{DA.db_name}.gold")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This will not work yet, because **SELECT** privilege on the table alone is insufficient. **USAGE** privilege is also required on the containing elements. Let's correct this now by executing the following.

-- COMMAND ----------

-- GRANT USAGE ON DATABASE `${da.db_name}` TO `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Repeat the query in the Databricks SQL environment, and with these two grants in place the operation should succeed.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Explore grants
-- MAGIC 
-- MAGIC Let's explore the grants on some of the objects in the Unity Catalog hierarchy, starting with the **gold** table.

-- COMMAND ----------

SHOW GRANT ON TABLE gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Currenly there is only the **SELECT** grant we set up earlier. Now let's check the grants on **silver**.

-- COMMAND ----------

SHOW GRANT ON TABLE silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC There are currently no grants on this table; only the owner can access this table.
-- MAGIC 
-- MAGIC Now let's look at the containing database.

-- COMMAND ----------

SHOW GRANT ON DATABASE `${da.db_name}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Currently we see the **USAGE** grant we set up earlier.
-- MAGIC 
-- MAGIC Now let's examine the catalog.

-- COMMAND ----------

SHOW GRANT ON CATALOG `${da.catalog}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Notice that **USAGE** is granted to **account users** already, which is why we did not explicitly have to grant **USAGE** on the catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean up
-- MAGIC Run the following cell to remove the database that we created in this example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>