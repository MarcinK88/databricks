-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Upgrade a Table to Unity Catalog
-- MAGIC 
-- MAGIC In this notebook you will learn how to:
-- MAGIC * Migrate a table from the existing legacy Hive metastore to Unity Catalog
-- MAGIC * Create appropriate grants to enable others to access the table
-- MAGIC * Perform simple transformations on a table while migrating to Unity Catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Up
-- MAGIC 
-- MAGIC Run the following cells to perform some setup. In order to avoid conflicts in a shared training environment, this will create a uniquely named database exclusively for your use. This will also create an example source table called **movies** within the legacy Hive metatore. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note: this notebook assumes a catalog named *main* in your Unity Catalog metastore. If you need to target a different catalog, edit the following notebook, **Classroom-Setup**, before proceeding.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %run ./Includes/04-4-Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Within the legacy Hive metastore local to this workspace, we now have a table called **movies**, residing in a the user-specific database outlined in the cell output from above. To make things easier, the database name is stored in a Hive variable named *da.db_name*. Let's preview the data stored in this table using that variable.

-- COMMAND ----------

SELECT * FROM hive_metastore.`${da.db_name}`.movies LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set up destination
-- MAGIC 
-- MAGIC With a source table in place, let's set up a destination in Unity Catalog to migrate our table to.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Select Unity Catalog metastore for usage
-- MAGIC 
-- MAGIC Let's begin by selecting a catalog from the Unity Catalog metastore. By default this is set to **main** unless you editted the **Classroom-Setup** notebook. It's not necessary to do this, but it eliminates the need to have to specify a catalog in your table references.

-- COMMAND ----------

USE CATALOG `${da.catalog}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and select database for usage
-- MAGIC 
-- MAGIC Let's create a unique database in Unity Catalog to avoid potentially interfering with others in a shared training environment. To do this, we will leverage the Hive variable defined earlier.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS `${da.db_name}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's select the newly created database to further simplify working with the destination table. Again, this step is not necessary but it will further simplify references to your upgraded tables.

-- COMMAND ----------

USE `${da.db_name}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Upgrade the table
-- MAGIC 
-- MAGIC Copying the table boils down to a simple **CREATE TABLE AS SELECT** (CTAS) operation, using the three-level namespace to specify the source table. We do not need to specify the destination using three levels due to the **USE CATALOG** and **USE** statements run previously.
-- MAGIC 
-- MAGIC Note: for large tables this operation can take time as all table data is copied.

-- COMMAND ----------

CREATE OR REPLACE TABLE movies
AS SELECT * FROM hive_metastore.`${da.db_name}`.movies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The table is now copied, with the new table under Unity Catalog control. Let's quickly check to see any grants on the new table.

-- COMMAND ----------

SHOW GRANTS ON movies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Currently there are no grants.
-- MAGIC 
-- MAGIC Now let's examine the grants on the original table. Uncomment the code in the following cell and run it.

-- COMMAND ----------

-- SHOW GRANTS ON hive_metastore.`${da.db_name}`.movies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This gives an error since this table lives in the legacy metastore and we are not running on a cluster with legacy table access control enabled. This highlights a key benefit of Unity Catalog: no additional configuration is needed to acheive a secure solution. Unity Catalog is secure by default.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Grant access to table [optional]
-- MAGIC 
-- MAGIC With a new table in place, let's allow users in the **analysts** group to read from it.
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
-- MAGIC ### Grant SELECT privilege on table
-- MAGIC 
-- MAGIC The first requirement is to grant the **SELECT** privilege on the new table to the **analysts** group.

-- COMMAND ----------

-- GRANT SELECT ON TABLE movies to `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Grant USAGE privilege on database
-- MAGIC 
-- MAGIC **USAGE** privilege is also required on the database.

-- COMMAND ----------

-- GRANT USAGE ON DATABASE `${da.db_name}` TO `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Access table as user
-- MAGIC 
-- MAGIC With appropriate grants in place, attempt to read from the table in the Databricks SQL environment of your secondary user. 
-- MAGIC 
-- MAGIC Run the following cell to output a query statement that reads from the newly created table. Copy and paste the output into a new query within the SQL environment of your secondary user, and run the query.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog}.{DA.db_name}.movies")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Transform table while upgrading
-- MAGIC 
-- MAGIC Migrating a table to Unity Catalog is itself a simple operation, but the overall move to Unity Catalog is a big one for any organization. It's a great time to closely consider your tables and schemas and whether they still address your organization's business requirements that may have changed over time.
-- MAGIC 
-- MAGIC The example we saw earlier takes an exact copy of the source table. Since migrating a table is a simple **CREATE TABLE AS SELECT** operation, we can perform any transformations during the migration that can be performed with **SELECT**. For example, let's expand on the previous example to do the following tranformations:
-- MAGIC * Assign the name *idx* to the first column
-- MAGIC * Additionally select only the columns **title**, **year**, **budget** and **rating**
-- MAGIC * Convert **year** and **budget** to **INT** (replacing any instances of the string *NA* with 0)
-- MAGIC * Convert **rating** to **DOUBLE**

-- COMMAND ----------

CREATE OR REPLACE TABLE movies
AS SELECT
  _c0 AS idx,
  title,
  CAST(year AS INT) AS year,
  CASE WHEN
    budget = 'NA' THEN 0
    ELSE CAST(budget AS INT)
  END AS budget,
  CAST(rating AS DOUBLE) AS rating
FROM hive_metastore.`${da.db_name}`.movies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If you are running queries as a secondary user, re-run the previous query in the Databricks SQL environment of the secondary user. Verify that:
-- MAGIC 1. the table can still be accessed
-- MAGIC 1. the table schema has been updated

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean up
-- MAGIC Run the following cell to remove the source database and table that was used in this example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>