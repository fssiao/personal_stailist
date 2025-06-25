# Databricks notebook source
# MAGIC %pip install -qqqq -U -r test_requirements.txt
# MAGIC # Restart to load the packages into the Python environment
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import yaml
import os
import pandas as pd

# Use the workspace client to retrieve information about the current user
w = WorkspaceClient()
user_email = w.current_user.me().display_name
username = user_email.split("@")[0]

# Catalog and schema have been automatically created thanks to lab environment
#catalog_name = f"{username}_vocareum_com"
#schema_name = "agents"

catalog_name = "rcbc_hackathon"
schema_name = "group_5"

# Allows us to reference these values when creating SQL/Python functions
dbutils.widgets.text("catalog_name", defaultValue=catalog_name, label="Catalog Name")
dbutils.widgets.text("schema_name", defaultValue=schema_name, label="Schema Name")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

df = pd.read_csv('(test) sample_data.csv', dtype={'clothing_id':str}).reset_index()
df.head()
closet_df = pd.DataFrame(df)
spark_df = spark.createDataFrame(df); 
spark_df.createOrReplaceTempView("closet_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from closet_df

# COMMAND ----------

# MAGIC %sql
# MAGIC (
# MAGIC   select occassion,category,brand_name,product_display_name
# MAGIC   from closet_df
# MAGIC   where category = 'Top' and occassion='Casual'
# MAGIC   order by rand()
# MAGIC   limit 1
# MAGIC )
# MAGIC union
# MAGIC (
# MAGIC   select occassion,category,brand_name,product_display_name
# MAGIC   from closet_df
# MAGIC   where category = 'Bottom' and occassion='Casual'
# MAGIC   order by rand()
# MAGIC   limit 1
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC   IDENTIFIER(:catalog_name || '.' || :schema_name || '.test_get_style')(occassion STRING)
# MAGIC RETURNS TABLE (
# MAGIC   occassion STRING,
# MAGIC   category STRING,
# MAGIC   brand_name STRING,
# MAGIC   product_display_name STRING
# MAGIC )
# MAGIC COMMENT 'Suggest outfit'
# MAGIC LANGUAGE SQL
# MAGIC RETURN (
# MAGIC   (
# MAGIC   select occassion,category,brand_name,product_display_name
# MAGIC   from closet_df
# MAGIC   where category = 'Top' and occassion= occassion
# MAGIC   order by rand()
# MAGIC   limit 1
# MAGIC )
# MAGIC union
# MAGIC (
# MAGIC   select occassion,category,brand_name,product_display_name
# MAGIC   from closet_df
# MAGIC   where category = 'Bottom' and occassion= occassion
# MAGIC   order by rand()
# MAGIC   limit 1
# MAGIC )
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from IDENTIFIER(:catalog_name || '.' || :schema_name || '.test_get_style')('Casual')
