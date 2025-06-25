# Databricks notebook source
# MAGIC %md
# MAGIC ## Import preliminaries

# COMMAND ----------

# DBTITLE 1,Library Installs
# MAGIC %pip install -qqqq -U -r requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import packages
from databricks.sdk import WorkspaceClient
from unitycatalog.ai.core.databricks import DatabricksFunctionClient
from IPython.display import display, HTML
from datetime import datetime

import yaml
import os
import pandas as pd
import itertools

# COMMAND ----------

# DBTITLE 1,Parameter Configs
w = WorkspaceClient()
user_email = w.current_user.me().display_name
username = user_email.split("@")[0]

catalog_name = "dsag_dev_catalog"
schema_name = "group_5"

dbutils.widgets.text("catalog_name", defaultValue=catalog_name, label="Catalog Name")
dbutils.widgets.text("schema_name", defaultValue=schema_name, label="Schema Name")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
tbl_src = f"{catalog_name}.{schema_name}.table"

# COMMAND ----------

# MAGIC %md
# MAGIC ## User-defined functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Functions

# COMMAND ----------

# func_name = 'sample_schema_function'
# func_comment = 'sample_comment'

# spark.sql(f"""
#     CREATE OR REPLACE FUNCTION 
#     IDENTIFIER('{catalog_name}.{schema_name}.{func_name}')()
#     RETURNS TABLE(
#         policy  STRING,
#         policy_details STRING,
#         last_updated DATE)
#     COMMENT '{func_comment}'
#     LANGUAGE SQL
#     RETURN (
#         SELECT     
#             policy,
#             policy_details,
#             last_updated
#         FROM agents_lab.product.policies
#         LIMIT 1
#     )
# """)
# #  ---- CAN ALSO RETURN STRING

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python Functions

# COMMAND ----------

def get_combination(df):
    categories = ['top', 'bottom', 'footwear']#, 'accessories']

    bottoms = df[df['category']=='Bottom']['clothing_id'].tolist()
    tops = df[df['category']=='Top']['clothing_id'].tolist()
    footwears = df[df['category']=='Footwear']['clothing_id'].tolist()
    accessories = df[df['category']=='Accessories']['clothing_id'].tolist()

    combi_list = list(itertools.product(tops, bottoms, footwears))

    df_combi = pd.DataFrame(combi_list, columns=categories)
    df_combi['combination_id'] = df_combi[categories].apply(lambda row: ''.join(row.values.astype(str)), axis=1)

    for cat in categories:
        df_combi[f'{cat}'] = df_combi[cat].astype(str).map(df.set_index('clothing_id')['item_desc'])

    return df_combi

def get_men_formal_combinations(df):
    df_filtered = df[(df['occassion']=='Formal') & (df['gender'].isin(['Men', 'Unisex']))]
    df_combi = get_combination(df_filtered)
    return df_combi

def get_women_formal_combinations(df):
    df_filtered = df[(df['occassion']=='Formal') & (df['gender'].isin(['Women', 'Unisex']))]
    df_combi = get_combination(df_filtered)
    return df_combi

def get_men_casual_combinations(df):
    df_filtered = df[(df['occassion']=='Casual') & (df['gender'].isin(['Men', 'Unisex']))]
    df_combi = get_combination(df_filtered)
    return df_combi

def get_women_casual_combinations(df):
    df_filtered = df[(df['occassion']=='Casual') & (df['gender'].isin(['Women', 'Unisex']))]
    df_combi = get_combination(df_filtered)
    return df_combi

# COMMAND ----------

df = pd.read_csv('sample_data_2.csv', dtype={'clothing_id':str})
df.replace('Unspecified', '', inplace=True)
df.drop(['product_display_name', 'age', 'purchase_date'], axis=1, inplace=True)
df.fillna('', inplace=True)
df['clothing_id'] = df['clothing_id'].str.pad(3, fillchar='0')
df['item_desc'] = df[['pattern', 'subcolor', 'brand_name', 'subcategory']].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
df.head(10)

# COMMAND ----------

get_combination(df)

# COMMAND ----------

client = DatabricksFunctionClient()

python_tool_uc_info = client.create_python_function(func=get_todays_date, catalog=catalog_name, schema=schema_name, replace=True)

print(f"Deployed Unity Catalog function name: {python_tool_uc_info.full_name}")

# COMMAND ----------

workspace_url = spark.conf.get('spark.databricks.workspaceUrl')
workspace_url

# COMMAND ----------

# Retrieve the Databricks host URL
workspace_url = spark.conf.get('spark.databricks.workspaceUrl')

# Create HTML link to created functions
html_link = f'<a href="https://{workspace_url}/explore/data/functions/{catalog_name}/{schema_name}/get_todays_date" target="_blank">Go to Unity Catalog to see Registered Functions</a>'
display(HTML(html_link))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Front-end: Streamlit

# COMMAND ----------

# DBTITLE 1,Create link to AI Playground
# Create HTML link to AI Playground
html_link = f'<a href="https://{workspace_url}/ml/playground" target="_blank">Go to AI Playground</a>'
display(HTML(html_link))
