# Databricks notebook source
# MAGIC %md
# MAGIC ## Import preliminaries

# COMMAND ----------

# DBTITLE 1,Library Installs
# MAGIC %pip install --q -U -r requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import packages
from databricks.sdk import WorkspaceClient
from unitycatalog.ai.core.databricks import DatabricksFunctionClient
from IPython.display import display, HTML

import yaml
import os
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Parameter Configs
w = WorkspaceClient()
client = DatabricksFunctionClient()
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

func_name = 'get_all_combinations'
func_comment = 'Get all possible combinations regardless of gender and occassion'

spark.sql(f"""
    CREATE OR REPLACE FUNCTION 
    IDENTIFIER('{catalog_name}.{schema_name}.{func_name}')()
    RETURNS TABLE(
        top  STRING,
        bottom STRING,
        footwear STRING,
        accessory STRING
    )
    COMMENT '{func_comment}'
    LANGUAGE SQL
    RETURN (
        WITH wardrobe AS (
            SELECT * FROM dsag_dev_catalog.group_5.wardrobe_sample 
        ),

        part_top AS (
            SELECT item_desc AS top FROM wardrobe 
            WHERE category = 'Top'
        ),

        part_bottom AS (
            SELECT item_desc AS bottom FROM wardrobe 
            WHERE category = 'Bottom'
        ),

        part_footwear AS (
            SELECT item_desc AS footwear FROM wardrobe 
            WHERE category = 'Footwear'
        ),

        part_accessory AS (
            SELECT item_desc AS accessory FROM wardrobe 
            WHERE category = 'Accessories'
        )

        SELECT DISTINCT
        *
        FROM part_top AS a
        FULL OUTER JOIN part_bottom AS b
        FULL OUTER JOIN part_footwear AS c
        FULL OUTER JOIN part_accessory AS d
        ORDER BY rand()
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions: Men

# COMMAND ----------

func_name = 'get_men_formal_combinations'
func_comment = 'Get all possible formal combinations for men'

spark.sql(f"""
    CREATE OR REPLACE FUNCTION 
    IDENTIFIER('{catalog_name}.{schema_name}.{func_name}')()
    RETURNS TABLE(
        top  STRING,
        bottom STRING,
        footwear STRING,
        accessory STRING
    )
    COMMENT '{func_comment}'
    LANGUAGE SQL
    RETURN (
        WITH wardrobe AS (
            SELECT * FROM dsag_dev_catalog.group_5.wardrobe_sample 
            WHERE occassion = 'Formal' AND gender IN ('Men', 'Unisex')
        ),

        part_top AS (
            SELECT item_desc AS top FROM wardrobe 
            WHERE category = 'Top'
        ),

        part_bottom AS (
            SELECT item_desc AS bottom FROM wardrobe 
            WHERE category = 'Bottom'
        ),

        part_footwear AS (
            SELECT item_desc AS footwear FROM wardrobe 
            WHERE category = 'Footwear'
        ),

        part_accessory AS (
            SELECT item_desc AS accessory FROM wardrobe 
            WHERE category = 'Accessories'
        )

        SELECT DISTINCT
        *
        FROM part_top AS a
        FULL OUTER JOIN part_bottom AS b
        FULL OUTER JOIN part_footwear AS c
        FULL OUTER JOIN part_accessory AS d
        ORDER BY rand()
        -- LIMIT 10
    )
""")

# COMMAND ----------

func_name = 'get_men_casual_combinations'
func_comment = 'Get all possible casual combinations for men'

spark.sql(f"""
    CREATE OR REPLACE FUNCTION 
    IDENTIFIER('{catalog_name}.{schema_name}.{func_name}')()
    RETURNS TABLE(
        top  STRING,
        bottom STRING,
        footwear STRING,
        accessory STRING
    )
    COMMENT '{func_comment}'
    LANGUAGE SQL
    RETURN (
        WITH wardrobe AS (
            SELECT * FROM dsag_dev_catalog.group_5.wardrobe_sample 
            WHERE occassion = 'Casual' AND gender IN ('Men', 'Unisex')
        ),

        part_top AS (
            SELECT item_desc AS top FROM wardrobe 
            WHERE category = 'Top'
        ),

        part_bottom AS (
            SELECT item_desc AS bottom FROM wardrobe 
            WHERE category = 'Bottom'
        ),

        part_footwear AS (
            SELECT item_desc AS footwear FROM wardrobe 
            WHERE category = 'Footwear'
        ),

        part_accessory AS (
            SELECT item_desc AS accessory FROM wardrobe 
            WHERE category = 'Accessories'
        )

        SELECT DISTINCT
        *
        FROM part_top AS a
        FULL OUTER JOIN part_bottom AS b
        FULL OUTER JOIN part_footwear AS c
        FULL OUTER JOIN part_accessory AS d
        ORDER BY rand()
        -- LIMIT 10
    )
""")

# COMMAND ----------

func_name = 'get_men_all_combinations'
func_comment = 'Get all possible combinations for men regardless of occassion'

spark.sql(f"""
    CREATE OR REPLACE FUNCTION 
    IDENTIFIER('{catalog_name}.{schema_name}.{func_name}')()
    RETURNS TABLE(
        top  STRING,
        bottom STRING,
        footwear STRING,
        accessory STRING
    )
    COMMENT '{func_comment}'
    LANGUAGE SQL
    RETURN (
        WITH wardrobe AS (
            SELECT * FROM dsag_dev_catalog.group_5.wardrobe_sample 
            WHERE gender IN ('Men', 'Unisex')
        ),

        part_top AS (
            SELECT item_desc AS top FROM wardrobe 
            WHERE category = 'Top'
        ),

        part_bottom AS (
            SELECT item_desc AS bottom FROM wardrobe 
            WHERE category = 'Bottom'
        ),

        part_footwear AS (
            SELECT item_desc AS footwear FROM wardrobe 
            WHERE category = 'Footwear'
        ),

        part_accessory AS (
            SELECT item_desc AS accessory FROM wardrobe 
            WHERE category = 'Accessories'
        )

        SELECT DISTINCT
        *
        FROM part_top AS a
        FULL OUTER JOIN part_bottom AS b
        FULL OUTER JOIN part_footwear AS c
        FULL OUTER JOIN part_accessory AS d
        ORDER BY rand()
        -- LIMIT 10
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions: Women

# COMMAND ----------

func_name = 'get_women_formal_combinations'
func_comment = 'Get all possible formal combinations for women'

spark.sql(f"""
    CREATE OR REPLACE FUNCTION 
    IDENTIFIER('{catalog_name}.{schema_name}.{func_name}')()
    RETURNS TABLE(
        top  STRING,
        bottom STRING,
        footwear STRING,
        accessory STRING,
        bag STRING
    )
    COMMENT '{func_comment}'
    LANGUAGE SQL
    RETURN (
        WITH wardrobe AS (
            SELECT * FROM dsag_dev_catalog.group_5.wardrobe_sample 
            WHERE occassion = 'Formal' AND gender IN ('Women', 'Unisex')
        ),

        part_top AS (
            SELECT item_desc AS top FROM wardrobe 
            WHERE category = 'Top'
        ),

        part_bottom AS (
            SELECT item_desc AS bottom FROM wardrobe 
            WHERE category = 'Bottom'
        ),

        part_footwear AS (
            SELECT item_desc AS footwear FROM wardrobe 
            WHERE category = 'Footwear'
        ),

        part_accessory AS (
            SELECT item_desc AS accessory FROM wardrobe 
            WHERE category = 'Accessories'
        ),

        part_bag AS (
            SELECT item_desc AS bag FROM wardrobe 
            WHERE category = 'Bag'
        )     

        SELECT DISTINCT
            a.top,
            CASE WHEN CONTAINS(lower(a.top), 'dress') THEN '' ELSE b.bottom END AS bottom,
            c.footwear,
            d.accessory,
            e.bag
        FROM part_top AS a
        FULL OUTER JOIN part_bottom AS b
        FULL OUTER JOIN part_footwear AS c
        FULL OUTER JOIN part_accessory AS d
        FULL OUTER JOIN part_bag AS e
        ORDER BY rand()
        -- LIMIT 10
    )
""")

# COMMAND ----------

func_name = 'get_women_casual_combinations'
func_comment = 'Get all possible casual combinations for women'

spark.sql(f"""
    CREATE OR REPLACE FUNCTION 
    IDENTIFIER('{catalog_name}.{schema_name}.{func_name}')()
    RETURNS TABLE(
        top  STRING,
        bottom STRING,
        footwear STRING,
        accessory STRING,
        bag STRING
    )
    COMMENT '{func_comment}'
    LANGUAGE SQL
    RETURN (
        WITH wardrobe AS (
            SELECT * FROM dsag_dev_catalog.group_5.wardrobe_sample 
            WHERE occassion = 'Casual' AND gender IN ('Women', 'Unisex')
        ),

        part_top AS (
            SELECT item_desc AS top FROM wardrobe 
            WHERE category = 'Top'
        ),

        part_bottom AS (
            SELECT item_desc AS bottom FROM wardrobe 
            WHERE category = 'Bottom'
        ),

        part_footwear AS (
            SELECT item_desc AS footwear FROM wardrobe 
            WHERE category = 'Footwear'
        ),

        part_accessory AS (
            SELECT item_desc AS accessory FROM wardrobe 
            WHERE category = 'Accessories'
        ),

        part_bag AS (
            SELECT item_desc AS bag FROM wardrobe 
            WHERE category = 'Bag'
        )     

        SELECT DISTINCT
            a.top,
            CASE WHEN CONTAINS(lower(a.top), 'dress') THEN '' ELSE b.bottom END AS bottom,
            c.footwear,
            d.accessory,
            e.bag
        FROM part_top AS a
        FULL OUTER JOIN part_bottom AS b
        FULL OUTER JOIN part_footwear AS c
        FULL OUTER JOIN part_accessory AS d
        FULL OUTER JOIN part_bag AS e
        ORDER BY rand()
        -- LIMIT 10
    )
""")

# COMMAND ----------

func_name = 'get_women_all_combinations'
func_comment = 'Get all possible combinations for women regardless of occassion'

spark.sql(f"""
    CREATE OR REPLACE FUNCTION 
    IDENTIFIER('{catalog_name}.{schema_name}.{func_name}')()
    RETURNS TABLE(
        top  STRING,
        bottom STRING,
        footwear STRING,
        accessory STRING,
        bag STRING
    )
    COMMENT '{func_comment}'
    LANGUAGE SQL
    RETURN (
        WITH wardrobe AS (
            SELECT * FROM dsag_dev_catalog.group_5.wardrobe_sample 
            WHERE gender IN ('Women', 'Unisex')
        ),

        part_top AS (
            SELECT item_desc AS top FROM wardrobe 
            WHERE category = 'Top'
        ),

        part_bottom AS (
            SELECT item_desc AS bottom FROM wardrobe 
            WHERE category = 'Bottom'
        ),

        part_footwear AS (
            SELECT item_desc AS footwear FROM wardrobe 
            WHERE category = 'Footwear'
        ),

        part_accessory AS (
            SELECT item_desc AS accessory FROM wardrobe 
            WHERE category = 'Accessories'
        ),

        part_bag AS (
            SELECT item_desc AS bag FROM wardrobe 
            WHERE category = 'Bag'
        )     

        SELECT DISTINCT
            a.top,
            CASE WHEN CONTAINS(lower(a.top), 'dress') THEN '' ELSE b.bottom END AS bottom,
            c.footwear,
            d.accessory,
            e.bag
        FROM part_top AS a
        FULL OUTER JOIN part_bottom AS b
        FULL OUTER JOIN part_footwear AS c
        FULL OUTER JOIN part_accessory AS d
        FULL OUTER JOIN part_bag AS e
        ORDER BY rand()
        -- LIMIT 10
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python Functions

# COMMAND ----------

# def get_combination() -> str:
#     """
#     Returns all possible combinations from a given dataframe.

#     Returns:
#     df_combi (pd.DataFrame): the dataframe containing all combinations.
#     """

#     df = spark.table('dsag_dev_catalog.group_5.wardrobe_sample').toPandas()

#     categories = ['top', 'bottom', 'footwear', 'accessories']

#     bottoms = df[df['category']=='Bottom']['clothing_id'].tolist()
#     tops = df[df['category']=='Top']['clothing_id'].tolist()
#     footwears = df[df['category']=='Footwear']['clothing_id'].tolist()
#     accessories = df[df['category']=='Accessories']['clothing_id'].tolist()

#     combi_list = list(itertools.product(tops, bottoms, footwears, accessories))

#     df_combi = pd.DataFrame(combi_list, columns=categories)
#     df_combi['combination_id'] = df_combi[categories].apply(lambda row: ''.join(row.values.astype(str)), axis=1)

#     for cat in categories:
#         df_combi[f'{cat}'] = df_combi[cat].astype(str).map(df.set_index('clothing_id')['item_desc'])

#     return df_combi

# def get_men_formal_combinations() -> str:
#     """
#     Returns men and unisex formal combinations from a given dataframe.

#     Returns:
#     df_combi (pd.DataFrame): the dataframe containing all combinations.
#     """
#     df = spark.table('dsag_dev_catalog.group_5.wardrobe_sample').toPandas()
#     df_filtered = df[(df['occassion']=='Formal') & (df['gender'].isin(['Men', 'Unisex']))]
#     df_combi = get_combination()
#     return df_combi

# def get_women_formal_combinations() -> str:
#     """
#     Returns women and unisex formal combinations from a given dataframe.

#     Returns:
#     df_combi (pd.DataFrame): the dataframe containing all combinations.
#     """
#     df = spark.table('dsag_dev_catalog.group_5.wardrobe_sample').toPandas()
#     df_filtered = df[(df['occassion']=='Formal') & (df['gender'].isin(['Women', 'Unisex']))]
#     df_combi = get_combination()
#     return df_combi

# def get_men_casual_combinations() -> str:
#     """
#     Returns men and unisex casual combinations from a given dataframe.

#     Returns:
#     df_combi (pd.DataFrame): the dataframe containing all combinations.
#     """
#     df = spark.table('dsag_dev_catalog.group_5.wardrobe_sample').toPandas()
#     df_filtered = df[(df['occassion']=='Casual') & (df['gender'].isin(['Men', 'Unisex']))]
#     df_combi = get_combination()
#     return df_combi

# def get_women_casual_combinations() -> str:
#     """
#     Returns women and unisex casual combinations from a given dataframe.

#     Returns:
#     df_combi (pd.DataFrame): the dataframe containing all combinations.
#     """
#     df = spark.table('dsag_dev_catalog.group_5.wardrobe_sample').toPandas()
#     df_filtered = df[(df['occassion']=='Casual') & (df['gender'].isin(['Women', 'Unisex']))]
#     df_combi = get_combination()
#     return df_combi

# COMMAND ----------

df = pd.read_csv('Data/DSAG Hackathon 2025 - Sample V1 (1).csv', dtype={'clothing_id':str})
# df.replace('Unspecified', '', inplace=True)
df.drop(['age'], axis=1, inplace=True)
df.fillna('', inplace=True)
df['clothing_id'] = df['clothing_id'].str.pad(3, fillchar='0')
df['item_desc'] = df[['pattern', 'subcolor', 'brand_name', 'subcategory']].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
df.head(10)

# COMMAND ----------

# sparkdf = spark.createDataFrame(df)
# sparkdf.createOrReplaceTempView('sparkdf')

# spark.sql(f"""
#     CREATE OR REPLACE TABLE
#     dsag_dev_catalog.group_5.wardrobe_sample
#     AS
#     SELECT * FROM sparkdf
# """)

# COMMAND ----------

# get_combination()

# COMMAND ----------

# get_men_casual_combinations()

# COMMAND ----------

# python_tool_uc_info = client.create_python_function(func=get_combination, catalog=catalog_name, schema=schema_name, replace=True)
# print(f"Deployed Unity Catalog function name: {python_tool_uc_info.full_name}")
# python_tool_uc_info = client.create_python_function(func=get_men_formal_combinations, catalog=catalog_name, schema=schema_name, replace=True)
# print(f"Deployed Unity Catalog function name: {python_tool_uc_info.full_name}")
# python_tool_uc_info = client.create_python_function(func=get_men_casual_combinations, catalog=catalog_name, schema=schema_name, replace=True)
# print(f"Deployed Unity Catalog function name: {python_tool_uc_info.full_name}")
# python_tool_uc_info = client.create_python_function(func=get_women_formal_combinations, catalog=catalog_name, schema=schema_name, replace=True)
# print(f"Deployed Unity Catalog function name: {python_tool_uc_info.full_name}")
# python_tool_uc_info = client.create_python_function(func=get_women_casual_combinations, catalog=catalog_name, schema=schema_name, replace=True)
# print(f"Deployed Unity Catalog function name: {python_tool_uc_info.full_name}")

# COMMAND ----------

# rules = pd.read_csv('Data/DSAG Hackathon 2025 - Combi Value.csv')
# rules.columns = ['fashion_rules', 'description']
# rules

# COMMAND ----------

# sparkdf = spark.createDataFrame(rules)
# sparkdf.createOrReplaceTempView('sparkdf')

# spark.sql(f"""
#     CREATE OR REPLACE TABLE
#     dsag_dev_catalog.group_5.wardrobe_policies
#     AS
#     SELECT * FROM sparkdf
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy AI

# COMMAND ----------

workspace_url = spark.conf.get('spark.databricks.workspaceUrl')
workspace_url

# COMMAND ----------

workspace_url = spark.conf.get('spark.databricks.workspaceUrl')
html_link = f'<a href="https://{workspace_url}/explore/data/functions/{catalog_name}/{schema_name}/get_todays_date" target="_blank">Go to Unity Catalog to see Registered Functions</a>'
display(HTML(html_link))

# COMMAND ----------

# DBTITLE 1,Create link to AI Playground
html_link = f'<a href="https://{workspace_url}/ml/playground" target="_blank">Go to AI Playground</a>'
display(HTML(html_link))
