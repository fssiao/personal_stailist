# Databricks notebook source
# MAGIC %md
# MAGIC ## Import Packages

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow<3 langchain langgraph==0.3.4 databricks-connect==16.1.5 databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] uv
# MAGIC %restart_python

# COMMAND ----------

from agent import tools, LLM_ENDPOINT_NAME, AGENT
from databricks import agents
from databricks_langchain import VectorSearchRetrieverTool
from databricks.sdk import WorkspaceClient
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from unitycatalog.ai.langchain.toolkit import UnityCatalogTool
from IPython.display import display, HTML

import os
import pandas as pd
import mlflow

# COMMAND ----------

# DBTITLE 1,Quick test to see if Agent works
AGENT.predict({"messages": [{"role": "user", "content": "Give me one formal wear for women."}]})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Log the agent as code from the [agent]($./agent) notebook. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------

# TODO: Manually include underlying resources if needed. See the TODO in the markdown above for more information.
resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME)]
for tool in tools:
    if isinstance(tool, VectorSearchRetrieverTool):
        resources.extend(tool.resources)
    elif isinstance(tool, UnityCatalogTool):
        resources.append(DatabricksFunction(function_name=tool.uc_function_name))

input_example = {
    "messages": [
        {
            "role": "user",
            "content": "Give me one formal wear for women."
        }
    ]
}

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model="agent.py",
        input_example=input_example,
        resources=resources,
        extra_pip_requirements=[
            "databricks-connect"
        ]
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the agent with [Agent Evaluation](https://docs.databricks.com/generative-ai/agent-evaluation/index.html)
# MAGIC
# MAGIC You can edit the requests or expected responses in your evaluation dataset and run evaluation as you iterate your agent, leveraging mlflow to track the computed quality metrics.

# COMMAND ----------

data = {
    "request": [
        "What can men wear for a date?",
        "Give me office or professional attires for women.",
        "Give me any combination with a dress.",
    ],
    "expected_facts": [
        [
            "Date attires should only include formal wear.",
        ],
        [
            "Office attires should only include formal wear.",
            "Office attire should not include stilettos and pumps footwear.",
            "Office attire should not include sneakers.",
            "Office attire and formal occassion should not include denim pants.",
            "Office attire should not include hat/cap accessories."
        ],
        [
            "Dresses should not include bottom wear such as pants."
        ]
    ]
}

eval_dataset = pd.DataFrame(data)

# COMMAND ----------

# DBTITLE 1,Run MLflow Evaluations!
with mlflow.start_run(run_id=logged_agent_info.run_id):
    eval_results = mlflow.evaluate(
        f"runs:/{logged_agent_info.run_id}/agent",  # replace `chain` with artifact_path that you used when calling log_model.
        data=eval_dataset,  # Your evaluation dataset
        model_type="databricks-agent",  # Enable Mosaic AI Agent Evaluation
    )

# Review the evaluation results in the MLFLow UI (see console output), or access them in place:
display(eval_results.tables['eval_results'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# Use the workspace client to retrieve information about the current user
w = WorkspaceClient()
user_email = w.current_user.me().display_name
username = user_email.split("@")[0]

# Catalog and schema have been automatically created thanks to lab environment
catalog_name = "dsag_dev_catalog"
schema_name = "group_5"

# TODO: define the catalog, schema, and model name for your UC model
model_name = "wearalyzer"
UC_MODEL_NAME = f"{catalog_name}.{schema_name}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME)

# COMMAND ----------

# Retrieve the Databricks host URL
workspace_url = spark.conf.get('spark.databricks.workspaceUrl')

# Create HTML link to created agent
html_link = f'<a href="https://{workspace_url}/explore/data/models/{catalog_name}/{schema_name}/product_agent" target="_blank">Go to Unity Catalog to see Registered Agent</a>'
display(HTML(html_link))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent
# MAGIC
# MAGIC ##### Note: This is disabled for lab users but will work on your own workspace

# COMMAND ----------

# Deploy the model to the review app and a model serving endpoint
agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version, tags = {"endpointSource": "Lab Admin"})

# COMMAND ----------


