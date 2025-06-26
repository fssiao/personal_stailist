# Databricks notebook source
# MAGIC %md
# MAGIC # Hands-On Lab: Building Agent Systems with Databricks
# MAGIC
# MAGIC ## Part 2 - Agent Evaluation
# MAGIC Now that we've created an agent, how do we evaluate its performance?
# MAGIC For the second part, we're going to create a product support agent so we can focus on evaluation.
# MAGIC This agent will use a RAG approach to help answer questions about products using the product documentation.
# MAGIC
# MAGIC ### 2.1 Define our new Agent and retriever tool
# MAGIC - [**agent.py**]($./agent.py): An example Agent has been configured - first we'll explore this file and understand the building blocks
# MAGIC - **Vector Search**: We've created a Vector Search endpoint that can be queried to find related documentation about a specific product.
# MAGIC - **Create Retriever Function**: Define some properties about our retriever and package it so it can be called by our LLM.
# MAGIC
# MAGIC ### 2.2 Create Evaluation Dataset
# MAGIC - We've provided an example evaluation dataset - though you can also generate this [synthetically](https://www.databricks.com/blog/streamline-ai-agent-evaluation-with-new-synthetic-data-capabilities).
# MAGIC
# MAGIC ### 2.3 Run MLflow.evaluate() 
# MAGIC - MLflow will take your evaluation dataset and test your agent's responses against it
# MAGIC - LLM Judges will score the outputs and collect everything in a nice UI for review
# MAGIC
# MAGIC ### 2.4 Make Needed Improvements and re-run Evaluations
# MAGIC - Take feedback from our evaluation run and change retrieval settings
# MAGIC - Run evals again and see the improvement!

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow<3 langchain langgraph==0.3.4 databricks-connect==16.1.5 databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] uv
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Quick test to see if Agent works
from agent import AGENT

AGENT.predict({"messages": [{"role": "user", "content": "Can you give me troubleshooting tips for my Soundwave X5 Pro Headphones?"}]})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Log the agent as code from the [agent]($./agent) notebook. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------

# Determine Databricks resources to specify for automatic auth passthrough at deployment time
import mlflow
from agent import tools, LLM_ENDPOINT_NAME
from databricks_langchain import VectorSearchRetrieverTool
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from unitycatalog.ai.langchain.toolkit import UnityCatalogTool

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
            "content": "Can you give me some troubleshooting steps for SoundWave X5 Pro Headphones that won't connect to my phone?"
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

import pandas as pd

data = {
    "request": [
        "What color options are available for the Aria Modern Bookshelf?",
        "How should I clean the Aurora Oak Coffee Table to avoid damaging it?",
        "How should I clean the BlendMaster Elite 4000 after each use?",
        "How many colors is the Flexi-Comfort Office Desk available in?",
        "What sizes are available for the StormShield Pro Men's Weatherproof Jacket?",
        "What should I do to optimize battery life for my SmartX Pro 10 Smartphone?",
        "How many people can the Elegance Extendable Dining Table seat comfortably?",
        "What colors is the Urban Explorer Windbreaker available in?",
        "What is the water resistance rating of the BrownBox SwiftWatch X500?",
        "What colors are available for the StridePro Runner?"
    ],
    "expected_facts": [
        [
            "The Aria Modern Bookshelf is available in natural oak finish",
            "The Aria Modern Bookshelf is available in black finish",
            "The Aria Modern Bookshelf is available in white finish"
        ],
        [
            "Use a soft, slightly damp cloth for cleaning.",
            "Avoid using abrasive cleaners."
        ],
        [
            "The jar of the BlendMaster Elite 4000 should be rinsed.",
            "Rinse with warm water.",
            "The cleaning should take place after each use."
        ],
        [
            "The Flexi-Comfort Office Desk is available in three colors."
        ],
        [
            "The available sizes for the StormShield Pro Men's Weatherproof Jacket are Small, Medium, Large, XL, and XXL."
        ],
        [
            "Reduce screen brightness.",
            "Enable dark mode.",
            "Limit screen time"
        ],
        [
            "The Elegance Extendable Dining Table can comfortably seat 6 people."
        ],
        [
            "The Urban Explorer Windbreaker is available in navy blue, forest green, and charcoal gray."
        ],
        [
            "The water resistance rating of the BrownBox SwiftWatch X500 is 5 ATM."
        ],
        [
            "The colors available for the StridePro Runner should include Midnight Blue.",
            "The colors available for the StridePro Runner should include Electric Red.",
            "The colors available for the StridePro Runner should include Forest Green."
        ]
    ]
}

eval_dataset = pd.DataFrame(data)

# COMMAND ----------

# DBTITLE 1,Run MLflow Evaluations!
import mlflow
import pandas as pd

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
# MAGIC ## Lets go back to the [agent.py]($./agent.py) file and change our retriever to 1.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import os

mlflow.set_registry_uri("databricks-uc")

# Use the workspace client to retrieve information about the current user
w = WorkspaceClient()
user_email = w.current_user.me().display_name
username = user_email.split("@")[0]

# Catalog and schema have been automatically created thanks to lab environment
catalog_name = f"{username}_vocareum_com"
schema_name = "agents"

# TODO: define the catalog, schema, and model name for your UC model
model_name = "product_agent"
UC_MODEL_NAME = f"{catalog_name}.{schema_name}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME)

# COMMAND ----------

from IPython.display import display, HTML

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

from databricks import agents

# Deploy the model to the review app and a model serving endpoint
# agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version, tags = {"endpointSource": "Lab Admin"})
