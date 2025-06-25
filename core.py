from openai import OpenAI


class Model():

    def __init__(self, dir=None):
        self.dir = dir
        self.client = OpenAI(
            api_key="",
            base_url="https://dbc-1d173222-f5c8.cloud.databricks.com/serving-endpoints"
        )

    # chat
    def chat(self, message):
        response = self.client.chat.completions.create(
            model="databricks-claude-sonnet-4",
            messages=[
                {
                    "role": "user",
                    "content": message
                }
            ]
        )
        return response.choices[0].message.content


