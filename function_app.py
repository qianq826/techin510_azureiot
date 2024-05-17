import os
import azure.functions as func
import logging
from azure.cosmos import CosmosClient

COSMOSDB_URL = os.getenv("COSMOSDB_URL")
COSMOSDB_KEY = os.getenv("COSMOSDB_KEY")

app = func.FunctionApp()
client = CosmosClient(COSMOSDB_URL, credential=COSMOSDB_KEY)


@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name="iothub-ehub-techin510-59467401-6d881a7ae1",  # FIXME: Change to your own iothub event_hub_name
    connection="IOTHUB_CONNECTION_STRING",  # Remember to add the IOTHUB_CONNECTION_STRING environment variable in your Function App
)
def eventhub_trigger1(azeventhub: func.EventHubEvent):
    logging.info(
        "Python EventHub trigger processed an event: %s",
        azeventhub.get_body().decode("utf-8"),
    )
    
