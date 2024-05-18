import os
import json
import uuid

import azure.functions as func
import logging
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
IOTHUB_CONNECTION_STRING_NAME = os.getenv("IOTHUB_CONNECTION_STRING_NAME")

app = func.FunctionApp()
client = psycopg2.connect(DATABASE_URL)


@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name=EVENT_HUB_NAME,  # FIXME: Change to your own iothub event_hub_name
    connection=IOTHUB_CONNECTION_STRING_NAME,  # Remember to add the IOTHUB_CONNECTION_STRING environment variable in your Function App
)
def eventhub_trigger1(azeventhub: func.EventHubEvent):
    logging.info(
        "Python EventHub trigger processed an event: %s",
        azeventhub.get_body().decode("utf-8"),
    )

    msg = json.loads(azeventhub.get_body().decode("utf-8"))
    msg["id"] = str(uuid.uuid4())
    logging.info(f"Message: {msg.get('device_id')}")
