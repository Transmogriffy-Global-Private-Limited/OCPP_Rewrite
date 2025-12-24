import uuid
from datetime import datetime

from peewee import (
    CharField,
    DateTimeField,
    FloatField,
    IntegerField,
    Model,
)

from dbconn import get_database

# Get the database connection
db = get_database()


def getUUID():
    return str(uuid.uuid4())


class BaseModel(Model):
    class Meta:
        database = db

class Transaction(BaseModel):
    uuiddb = CharField(default=getUUID)
    charger_id = CharField()
    connector_id = IntegerField()
    meter_start = FloatField()
    meter_stop = FloatField(null=True)
    total_consumption = FloatField(null=True)
    start_time = DateTimeField()
    stop_time = DateTimeField(null=True)
    id_tag = CharField()
    transaction_id = IntegerField(unique=True, null=True)
    
    class Meta:
        table_name = "transactions"
        indexes = ((("charger_id", "connector_id", "id_tag"), False),)

class Analytics(BaseModel):
    uuiddb = CharField(default=getUUID)
    charger_id = CharField()
    timestamp = DateTimeField()
    total_uptime = CharField()  # Storing in a human-readable format
    uptime_percentage = FloatField()
    total_transactions = IntegerField()
    total_electricity_used_kwh = FloatField()
    occupancy_rate_percentage = FloatField()
    average_session_duration = CharField()  # Storing in a human-readable format
    peak_usage_times = CharField()  # Storing as a comma-separated string

    class Meta:
        table_name = "analytics"

# Create the tables
db.connect()
db.create_tables(
    [
        Transaction,
        Analytics
    ],
    safe=True,
)
