from kafka import KafkaProducer
import json
import time
import random
import uuid
import numpy as np
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

Event_NameSPACE = "bmwanalytics"
Event_HOSTNAME = "bmwspace.servicebus.windows.net"

CONNECTION_STRING = "Endpoint=sb://bmwspace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mNpMxG5JfRoypFjy3d8WCoMX3QOyooR91+AEhOg++c8="

producer = KafkaProducer(
    bootstrap_servers=f"{Event_HOSTNAME}:9093",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
    
)
CSV_FILE_PATH = "./CSV/BMW sales data (2010-2024).csv"
df = pd.read_csv(CSV_FILE_PATH)

# Build clean lists (avoid NaN)
models = df["Model"].dropna().tolist()
years = df["Year"].dropna().tolist()
regions = df["Region"].dropna().tolist()
colors = df["Color"].dropna().tolist()
fuel_types = df["Fuel_Type"].dropna().tolist()
transmissions = df["Transmission"].dropna().tolist()
engine_sizes = df["Engine_Size_L"].dropna().tolist()
mileages = df["Mileage_KM"].dropna().tolist()
prices = df["Price_USD"].dropna().tolist()
sales_volumes = df["Sales_Volume"].dropna().tolist()
sales_classifications = df["Sales_Classification"].dropna().tolist()
def send_bmw_data():
    row = {
        "Model": random.choice(models),
        "Year": random.choice(years),
        "Region": random.choice(regions),
        "Color": random.choice(colors),
        "Fuel_Type": random.choice(fuel_types),
        "Transmission": random.choice(transmissions),
        "Engine_Size_L": random.choice(engine_sizes),
        "Mileage_KM": random.choice(mileages),
        "Price_USD": random.choice(prices),
        "Sales_Volume": random.choice(sales_volumes),
        "Sales_Classification": random.choice(sales_classifications)
    }
    
    event = {
        "event_id":str(uuid.uuid4()),
        "event_timestamp":datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        "vehicle_id":str(uuid.uuid4()),
        "model": row["Model"],
        "year": row["Year"],
        "region": row["Region"],
        "color": row["Color"],
        "fuel_type": row["Fuel_Type"],
        "transmission": row["Transmission"],
        "engine_size": row["Engine_Size_L"],
        "mileage": row["Mileage_KM"],
        "price": row["Price_USD"],
        "sales_volume": row["Sales_Volume"],
        "sales_classification": row["Sales_Classification"]
    }
    return event
if __name__ == "__main__":
    while True:
        event = send_bmw_data()
        producer.send(Event_NameSPACE,key=event["vehicle_id"].encode(),value=event)
        print(f"Sent event: {event}")
        time.sleep(5)
  