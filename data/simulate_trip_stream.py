import pandas as pd
import random
import time
from datetime import datetime

# Read CSVs
trip_begin_df = pd.read_csv('data/trip_start.csv')
trip_end_df = pd.read_csv('data/trip_end.csv')

# Tag events
trip_begin_events = trip_begin_df.to_dict(orient='records')
for event in trip_begin_events:
    event['event_type'] = 'trip_begin'

trip_end_events = trip_end_df.to_dict(orient='records')
for event in trip_end_events:
    event['event_type'] = 'trip_end'

# Combine and shuffle
all_events = trip_begin_events + trip_end_events
random.shuffle(all_events)

# Simulate streaming
for event in all_events:
    event['ingest_timestamp'] = datetime.utcnow().isoformat() + 'Z'
    print(event)  # Replace with your processing logic
    time.sleep(random.uniform(0.5, 1.5))  # Simulate delay
