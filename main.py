import psycopg
from dataclasses import dataclass
import pandas as pd 
import matplotlib.pyplot as plt

@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    user_id: str
    time_created: str

def get_connection():
    return psycopg.connect("postgres://user:password@localhost:5432/orders_db")

def fetch_events(conn: psycopg.Connection):
    query = "SELECT event_id, user_id, event_type, time_created FROM events"
    events = []
    with conn.cursor() as cur:
        cur.execute(query)
        for event_id, user_id, event_type, time_created in cur:
            events.append({"event_id": event_id, "user_id": user_id, "event_type": event_type, "time_created":time_created})
    return events

def main():
    connection = get_connection()
    events = fetch_events(conn=connection)
    df = pd.DataFrame(events)
    df["time_created"] = pd.to_datetime(df["time_created"], utc=True)
    df = df.sort_values(["user_id", "time_created"])
    df["prev_state"] = df.groupby("user_id")["event_type"].shift(1)
    df["prev_time"] = df.groupby("user_id")["time_created"].shift(1)

    completed = df.dropna(subset=["prev_state"]).copy()
    completed["duration"] = (completed["time_created"] - completed["prev_time"]).dt.total_seconds()

    now = df["time_created"].max()

    last_per_user = df.groupby("user_id").tail(1)
    still_open = last_per_user[last_per_user["event_type"] != "FERDIG"].copy()
    still_open["censored_duration"] = (now - still_open["time_created"]).dt.total_seconds()

    N = completed.groupby(["prev_state", "event_type"]).size()

    T_completed = completed.groupby("prev_state")["duration"].sum()
    T_censored = still_open.groupby("event_type")["censored_duration"].sum()
    T = T_completed.add(T_censored, fill_value=0)
    q_hat = N / T   # T's index (source state) aligns automatically with N's first index level
    print(q_hat)

if __name__ == "__main__":
    main()
