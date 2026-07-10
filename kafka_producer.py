import json
from typing import Union
from kafka import KafkaProducer
import time
import random 
import uuid
import threading
from datetime import datetime
from dataclasses import dataclass, asdict
import numpy as np


BETAS = {
    "MOTTATT":           {"UNDER_BEHANDLING": 0.0},
    "UNDER_BEHANDLING":  {"VEDTAK_INNVILGET": -0.4, "VEDTAK_AVSLÅTT": 0.3, "MOTTATT": 0.2},
    "VEDTAK_INNVILGET":  {"FERDIG": 0.0},
    "VEDTAK_AVSLÅTT":    {"FERDIG": 0.0},
    "FERDIG":            {},# terminal
}

TRANSITIONS = {
    "MOTTATT":           {"UNDER_BEHANDLING": 1/2},
    "UNDER_BEHANDLING":  {"VEDTAK_INNVILGET": 1/20, "VEDTAK_AVSLÅTT": 1/30, "MOTTATT": 1/10},
    "VEDTAK_INNVILGET":  {"FERDIG": 1/30},
    "VEDTAK_AVSLÅTT":    {"FERDIG": 1/2},
    "FERDIG":            {},   # terminal
}

FIRST_NAMES = [
    "Kari", "Ola", "Ingrid", "Erik", "Maja", "Thomas", "Anna", "Lars",
    "Emma", "Jonas", "Nora", "Andreas", "Sofie", "Magnus", "Ida",
    "Sander", "Emilie", "Henrik", "Thea", "Kristian", "Julie", "Aksel",
    "Sara", "Oskar", "Mathilde", "Fredrik", "Amalie", "William", "Vilde",
    "Isak", "Marte", "Tobias", "Frida", "Martin", "Live", "Sondre",
    "Hanna", "Daniel", "Malin", "Elias", "Astrid", "Jakob", "Selma",
    "Noah", "Alma", "Filip", "Tuva", "Adrian", "Mia", "Leo",
    "Sigrid", "Håkon", "Marie", "Bjørn", "Silje", "Petter", "Camilla",
    "Espen", "Marius", "Cecilie", "Torstein", "Eline", "Jørgen", "Linnea",
    "Ivar", "Kristine", "Kåre", "Solveig", "Vegard", "Nina", "Trygve",
    "Ragnhild", "Odd", "Berit", "Arne", "Gunnar", "Turid", "Sverre",
    "Liv", "Rolf", "Eva", "Knut", "Kirsten", "Geir", "Randi", "Terje",
    "Anne", "Stein", "Bente", "Ove", "Wenche", "Roar", "Hilde",
    "Jan", "Tone", "Bjørnar", "Marianne", "Trond", "Elisabeth", "Frode",
    "Katrine", "Snorre", "Gro", "Ottar", "Synnøve", "Halvor", "Aslaug"
]

LAST_NAMES = [
    "Nordmann", "Hansen", "Bakke", "Dahl", "Lie", "Berg", "Andersen",
    "Johansen", "Olsen", "Larsen", "Pedersen", "Nilsen", "Kristiansen",
    "Jensen", "Karlsen", "Johnsen", "Pettersen", "Eriksen", "Haugen",
    "Halvorsen", "Solberg", "Iversen", "Moen", "Strand", "Solheim",
    "Rasmussen", "Berge", "Nygård", "Fossum", "Rønning", "Aas",
    "Sæther", "Skoglund", "Lunde", "Vik", "Aune", "Sørensen", "Braaten",
    "Bergli", "Amundsen", "Wold", "Sundby", "Wiik", "Aarnes",
    "Gundersen", "Jacobsen", "Moe", "Ødegård", "Fjeld", "Haugland",
    "Vold", "Skaug", "Aasen", "Bruun", "Solvik", "Tangen", "Løvås",
    "Marthinsen", "Storli", "Bratli", "Vangen", "Grønli", "Kaasa",
    "Bjørnstad", "Hagen", "Reinholt", "Krogh", "Steen", "Enger",
    "Fagerheim", "Nordby", "Sundal", "Lien", "Toft", "Kvam", "Skarstein",
    "Aalvik", "Rise", "Nordahl", "Skei", "Grande", "Frydenlund",
    "Winther", "Bratland", "Sæterøy", "Wibe", "Hovland", "Melby",
    "Løken", "Fredheim", "Bakken", "Kolstad", "Ruud", "Solum", "Alm"
]
KOMMUNER = [
    "Oslo", "Bergen", "Trondheim", "Stavanger", "Tromsø", "Kristiansand",
    "Fredrikstad", "Sandnes", "Drammen", "Skien", "Sarpsborg", "Ålesund",
    "Sandefjord", "Haugesund", "Tønsberg", "Moss", "Porsgrunn", "Bodø",
    "Arendal", "Hamar", "Larvik", "Halden", "Lillehammer", "Molde",
    "Harstad", "Kongsberg", "Gjøvik", "Askøy", "Bærum", "Asker",
    "Lørenskog", "Lillestrøm", "Ullensaker", "Ringerike", "Steinkjer",
    "Kongsvinger", "Alta", "Narvik", "Elverum", "Levanger", "Voss",
    "Kristiansund", "Rana", "Gjesdal", "Grimstad", "Notodden", "Bamble",
    "Nesodden", "Nittedal", "Karmøy"
]

@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    user_id: str
    time_created: str

@dataclass(frozen=True)
class User:
    user_id: str
    name: str
    age: int
    kommune: str

@dataclass
class UserProcess:
    user_id: str
    state: str

def get_next_event_state(user_process: UserProcess):
    rnd_number = random.random()
    possible_events = TRANSITIONS[user_process.state]
    if not possible_events:
        return None                        # terminal state
    for possible_event in possible_events:
        min_nmbr, max_nmbr = possible_event[1]
        if min_nmbr < rnd_number < max_nmbr:
            return possible_event[0]
    return None

def draw_from_exponential(rate):
    waiting_time = np.random.exponential(1/rate)
    return waiting_time

def update_user_process(user_process: UserProcess, event_state: str) -> UserProcess:
    user_process.state = event_state
    return user_process

def generate_rnd_user(user_count_id: int) -> User:
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    kommune = random.choice(KOMMUNER)
    return User(user_id=f"{first_name}.{last_name}.{user_count_id}", name=f"{first_name} {last_name}", kommune=kommune, age=random.randrange(16, 80))

def create_producer() -> KafkaProducer:
    """Create a connection to Kafka broker"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
    )
    return producer

def send_data(producer: KafkaProducer, topic: str, data: Event | User):
    producer.send(
        topic,
        key=data.user_id.encode("utf-8"),  # same user → same partition → ordered
        value=json.dumps(asdict(data)).encode("utf-8")
    )

def run_user_process(producer: KafkaProducer, user: User, case_complexity: float):
    user_process = UserProcess(user.user_id, state="MOTTATT")
    time.sleep(0.1)
    while True:
        params = TRANSITIONS[user_process.state]
        betas = BETAS[user_process.state]   # 1. what can happen from HERE
        if not params:                              # 2. terminal check — before drawing anything
            print(f"[{user.name}] done — terminal state: {user_process.state}")
            break

        adjusted_rates = {dest: q0 * np.exp(betas[dest] * case_complexity) for dest, q0 in params.items()}
        q_i = sum(adjusted_rates.values())
        #q_i = sum(params.values())                  # 3. total rate out of here
        waiting_time = draw_from_exponential(q_i)    # 4. how long until we leave
        event_state = random.choices(list(adjusted_rates.keys()), weights=list(adjusted_rates.values()))[0]  # 5. where we go
        time.sleep(waiting_time)                     # 6. actually wait

        user_process = update_user_process(user_process, event_state)  # 7. commit the move
        event = Event(
            event_id=f"{user_process.user_id}-{event_state}-{uuid.uuid4()}",
            event_type=event_state,
            user_id=user_process.user_id,
            time_created=datetime.now().isoformat(),
        )
        send_data(producer=producer, topic="events", data=event)


def run_producer():
    """Run the producer, sending a few messages"""
    producer = create_producer()
    user_count = 0
    try:
        while True:
            user = generate_rnd_user(user_count)
            mean_complexity = 2.0 - 0.03 * user.age   # e.g. age 16 -> mean 1.52; age 80 -> mean -0.4
            case_complexity = np.random.normal(loc=mean_complexity, scale=0.5)
            send_data(producer=producer, topic="users", data=user)

            user_count += 1
            print(f"\n[generator] new user #{user_count}: {user.name} from {user.kommune}")

                        # each user gets their own thread
            t = threading.Thread(
                target=run_user_process,
                args=(producer, user, case_complexity),
                daemon=True,
            )
            t.start()

            time.sleep(random.uniform(0.5, 1.5))
    finally:
        producer.flush()
        producer.close()
        print("Producer closed")

if __name__ == "__main__":
    run_producer()