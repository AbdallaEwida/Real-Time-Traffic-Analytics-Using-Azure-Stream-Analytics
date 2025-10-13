#!/usr/bin/env python3
# traffic_simulator_final_v5_with_eventhub_fixed_resume.py
# Enhanced version: streams events to Azure Event Hub + saves locally.
# Minimal fix: reliable resume of event_id and append to events file.

import os
import random
import json
import time
import uuid
from datetime import datetime
from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData

fake = Faker()

# --------------- CONFIG ---------------
NUM_LOCATIONS = 1000
SENSOR_COVERAGE_RATIO = 0.55
NUM_VEHICLES = 2000
TOTAL_EVENTS = 150_000
EVENT_INTERVAL_SECONDS = 0
DATA_DIR = "data"
LOCATIONS_FILE = os.path.join(DATA_DIR, "locations.json")
SENSORS_FILE = os.path.join(DATA_DIR, "sensors.json")
VEHICLES_FILE = os.path.join(DATA_DIR, "vehicles.json")
EVENTS_FILE = os.path.join(DATA_DIR, "events.json")

# ======== AZURE EVENT HUB CONFIG ========
# 🔹 Replace these with your actual values
EVENTHUB_CONNECTION_STR = (
    "Endpoint=sb://.servicebus.windows.net/;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey="
)
EVENTHUB_NAME = ""
# ========================================

ROAD_TYPE_WEIGHTS = {
    "Highway": 0.20,
    "Main Road": 0.30,
    "Intersection": 0.25,
    "Bridge": 0.10,
    "Urban Road": 0.15
}

VEHICLE_TYPE_DISTRIBUTION = {
    "Car": 0.65,
    "Truck": 0.10,
    "Van": 0.10,
    "Motorcycle": 0.10,
    "Bus": 0.05
}

VEHICLE_COLORS = [
    "White", "Black", "Silver", "Gray", "Red", "Blue", "Green", "Yellow", "Brown", "Orange"
]

VEHICLE_MODELS_BY_TYPE = {
    "Car": ["Toyota Corolla", "Hyundai Elantra", "Kia Cerato", "Nissan Sunny",
            "Renault Logan", "Mitsubishi Lancer", "Chevrolet Optra", "Peugeot 301",
            "Honda Civic", "Skoda Octavia", "BMW 320i", "Mercedes C180", "MG 5",
            "Chery Arrizo 5", "BYD F3", "Suzuki Swift", "Fiat Tipo", "Seat Ibiza",
            "Citroen C4", "Volkswagen Passat", "Opel Astra", "Ford Focus", "Audi A4", "Mazda 3"],
    "Truck": ["Volvo FH16", "Mercedes Actros", "MAN TGS", "Scania R-Series", "Isuzu FVZ",
              "Sinotruk Howo", "DFAC Cargo", "Iveco Stralis"],
    "Van": ["Ford Transit", "Mercedes Sprinter", "Renault Master", "Nissan Urvan",
            "Peugeot Boxer", "Fiat Ducato"],
    "Motorcycle": ["Yamaha YBR", "Honda CG125", "Honda CBR500R", "Kawasaki Ninja 400",
                   "Bajaj Pulsar", "TVS Apache", "Suzuki GSX"],
    "Bus": ["Mercedes Citaro", "Volvo 9700", "MAN Lion's City", "Scania Interlink",
            "Isuzu City Bus", "King Long"]
}

# --------------- STREET TO CITY MAPPING ---------------
STREET_TO_CITY = {
    "Salah Salem": "Cairo",
    "Nasr City": "Cairo",
    "Ring Road": "Cairo",
    "6th October Bridge": "Giza",
    "Corniche El Nil": "Cairo",
    "Tahrir": "Cairo",
    "Al Haram": "Giza",
    "El Nasr Road": "Cairo",
    "26 July": "Giza",
    "El Teseen": "Cairo",
    "El Geish": "Cairo",
    "El Thawra": "Cairo",
    "El Mokattam": "Cairo",
    "Ramses": "Cairo",
    "Dokki": "Giza",
    "Moustafa Kamel": "Cairo",
    "Kamel Ibrahim": "Cairo",
    "El Tayaran": "Cairo",
    "El Maadi Corniche": "Cairo",
    "Abbass Al Akkad": "Cairo",
    "El Omraniya": "Giza",
    "Gamaat El Dowal": "Cairo",
    "El Orouba": "Cairo",
    "Abdel Aziz Fahmy": "Cairo",
    "Wahat Road": "New Cairo",
    "Sheikh Zayed Road": "6th October",
    "October Corridor": "6th October",
    "El Shorouk Road": "New Cairo",
    "El Sakkakini": "Cairo",
    "El Marg": "Cairo",
    "El Nozha": "Cairo",
    "Heliopolis Avenue": "Cairo",
    "Masr El Gedida": "Cairo",
    "Sayeda Zeinab": "Cairo",
    "Giza Corniche": "Giza",
    "Mohandessin": "Giza",
    "El Qahera Street": "Cairo",
    "Smart Village Road": "6th October",
    "Al Rehab Road": "New Cairo",
    "El Obour Road": "Cairo",
    "Al Amiriya Road": "Cairo"
}

STREET_DIRECTIONS = {name: random.choice(
    ["NS", "EW", "CIRCULAR"]) for name in STREET_TO_CITY.keys()}

# ----------------- HELPERS -----------------


def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)


def weighted_choice(mapping):
    keys = list(mapping.keys())
    weights = list(mapping.values())
    return random.choices(keys, weights=weights, k=1)[0]


def jitter_coordinate(lat, lon, meters=500):
    dlat = (random.uniform(-1, 1) * meters) / 111111.0
    dlon = (random.uniform(-1, 1) * meters) / (111111.0 * max(0.1, abs(lat)))
    return round(lat + dlat, 6), round(lon + dlon, 6)


def slugify(s):
    s2 = "".join(c if c.isalnum() else "_" for c in s)
    while "__" in s2:
        s2 = s2.replace("__", "_")
    return s2.strip("_")[:60]

# ----------------- Robust get_last_event_id -----------------


def get_last_event_id(file_path):
    """
    Always return the last numeric event_id from events.json.
    Works even if the file ends with blank lines or partial writes.
    (This implementation scans forward and records the last valid id.)
    """
    last_id = 0
    if not os.path.exists(file_path):
        return 0
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if "event_id" in obj:
                        last_id = int(obj["event_id"])
                except Exception:
                    # skip malformed/partial lines
                    continue
        return last_id
    except Exception as e:
        print("DEBUG → get_last_event_id error:", e)
        return 0

# ----------------- BUILDERS -----------------


def build_locations(target_count):
    locations = []
    anchors = []
    for name in STREET_TO_CITY.keys():
        base_lat = random.uniform(29.75, 30.20)
        base_lon = random.uniform(31.0, 31.55)
        anchors.append({"name": name, "lat": base_lat, "lon": base_lon})
    idx = 0
    while len(locations) < target_count:
        base = random.choice(anchors)
        idx += 1
        lat, lon = jitter_coordinate(
            base["lat"], base["lon"], meters=random.uniform(30, 1000))
        road_type = weighted_choice(ROAD_TYPE_WEIGHTS)
        speed_limit = random.choice([40, 50, 60, 70, 80, 100])
        city = STREET_TO_CITY.get(base["name"], "Cairo")
        direction = STREET_DIRECTIONS.get(base["name"], "Both")
        suffix = f" (segment {idx})" if random.random() < 0.35 else ""
        street_name = f"{base['name']}{suffix}"
        locations.append({
            "location_id": idx,
            "street_name": street_name,
            "city": city,
            "lat": lat,
            "lon": lon,
            "road_type": road_type,
            "speed_limit": speed_limit,
            "direction": direction
        })
    return locations


def build_sensors(locations, coverage_ratio=0.55):
    sensors = []
    target = int(len(locations) * coverage_ratio)
    picked = random.sample(locations, target)
    numeric = 1
    for i, loc in enumerate(picked, start=1):
        clean = slugify(loc["street_name"])
        kind = random.choice(
            ["CCTV", "Radar", "TrafficCounter", "MultiSensor"])
        status = random.choices(["Active", "Inactive", "Under Maintenance"],
                                weights=[0.8, 0.1, 0.1], k=1)[0]
        sid = f"{kind}_{clean}_{i:04d}"
        sensors.append({
            "sensor_id": sid,
            "numeric_id": numeric,
            "lat": round(loc["lat"] + random.uniform(-0.0009, 0.0009), 6),
            "lon": round(loc["lon"] + random.uniform(-0.0009, 0.0009), 6),
            "location_id": loc["location_id"],
            "install_date": fake.date_between(start_date='-5y', end_date='today').isoformat(),
            "status": status,
            "sensor_type": kind
        })
        numeric += 1
    return sensors


def build_vehicles_with_owner(total):
    types = list(VEHICLE_TYPE_DISTRIBUTION.keys())
    probs = [VEHICLE_TYPE_DISTRIBUTION[t] for t in types]
    counts = [int(round(p * total)) for p in probs]
    s = sum(counts)
    i = 0
    while s != total:
        if s < total:
            counts[i % len(counts)] += 1
            s += 1
        else:
            if counts[i % len(counts)] > 0:
                counts[i % len(counts)] -= 1
                s -= 1
        i += 1
    vehicles = []
    plates_seen = set()
    for t, cnt in zip(types, counts):
        model_pool = VEHICLE_MODELS_BY_TYPE.get(
            t, VEHICLE_MODELS_BY_TYPE["Car"])
        for _ in range(cnt):
            plate = fake.license_plate()
            while plate in plates_seen:
                plate = fake.license_plate()
            plates_seen.add(plate)
            owner_id = str(uuid.uuid4())
            vehicles.append({
                "owner_id": owner_id,
                "plate_number": plate,
                "vehicle_type": t,
                "model": random.choice(model_pool),
                "color": random.choice(VEHICLE_COLORS)
            })
    random.shuffle(vehicles)
    return vehicles


def generate_speed(location, vehicle_type):
    sl = location.get("speed_limit", 60)
    r = random.random()
    if vehicle_type in ("Truck", "Bus"):
        base_factor = random.uniform(0.75, 0.95)
    elif vehicle_type == "Motorcycle":
        base_factor = random.uniform(0.95, 1.15)
    else:
        base_factor = 1.0
    effective_limit = max(20, int(sl * base_factor))
    if r < 0.50:
        var = random.uniform(-0.10, 0.10)
        speed = max(1, int(effective_limit * (1 + var)))
    elif r < 0.80:
        var = random.uniform(0.20, 0.50)
        speed = int(effective_limit * (1 + var))
    else:
        var = random.uniform(0.20, 0.50)
        speed = max(1, int(effective_limit * (1 - var)))
    return int(speed)


def generate_event(event_id, sensor, location, vehicle):
    street_dir = location.get("direction", "CIRCULAR")
    if street_dir == "NS":
        possible = ["N", "S"]
    elif street_dir == "EW":
        possible = ["E", "W"]
    else:
        possible = ["N", "S", "E", "W"]
    direction = random.choice(possible)
    speed = generate_speed(location, vehicle["vehicle_type"])
    return {
        "event_id": event_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "sensor_id": sensor["sensor_id"],
        "sensor_numeric_id": sensor["numeric_id"],
        "sensor_lat": sensor["lat"],
        "sensor_lon": sensor["lon"],
        "plate_number": vehicle["plate_number"],
        "owner_id": vehicle["owner_id"],
        "vehicle_type": vehicle["vehicle_type"],
        "model": vehicle["model"],
        "color": vehicle["color"],
        "speed": speed,
        "direction": direction,
        "location_id": location["location_id"],
        "location_name": location["street_name"],
        "location_lat": location["lat"],
        "location_lon": location["lon"],
        "road_type": location.get("road_type"),
        "street_direction": location.get("direction")
    }

# --------------- MAIN RUN ---------------


def run():
    ensure_data_dir()
    input("Press Enter to build base tables ...")

    locations = build_locations(NUM_LOCATIONS)
    sensors = build_sensors(locations, SENSOR_COVERAGE_RATIO)
    vehicles = build_vehicles_with_owner(NUM_VEHICLES)

    with open(LOCATIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(locations, f, ensure_ascii=False, indent=2)
    with open(SENSORS_FILE, "w", encoding="utf-8") as f:
        json.dump(sensors, f, ensure_ascii=False, indent=2)
    with open(VEHICLES_FILE, "w", encoding="utf-8") as f:
        json.dump(vehicles, f, ensure_ascii=False, indent=2)

    input("\nReview base tables. Press Enter to START streaming events (Ctrl+C to stop)...")

    plate_to_owner = {v["plate_number"]: v["owner_id"] for v in vehicles}
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STR,
        eventhub_name=EVENTHUB_NAME
    )

    # ✅ Load last event ID safely (scans forward and returns last valid id)
    event_count = get_last_event_id(EVENTS_FILE)
    print(f"Resuming from last event ID: {event_count}")

    start_time = time.time()
    # ✅ Open in append mode so we don't truncate previous runs
    with open(EVENTS_FILE, "a", encoding="utf-8") as fe:
        try:
            while event_count < TOTAL_EVENTS:
                active_sensors = [
                    s for s in sensors if s.get("status") == "Active"]
                if not active_sensors:
                    print("⚠ No active sensors available. Stopping simulation.")
                    break
                sensor = random.choice(active_sensors)
                location = next(
                    (l for l in locations if l["location_id"]
                     == sensor["location_id"]),
                    random.choice(locations)
                )
                vehicle = random.choice(vehicles)

                owner_id = plate_to_owner.get(
                    vehicle["plate_number"], vehicle.get("owner_id", str(uuid.uuid4())))
                event_count += 1
                ev = generate_event(event_count, sensor, location, vehicle)
                ev["owner_id"] = owner_id

                # Save locally
                fe.write(json.dumps(ev, ensure_ascii=False) + "\n")
                if event_count % 100 == 0:
                    fe.flush()

                # Send to Azure Event Hub
                event_json = json.dumps(ev, ensure_ascii=False)
                event_data_batch = producer.create_batch()
                event_data_batch.add(EventData(event_json))
                producer.send_batch(event_data_batch)

                print(
                    f"Event {event_count} sent: {ev['plate_number']} - {ev['speed']} km/h")
                time.sleep(EVENT_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\nSimulation interrupted by user (Ctrl+C).")
        finally:
            producer.close()

    elapsed = time.time() - start_time
    print(
        f"\nSimulation finished. Total events: {event_count} in {elapsed:.1f}s")
    print("Saved files:")
    print(" ", LOCATIONS_FILE)
    print(" ", SENSORS_FILE)
    print(" ", VEHICLES_FILE)
    print(" ", EVENTS_FILE)


if __name__ == "__main__":
    run()
