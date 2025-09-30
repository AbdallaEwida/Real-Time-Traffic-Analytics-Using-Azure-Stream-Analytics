#!/usr/bin/env python3
# traffic_simulator_final_v2.py

import os
import random
import json
import time
import uuid
from datetime import datetime
from faker import Faker

fake = Faker()

# --------------- CONFIG ---------------
NUM_LOCATIONS = 1000
SENSOR_COVERAGE_RATIO = 0.55
NUM_VEHICLES = 2000
TOTAL_EVENTS = 100_000
EVENT_INTERVAL_SECONDS = 1.5
DATA_DIR = "data"
LOCATIONS_FILE = os.path.join(DATA_DIR, "locations.json")
SENSORS_FILE = os.path.join(DATA_DIR, "sensors.json")
VEHICLES_FILE = os.path.join(DATA_DIR, "vehicles.json")
EVENTS_FILE = os.path.join(DATA_DIR, "events.json")

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
    "Car": [
        "Toyota Corolla", "Hyundai Elantra", "Kia Cerato", "Nissan Sunny", "Renault Logan",
        "Mitsubishi Lancer", "Chevrolet Optra", "Peugeot 301", "Honda Civic", "Skoda Octavia",
        "BMW 320i", "Mercedes C180", "MG 5", "Chery Arrizo 5", "BYD F3", "Suzuki Swift",
        "Fiat Tipo", "Seat Ibiza", "Citroen C4", "Volkswagen Passat", "Opel Astra", "Ford Focus",
        "Audi A4", "Mazda 3"
    ],
    "Truck": [
        "Volvo FH16", "Mercedes Actros", "MAN TGS", "Scania R-Series", "Isuzu FVZ",
        "Sinotruk Howo", "DFAC Cargo", "Iveco Stralis"
    ],
    "Van": [
        "Ford Transit", "Mercedes Sprinter", "Renault Master", "Nissan Urvan", "Peugeot Boxer", "Fiat Ducato"
    ],
    "Motorcycle": [
        "Yamaha YBR", "Honda CG125", "Honda CBR500R", "Kawasaki Ninja 400", "Bajaj Pulsar", "TVS Apache", "Suzuki GSX"
    ],
    "Bus": [
        "Mercedes Citaro", "Volvo 9700", "MAN Lion's City", "Scania Interlink", "Isuzu City Bus", "King Long"
    ]
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

STREET_NAMES = list(STREET_TO_CITY.keys())

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

# ----------------- BUILD LOCATIONS -----------------


def build_locations(target_count):
    locations = []
    anchors = []
    for name in STREET_NAMES:
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
        # city fixed per street
        city = STREET_TO_CITY.get(base["name"], "Cairo")
        suffix = f" (segment {idx})" if random.random() < 0.35 else ""
        street_name = f"{base['name']}{suffix}"
        locations.append({
            "location_id": idx,
            "street_name": street_name,
            "city": city,
            "lat": lat,
            "lon": lon,
            "road_type": road_type,
            "speed_limit": speed_limit
        })
    return locations

# ------------- BUILD SENSORS -------------


def build_sensors(locations, coverage_ratio=0.55):
    sensors = []
    target = int(len(locations) * coverage_ratio)
    picked = random.sample(locations, target)
    numeric = 1
    for i, loc in enumerate(picked, start=1):
        clean = slugify(loc["street_name"])
        kind = random.choice(
            ["CCTV", "Radar", "TrafficCounter", "MultiSensor"])
        status = random.choices(["Active", "Inactive", "Under Maintenance"], weights=[
                                0.8, 0.1, 0.1], k=1)[0]
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

# ------------- BUILD VEHICLES (owner kept) -------------


def build_vehicles_with_owner(total):
    # allocate counts per type
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
            # ensure unique plate numbers
            plate = fake.license_plate()
            while plate in plates_seen:
                plate = fake.license_plate()
            plates_seen.add(plate)
            owner_id = str(uuid.uuid4())  # owner identifier (kept)
            vehicles.append({
                # no vehicle_id; owner_id kept as requested
                "owner_id": owner_id,
                "plate_number": plate,
                "vehicle_type": t,
                "model": random.choice(model_pool),
                "color": random.choice(VEHICLE_COLORS)
            })
    random.shuffle(vehicles)
    return vehicles

# --------------- EVENT LOGIC ---------------


def generate_speed(location, vehicle_type):
    sl = location.get("speed_limit", 60)
    r = random.random()
    if vehicle_type in ("Truck", "Bus"):
        sl_eff = max(30, int(sl * random.uniform(0.8, 1.0)))
    elif vehicle_type == "Motorcycle":
        sl_eff = int(sl * random.uniform(0.9, 1.15))
    else:
        sl_eff = sl
    if r < 0.75:
        speed = max(1, int(sl_eff * (1 + random.uniform(-0.12, 0.12))))
    elif r < 0.95:
        var = random.uniform(0.12, 0.30)
        speed = int(
            sl_eff * (1 + var)) if random.random() < 0.6 else max(1, int(sl_eff * (1 - var)))
    else:
        speed = random.randint(1, max(5, sl_eff + 60))
    return int(speed)


def generate_event(event_id, sensor, location, vehicle):
    direction = random.choice(["N", "S", "E", "W"])
    speed = generate_speed(location, vehicle["vehicle_type"])
    ev = {
        "event_id": event_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "sensor_id": sensor["sensor_id"],
        "sensor_numeric_id": sensor["numeric_id"],
        "sensor_lat": sensor["lat"],
        "sensor_lon": sensor["lon"],
        # vehicle reference via plate + owner_id (owner lookup realistic)
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
        "road_type": location.get("road_type")
    }
    return ev

# --------------- RUN ---------------


def run():
    ensure_data_dir()
    input("Press Enter to build base tables (you'll be able to review them) ...")

    locations = build_locations(NUM_LOCATIONS)
    sensors = build_sensors(locations, SENSOR_COVERAGE_RATIO)
    vehicles = build_vehicles_with_owner(NUM_VEHICLES)

    # save base tables
    with open(LOCATIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(locations, f, ensure_ascii=False, indent=2)
    with open(SENSORS_FILE, "w", encoding="utf-8") as f:
        json.dump(sensors, f, ensure_ascii=False, indent=2)
    with open(VEHICLES_FILE, "w", encoding="utf-8") as f:
        json.dump(vehicles, f, ensure_ascii=False, indent=2)

    # print base tables (top samples and counts)
    print("\n=== LOCATIONS (sample 50) ===")
    for loc in locations[:50]:
        print(f"{loc['location_id']:4d} | {loc['street_name'][:50]:50s} | {loc['road_type']:11s} | speed_limit:{loc['speed_limit']:3d} | ({loc['lat']},{loc['lon']})")
    print(f"(Total locations: {len(locations)})")

    print("\n=== SENSORS (sample 50) ===")
    for s in sensors[:50]:
        print(f"{s['sensor_id']:30s} | type:{s['sensor_type']:12s} | status:{s['status']:16s} | loc_id:{s['location_id']:4d}")
    print(f"(Total sensors: {len(sensors)})")

    print("\n=== VEHICLES (sample 50) ===")
    for v in vehicles[:50]:
        print(f"Owner:{v['owner_id'][:8]}... | Plate:{v['plate_number']:10s} | Type:{v['vehicle_type']:10s} | Model:{v['model'][:20]:20s} | Color:{v['color']}")
    print(f"(Total vehicles: {len(vehicles)})")

    input("\nReview base tables above. Press Enter to START streaming events (Ctrl+C to stop)...")

    # create plate -> owner map for "lookup"
    plate_to_owner = {v["plate_number"]: v["owner_id"] for v in vehicles}

    event_count = 0
    start_time = time.time()
    with open(EVENTS_FILE, "w", encoding="utf-8") as fe:
        try:
            while event_count < TOTAL_EVENTS:
                # choose only active sensors
                active_sensors = [
                    s for s in sensors if s.get("status") == "Active"]
                if not active_sensors:
                    print("⚠️ No active sensors available. Stopping simulation.")
                    break
                sensor = random.choice(active_sensors)
                location = next(
                    (l for l in locations if l["location_id"] == sensor["location_id"]), random.choice(locations))
                vehicle = random.choice(vehicles)

                # owner lookup by plate (realistic path)
                owner_id = plate_to_owner.get(vehicle["plate_number"])
                # (should always exist since we built the map from vehicles)
                if owner_id is None:
                    # fallback - shouldn't happen
                    owner_id = vehicle.get("owner_id", str(uuid.uuid4()))

                event_count += 1
                ev = generate_event(event_count, sensor, location, vehicle)
                # ensure owner_id is consistent with lookup
                ev["owner_id"] = owner_id

                fe.write(json.dumps(ev, ensure_ascii=False) + "\n")
                if event_count % 100 == 0:
                    fe.flush()

                # Print detailed info (accumulating)
                print("\n--- LOCATION INFO ---")
                print(f" Name        : {location['street_name']}")
                print(f" City        : {location['city']}")
                print(f" Road Type   : {location['road_type']}")
                print(f" Coordinates : ({location['lat']}, {location['lon']})")
                print(f" Speed Limit : {location['speed_limit']} km/h")

                print("\n--- SENSOR INFO ---")
                print(
                    f" Sensor ID   : {sensor['sensor_id']} (num {sensor['numeric_id']})")
                print(
                    f" Sensor Type : {sensor['sensor_type']}     Status: {sensor['status']}")
                print(f" Sensor Coords: ({sensor['lat']}, {sensor['lon']})")

                print("\n--- VEHICLE INFO ---")
                print(f" Plate       : {vehicle['plate_number']}")
                print(f" Owner ID    : {owner_id}")
                print(
                    f" Type/Model  : {vehicle['vehicle_type']} / {vehicle['model']}")
                print(f" Color       : {vehicle['color']}")

                print("\n--- EVENT INFO ---")
                print(
                    f" Event ID    : {ev['event_id']}   Timestamp: {ev['timestamp']}")
                print(
                    f" Speed       : {ev['speed']} km/h   Direction: {ev['direction']}")
                print("-------------------------------")
                # also print compact JSON line for convenience
                print(json.dumps(ev, ensure_ascii=False))

                if event_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    print(
                        f"\n>>> Progress: {event_count}/{TOTAL_EVENTS}  Elapsed: {elapsed:.1f}s\n")

                time.sleep(EVENT_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\nSimulation interrupted by user (Ctrl+C).")

    elapsed = time.time() - start_time
    print(
        f"\nSimulation finished. Total events generated: {event_count} in {elapsed:.1f}s")
    print("Saved files:")
    print(" ", LOCATIONS_FILE)
    print(" ", SENSORS_FILE)
    print(" ", VEHICLES_FILE)
    print(" ", EVENTS_FILE)


if __name__ == "__main__":
    run()
