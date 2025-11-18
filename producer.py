import time
import json
import uuid
import random
from datetime import datetime, timedelta

from kafka import KafkaProducer
from faker import Faker

fake = Faker()


def generate_synthetic_trip():
    """
    Generates synthetic ride-sharing trip data.
    """
    cities = [
        "New York",
        "San Francisco",
        "Chicago",
        "Los Angeles",
        "Seattle",
        "Austin",
        "Miami",
    ]
    statuses = ["Requested", "Completed", "Cancelled"]
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Cash"]
    surge_options = [1.0, 1.2, 1.5, 2.0]

    city = random.choice(cities)
    status = random.choices(
        statuses, weights=[0.1, 0.8, 0.1], k=1
    )[0]  # most trips complete
    payment_method = random.choice(payment_methods)
    surge = random.choice(surge_options)

    # trip distance (km)
    distance_km = round(random.uniform(1, 25), 2)

    base_fare = 3.0  # base fee
    per_km = 1.75
    fare = (base_fare + per_km * distance_km) * surge
    fare = round(fare + random.uniform(-2, 2), 2)  # add noise

    pickup_time = datetime.now()
    # duration ~= distance * (2–4 minutes per km)
    minutes = distance_km * random.uniform(2, 4)
    dropoff_time = pickup_time + timedelta(minutes=minutes)

    return {
        "trip_id": str(uuid.uuid4())[:8],
        "status": status,
        "city": city,
        "pickup_time": pickup_time.isoformat(),
        "dropoff_time": dropoff_time.isoformat(),
        "distance_km": float(distance_km),
        "fare": float(fare),
        "surge": float(surge),
        "payment_method": payment_method,
    }


def run_producer():
    """
    Kafka producer that sends synthetic trips to the 'trips' topic.
    """
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")

        count = 0
        while True:
            trip = generate_synthetic_trip()
            print(f"[Producer] Sending trip #{count}: {trip}")

            future = producer.send("trips", value=trip)
            record_metadata = future.get(timeout=10)
            print(
                f"[Producer] ✓ Sent to partition {record_metadata.partition} "
                f"at offset {record_metadata.offset}"
            )

            producer.flush()
            count += 1

            # send a new trip every 0.5–2 seconds
            time.sleep(random.uniform(0.5, 2.0))

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_producer()
