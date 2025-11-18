import json

import psycopg2
from kafka import KafkaConsumer


def run_consumer():
    """
    Consumes trip messages from Kafka and inserts them into PostgreSQL.
    """
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "trips",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="trips-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        # Create trips table if it does not exist
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trips (
                trip_id VARCHAR(50) PRIMARY KEY,
                status VARCHAR(50),
                city VARCHAR(100),
                pickup_time TIMESTAMP,
                dropoff_time TIMESTAMP,
                distance_km NUMERIC(8, 2),
                fare NUMERIC(10, 2),
                surge NUMERIC(4, 2),
                payment_method VARCHAR(50)
            );
            """
        )
        print("[Consumer] ✓ Table 'trips' ready.")
        print("[Consumer] 🎧 Listening for messages...\n")

        message_count = 0
        for message in consumer:
            try:
                trip = message.value

                insert_query = """
                    INSERT INTO trips (
                        trip_id, status, city, pickup_time, dropoff_time,
                        distance_km, fare, surge, payment_method
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trip_id) DO NOTHING;
                """

                cur.execute(
                    insert_query,
                    (
                        trip["trip_id"],
                        trip["status"],
                        trip["city"],
                        trip["pickup_time"],
                        trip["dropoff_time"],
                        trip["distance_km"],
                        trip["fare"],
                        trip["surge"],
                        trip["payment_method"],
                    ),
                )

                message_count += 1
                print(
                    f"[Consumer] ✓ #{message_count} Inserted trip "
                    f"{trip['trip_id']} | {trip['city']} | "
                    f"${trip['fare']} | status={trip['status']}"
                )

            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_consumer()