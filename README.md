# apache_kafka_data_stream

# Real-Time Ride-Sharing Trips Stream

This project implements a real-time data streaming pipeline for ride-sharing trips using:

- Apache Kafka – message broker for streaming trip events  
- PostgreSQL – storage for trips data  
- Streamlit – live dashboard for real-time monitoring  
- Docker Compose – runs Kafka and PostgreSQL

The system continuously generates synthetic ride-sharing trips (city, distance, fare, surge, status, etc.), streams them through Kafka, stores them in PostgreSQL, and visualizes key metrics in a live dashboard.

## Project Structure

```text
apache_kafka_data_streamer/
├── consumer.py        # Kafka consumer → inserts trips into PostgreSQL
├── producer.py        # Kafka producer → generates synthetic trips
├── dashboard.py       # Streamlit dashboard (real-time view)
├── docker-compose.yml # Kafka + PostgreSQL services
├── requirements.txt   # Python dependencies
└── ScreenshotsForGrading/  # Example output screenshots (optional)
