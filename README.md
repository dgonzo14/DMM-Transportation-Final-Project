# Comparative Transit Analysis: NYC (MTA) vs. London (TfL)

This project implements a scalable data engineering pipeline to monitor and compare the performance of two major global transit systems. By integrating real-time transit feeds with environmental weather data, we analyze how external stressors impact urban mobility.

## 🏗️ System Architecture: Medallion Lakehouse
We utilize a **Medallion Architecture** to ensure data reliability and scalability:
* **Bronze (Raw):** Immutable snapshots of MTA (Protobuf), TfL (JSON), and Open-Meteo weather data.
* **Silver (Clean):** Structured tables where data types are normalized, timestamps are aligned (including "data lagging" for weather), and duplicates are removed.
* **Gold (Analytical):** Joined views optimized for the Streamlit dashboard, focusing on arrival accuracy and weather correlations.

## 🌦️ Weather Pipeline & Automation
A key component of this project is the automated weather ingestion system.
* **Source:** Open-Meteo API (Hourly snapshots).
* **Authentication Pivot:** To bypass Snowflake MFA barriers, we transitioned from manual browser-based authentication to **RSA-2048 Key-Pair Authentication**. This allows the `weather_producer.py` to run autonomously on a 24/7 cycle.
* **Fault Tolerance:** The producer utilizes a `LocalFileSystemSink` to back up data locally before pushing to Snowflake, ensuring no data loss during network interruptions.

## 🛠️ Tech Stack
* **Languages:** Python (Producers/Analytics), SQL (Snowflake Transformation)
* **Data Warehouse:** Snowflake
* **Cloud Storage:** Cloudflare R2 (Lightweight Kafka alternative)
* **Visualization:** Streamlit
* **Protocols:** GTFS-Realtime, REST APIs, RSA/SHA-256 Security

## 📁 Repository Structure
* `train_project/`: Contains the automated `weather_producer.py`.
* `sql_queries/`: Documentation of Silver-tier de-duplication and security setup.
* `landing_zone/`: Local staging area for raw JSON snapshots.
* `fetch_april_history.py`: Script used for initial 20-day historical backfill.
