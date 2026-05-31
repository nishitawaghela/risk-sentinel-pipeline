# Technical Case Study: Sentinel - Real-Time Market Anomaly & Fraud Detection Engine

**Author:** Nishita Waghela

**Project Type:** End-to-End Data Engineering & Machine Learning

**Deployment:** https://sentinel-dashboard-u5bdxsxgfw5uzp7z6kthpy.streamlit.app/

---

## 1. Executive Summary

In institutional finance, the latency between a fraudulent market manipulation event (like a coordinated volume dump) and its detection is directly correlated to financial loss. Legacy risk architectures rely heavily on *T+1 (next day)* batch processing to identify these anomalies, meaning the damage to the market is already done by the time the compliance team receives the alert.

**Sentinel** solves this "time-to-discovery" problem by implementing a real-time, event-driven AI pipeline. By decoupling high-throughput ingestion from continuous micro-batch stream processing, the system evaluates live market trades in milliseconds. The solution integrates a fault-tolerant Kafka messaging layer, a PySpark processing engine, and a deployed Random Forest classifier to successfully flag structural market threats in near real-time.

---

## 2. Problem Statement

Current legacy trading compliance infrastructures face three distinct engineering challenges:

1. **The T+1 Latency Penalty:** Traditional ETL jobs run overnight on relational data warehouses. They cannot catch high-frequency, algorithmic market manipulation as it happens. 
2. **Ingestion Bottlenecks During Market Spikes:** Synchronous API gateways often lock up or drop data when subjected to extreme market volatility and high-volume transaction bursts.
3. **The "Small File" Analytics Problem:** Writing raw, continuous data streams directly into a data lake creates millions of tiny files, destroying I/O performance and driving up cloud compute costs for forensic analysts running historical queries.

---

## 3. Proposed Solution

Sentinel leverages **Event-Driven Architecture** and **Stream Processing** to evaluate risk prior to final trade settlement.

* **Fault-Tolerant Ingestion:** A non-blocking API gateway that guarantees zero data loss by pairing a synchronous local backup with an asynchronous event producer.
* **Micro-Batch ML Inference:** A stream processing engine that pulls live market events and executes on-the-fly Machine Learning inference to classify risk.
* **Cost-Optimized Data Lake:** An intelligent storage layer that heavily compresses and physically partitions processed data to accelerate downstream OLAP queries.

---

## 4. Technology Stack

I selected a highly robust, scalable stack recognized as the industry standard for modern data engineering and risk pipelines.

* **Language:** Python 3.12
* **API Gateway:** FastAPI, Uvicorn
* **Message Broker:** Apache Kafka (KRaft Mode), Zookeeper-less
* **Stream Processing:** Apache Spark (PySpark Structured Streaming)
* **Machine Learning:** Scikit-Learn (Random Forest Classifier), Pandas
* **Storage / Persistence:** SQLite (Ingestion Backup), Apache Parquet (Data Lake)
* **Orchestration & Deployment:** Docker, Docker Compose, Streamlit Cloud

---

## 5. Technical Methodology

My approach followed a strict Data Engineering lifecycle, moving from raw ingestion to model inference and optimized storage.

### A. Data Ingestion (The Gatekeeper)
To prevent API blocking under heavy load, I engineered a highly decoupled FastAPI ingestion layer.
* When a trade arrives, the API performs a fast, synchronous write to a local SQLite database, guaranteeing immediate persistence (zero data loss).
* Simultaneously, it fires an **asynchronous** message to the Kafka broker. By explicitly passing the `trade_id` as the message key, Kafka mathematically guarantees that any updates to the same trade are routed to the same exact partition, ensuring strict chronological ordering.

### B. Stream Processing & Feature Engineering
Data is continuously consumed from Kafka using PySpark Structured Streaming.
* The stream reads from the `latest` offset to ensure the risk engine is always evaluating the bleeding edge of the market, even if the cluster restarts.
* Within the `foreachBatch` micro-batch loop, the raw JSON payload is parsed and categorical variables (like Order Type and Action) are one-hot encoded on the fly to perfectly match the feature vectors required by the Machine Learning binary.

### C. Machine Learning & Classification
I selected a **Random Forest Classifier** over deep learning models due to strict financial compliance laws regarding model explainability.
* **The Inference:** The `.pkl` binary evaluates the engineered features (`price`, `volume`, `action_SELL`, `order_type_MARKET`).
* **The Result:** Standard retail trades are classified as `Class 0` (Clean). Massive, coordinated volume dumps that skew normal pricing distributions are immediately flagged as `Class 3` (High-Risk Threat), triggering an automated alert for manual review.

---

## 6. Engineering Challenges & Solutions

Building a fault-tolerant streaming architecture presented several hurdles.

### Challenge 1: API Latency & Dropped Payloads
* **The Issue:** Initially, forcing the API to wait for a successful Kafka broker acknowledgment during load testing caused response times to spike and connections to timeout.
* **The Fix:** I decoupled the architecture. I relied on local SQLite for guaranteed state, and switched the Kafka producer to fire asynchronously (fire-and-forget). This returned API response times to nominal levels while maintaining data integrity.

### Challenge 2: Zookeeper Overhead in Docker
* **The Issue:** Running both a Kafka Broker and Zookeeper locally inside Docker consumed excessive RAM, causing out-of-memory (OOM) crashes on my machine.
* **The Fix:** I modernized the stack by implementing Kafka in **KRaft (Kafka Raft) mode**. This completely eliminated the Zookeeper dependency, streamlining the infrastructure and significantly reducing the memory footprint of the Docker container.

### Challenge 3: Downstream Query Inefficiency
* **The Issue:** Writing raw JSON streams directly to disk made it nearly impossible to quickly query historical fraud data.
* **The Fix:** I implemented **Partition Pruning** via Apache Parquet. The Spark output is physically partitioned into directories based on `execution_date` and `risk_status` (e.g., `/date=2026-05-31/risk=3/`). When investigators query for anomalies, the OLAP engine skips 99% of the data, scanning megabytes instead of terabytes.

---

## 7. Project Limitations

To ensure transparency regarding system capabilities and areas for future iteration:

1. **Infrastructure Scalability:** The current streaming backend is orchestrated locally via `docker-compose`. A true enterprise deployment would require migrating the Kafka and Spark clusters to a managed cloud environment (e.g., Confluent Cloud, Databricks) or Kubernetes.
2. **Model Drift:** The Random Forest model is static. In a production environment, adversarial actors constantly change tactics. The system requires an MLOps pipeline (like Apache Airflow) to automatically retrain the model on fresh Parquet data weekly.
3. **Stateless Demo Frontend:** Because Streamlit Community Cloud cannot host live Kafka brokers, the interactive deployment dashboard relies on a stateless architecture. It successfully demonstrates the exact ML inference logic on custom payloads, but does not render a live WebSocket feed directly from the Kafka topic.
