# 🗄️ XAU Dynamics - Data Pipeline (Microservice)

![Python Version](https://img.shields.io/badge/Python-3.10%2B-blue?style=flat-square&logo=python)
![Azure Ready](https://img.shields.io/badge/Deployed_on-Azure_Container_Apps-0078D4?style=flat-square&logo=microsoftazure)
![Cosmos DB](https://img.shields.io/badge/Database-Azure_Cosmos_DB-51A6E8?style=flat-square&logo=microsoftazure)
![Status](https://img.shields.io/badge/Status-Production_Ready-success?style=flat-square)

**High-throughput, asynchronous data ingestion and preprocessing engine for the XAU Dynamics ecosystem.**

---

## 📌 Executive Summary
The **Data Pipeline** is a mission-critical microservice within the XAU Dynamics infrastructure. Designed for the highly volatile XAU/USD market, it handles the continuous ingestion, normalization, and low-latency distribution of high-frequency market ticks and real-time macroeconomic event feeds.

This component ensures that our predictive NLP agents (`MacroAI`) and MQL5 execution nodes (`CoreAPI`) receive structured, sanitized, and enriched data streams without computational bottlenecks.

## ⚙️ Core Architecture & Data Flow

The pipeline operates on an Event-Driven, non-blocking architecture using `asyncio`:

1. **Ingestion Layer:** Connects to institutional WebSocket feeds to capture raw XAU/USD tick data (Bid/Ask/Volume).
2. **Processing Layer:** Cleans JSON payloads, calculates real-time volatility spreads, and normalizes timestamps to UTC ISO formats.
3. **Storage Layer:** Streams processed batches directly into **Azure Cosmos DB** for high-speed retrieval and compliance logging.
4. **Distribution Layer:** Routes anomalous market events to the AI sentiment engine for immediate macro-analysis.

## 📂 Repository Structure

```text
XAU-Dynamics-DataPipeline/
├── config.py             # Enterprise environment variables & Azure routing
├── pipeline.py           # Core asynchronous processing logic
├── requirements.txt      # Microservice dependencies
└── Dockerfile            # Containerization for Azure deployment

🔐 Environment Variables
To run this microservice securely, the following variables must be configured in your ⁠.env⁠ file or Azure Key Vault:
Variable	Description	Default / Example
AZURE_COSMOS_URI	Endpoint for Azure Cosmos DB	https://xau-dynamics-db.documents...
AZURE_COSMOS_KEY	Primary API access key	************************
GOLD_FEED_URL	Primary WebSocket market feed	wss://stream-api.xau-dynamics.io/v3/gold
