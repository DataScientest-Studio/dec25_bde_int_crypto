# Cryptocurrency Market Analysis & Prediction Pipeline

## Overview

This project is an end-to-end, containerized data and machine-learning pipeline for cryptocurrency market analysis and
prediction, built around Binance price data.

## Data Ingestion

The system ingests both historical and real-time crypto prices (e.g., BTC/USDT) from the Binance API:

- **Historical Data**: Pulled daily using Apache Airflow
- **Real-time Data**: Streamed via WebSockets using Python or NiFi

All incoming data is cleaned and reorganized in a preprocessing step before being stored.

## Data Storage

Processed data is persisted in a dual storage layer:

- **SQL Databases** (PostgreSQL/MySQL): Structured historical and training data
- **Elasticsearch or MongoDB**: High-volume, real-time stream collections

## Machine Learning

A machine-learning engine, developed in Jupyter Notebooks, fetches training data from the database to train predictive
models. Once trained, the model is deployed and exposed through a FastAPI service, which handles inference and business
logic.

## User Interaction

End users (traders) interact with the system in two ways:

1. **Predictions**: Requesting predictions via the FastAPI inference API
2. **Monitoring**: Viewing live market evolution and signals through a Kibana dashboard

## Infrastructure

All components run within a Dockerized infrastructure, ensuring scalability, isolation, and smooth orchestration across
ingestion, storage, ML training, inference, and visualization.

## Architecture
![Alt text](assets/Binance%20Data%20ML%20Pipeline-2026-01-30-100112.png)
