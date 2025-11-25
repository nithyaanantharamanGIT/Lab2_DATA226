A production-ready data analytics pipeline for automated stock market technical analysis, demonstrating modern ELT patterns and data engineering best practices.

🎯 Overview
This project implements a complete end-to-end data analytics system that:

Extracts historical stock data from Yahoo Finance API
Loads raw data into Snowflake cloud data warehouse
Transforms data using dbt with staged modeling approach
Orchestrates workflows with Apache Airflow
Visualizes insights through interactive BI dashboards

The pipeline automatically calculates technical indicators (SMA, RSI) and generates trading signals for informed investment decisions.

✨ Features

Automated Data Pipeline: Daily scheduled extraction and processing
Technical Analysis:

Simple Moving Averages (7, 14, 30, 50-day periods)
Relative Strength Index (14-period)
Trend strength classification
Multi-layer trading signal generation


Data Quality: Comprehensive testing and validation
Historical Tracking: SCD Type 2 snapshots for trend analysis
Interactive Dashboards: Real-time visualization of market indicators
Scalable Architecture: Easy addition of new stocks and indicators
Idempotent Operations: Reliable transaction management

🛠️ Technology Stack


<img width="732" height="216" alt="image" src="https://github.com/user-attachments/assets/e6ed9561-5b80-4bac-8545-453ba9d6f2e7" />


📦 Prerequisites

Python 3.8 or higher
Apache Airflow 2.0+
Snowflake account
dbt Core 1.0+
Git
