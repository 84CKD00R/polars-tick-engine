# Polars Tick Engine

A high-performance Python tool designed to process millions of financial market data points (tick data) into various time intervals (OHLC bars) and visualize the results in real-time using Streamlit.

This project demonstrates efficient **Big Data** handling through batch processing and visualizations using the **Polars** library.

## IMPORTANT! -> Data Source
The sample data used in this project is provided by **[Databento](https://databento.com/)**. 
*Note: This repository contains only a small sample (Because of GitHub file size limit its only ~2 Day of data) for demonstration purposes. Users should obtain their own datasets directly from Databento for full-scale use.*

## Highlights

- **Extreme Performance:** Leverages the **Polars** library for lightning-fast data processing using memory-efficient batch reading.
- **Financial Logic:** Automatic futures symbol detection and **rollover logic** (H/M/U/Z) based on file naming conventions.
- **Data Quality:** Integrated sanity checks to filter out erroneous data points and outliers.
- **Interactive UI:** Visualization of generated candlestick charts using **Streamlit** and **Plotly**.

## Tech Stack

- **Language:** Python 3.11+
- **Data Processing:** [Polars](https://pola.rs/) (Optimized for large-scale datasets)
- **Visualization:** Plotly & Streamlit
- **Logic:** Regular Expressions for file parsing & Time-series analysis

## How it Works

1. **File Upload:** Ingests CSV files containing tick data (e.g., `NQ_2025_01.csv`).
2. **Preprocessing:** Normalizes timestamps to UTC and scales prices.
3. **Resampling:** Aggregates ticks into Open, High, Low, and Close (OHLC) values for timeframes ranging from 1 minute to 1 day.
4. **Visualization:** Dynamic timeframe selection with interactive candlestick rendering.

## Demo

### Upload Files
![App Demo](guide/UploadFiles.gif)
### Choose Timeframe
![App Demo](guide/ChooseTimeframe.gif)

## Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/84CKD00R/polars-tick-engine.git

2. **Install dependencies**
    ```bash
   pip install -r requirements.txt

3. **Run the app**
    ```bash
   streamlit run main.py