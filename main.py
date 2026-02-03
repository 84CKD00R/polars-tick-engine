import re
from datetime import datetime, timezone
from typing import List, Dict

import plotly.graph_objects as go
import polars as pl
import streamlit as st

class LoopThroughFilesInputData:
    def __init__(self, reader, rules, rule_secs, symbol_filter, buffers, bars_out, sanity_min_price, sanity_max_price):
        self.reader = reader
        self.rules = rules
        self.rule_secs = rule_secs
        self.symbol_filter = symbol_filter
        self.buffers = buffers
        self.bars_out = bars_out
        self.sanity_min_price = sanity_min_price
        self.sanity_max_price = sanity_max_price

class BuildBarsFromTicksOutputData:
    def __init__(self, data, file_name, timeframemap, sanity_min_price, sanity_max_price):
        self.data = data
        self.file_name = file_name
        self.timeframemap = timeframemap
        self.sanity_min_price = sanity_min_price
        self.sanity_max_price = sanity_max_price

def loop_through_files(data) -> None:
    while True:
        batches = data.reader.next_batches(1)
        if not batches:
            break

        raw = batches[0]
        ticks = clean_tick_batch(raw, data.symbol_filter, data.sanity_min_price, data.sanity_max_price)
        if ticks.height == 0:
            continue

        for r in data.rules:
            buf = data.buffers[r]
            combined = pl.concat([buf, ticks], how="vertical").sort("datetime")

            last_dt = combined.select(pl.col("datetime").max()).item()
            if last_dt is None:
                data.buffers[r] = combined
                continue

            cutoff = floor_dt_to_seconds(last_dt, data.rule_secs[r])

            to_finalize = combined.filter(pl.col("datetime") < cutoff)
            to_keep = combined.filter(pl.col("datetime") >= cutoff)

            if to_finalize.height:
                b = bars_from_ticks(to_finalize, r)
                if b.height:
                    data.bars_out[r].append(b)

            data.buffers[r] = to_keep

    for r in data.rules:
        buf = data.buffers[r]
        if buf.height:
            b = bars_from_ticks(buf, r)
            if b.height:
                data.bars_out[r].append(b)

def clean_tick_batch(df_pl: pl.DataFrame, symbol_filter, sanity_min_price, sanity_max_price) -> pl.DataFrame:
    out = (
        df_pl.with_columns(
            pl.col("ts_event")
            .str.strptime(pl.Datetime,
                          format="%Y-%m-%dT%H:%M:%S%Z",
                          strict=False)
            .dt.convert_time_zone("UTC")
            .alias("datetime"),
            (pl.col("price").cast(pl.Float64, strict=False) * 1e-9).alias("mid"),
        )
        .select(
            [pl.col("datetime"), pl.col("mid")]
            + ([pl.col("symbol")] if "symbol" in df_pl.columns else [])
        )
    )
    out = sanity_check_tick_data(out, symbol_filter, sanity_min_price, sanity_max_price)
    return out

def filter_symbol_in_files(symbol, years, month) -> str | None:

    # Futures rollover logic - typically roll ~2 weeks before expiration
    # H (Mar): Dec 15 - Mar 14
    # M (Jun): Mar 15 - Jun 14
    # U (Sep): Jun 15 - Sep 14
    # Z (Dec): Sep 15 - Dec 14

    if month in {12}:
        code = "H"
        years = years + 1
    elif month in {1, 2, 3}:
        code = "H"
    elif month in {4, 5, 6}:
        code = "M"
    elif month in {7, 8, 9}:
        code = "U"
    elif month in {10, 11}:
        code = "Z"
    else:
        return None

    return f"{symbol}{code}{str(years)[-1]}"

def bars_from_ticks(ticks: pl.DataFrame, rule: str) -> pl.DataFrame:
    return (
        ticks.group_by_dynamic(
            index_column="datetime",
            every=rule,
            period=rule,
            closed="left",
            label="left",
        )
        .agg(
            pl.col("mid").first().alias("open"),
            pl.col("mid").max().alias("high"),
            pl.col("mid").min().alias("low"),
            pl.col("mid").last().alias("close"),
            pl.len().alias("volume"),
        )
        .drop_nulls(["open", "high", "low", "close"])
        .sort("datetime")
    )

def sanity_check_tick_data(out, symbol_filter, sanity_min_price, sanity_max_price) -> pl.DataFrame:
    out = out.drop_nulls(["datetime", "mid"])

    if "symbol" in out.columns:
        out = out.filter(pl.col("symbol") == symbol_filter)
        out = out.drop("symbol")

    if sanity_min_price is not None:
        out = out.filter(pl.col("mid") >= sanity_min_price)
    if sanity_max_price is not None:
        out = out.filter(pl.col("mid") <= sanity_max_price)

    return out.sort("datetime")

def rule_to_seconds(rule: str) -> int:
    m = re.fullmatch(r"(\d+)\s*s", rule.strip())
    if not m:
        raise ValueError(f"Only 'Ns' rules supported here, got: {rule}")
    return int(m.group(1))

def floor_dt_to_seconds(dt: datetime, sec: int) -> datetime:
    ts = dt.timestamp()
    flo = (int(ts) // sec) * sec
    return datetime.fromtimestamp(flo, tz=timezone.utc)

def merge_data(bars_out, rules) -> Dict[str, pl.DataFrame]:
    final_bars: Dict[str, pl.DataFrame] = {}
    for r in rules:
        if bars_out[r]:
            all_bars = pl.concat(bars_out[r], how="vertical").sort("datetime")

            all_bars = all_bars.unique(subset=["datetime"], keep="last").sort("datetime")
            final_bars[r] = all_bars
        else:
            final_bars[r] = pl.DataFrame(
                schema={
                    "datetime": pl.Datetime(time_zone="UTC"),
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Int64,
                }
            )
    return final_bars

def build_bars_from_ticks(data) -> Dict[str, pl.DataFrame]:
    rules: List[str] = data.timeframemap.values()

    rule_secs = {r: rule_to_seconds(r) for r in rules}

    bars_out: Dict[str, List[pl.DataFrame]] = {r: [] for r in rules}

    buffers: Dict[str, pl.DataFrame] = {
        r: pl.DataFrame({"datetime": pl.Series([], dtype=pl.Datetime(time_zone="UTC")), "mid": pl.Series([], dtype=pl.Float64)})
        for r in rules
    }

    reader = pl.read_csv_batched(
        data.data,
        batch_size=200_000,
        has_header=True,
    )

    m = re.search(r'^([A-Z]+)[-_](20\d{2})[-_]?(\d{2})\.csv$', data.file_name)

    symbol = m.group(1)
    year = int(m.group(2))
    month = int(m.group(3))

    symbol_filter = filter_symbol_in_files(symbol, year, month)

    loop = LoopThroughFilesInputData(reader, rules, rule_secs, symbol_filter, buffers, bars_out, data.sanity_min_price, data.sanity_max_price)

    loop_through_files(loop)

    return merge_data(bars_out, rules)

def main() -> None:
    file = st.file_uploader(
        "Upload a file that contains 1 month of ticks data",
        type=["csv"]
    )
    sanity_min_price = st.sidebar.selectbox("Sanity check Min Price",
                                            [0, 5_000, 10_000.0, 20_000.0, 30_000.0, 40_000.0, 50_000.0, 60_000.0,
                                             70_000.0, 80_000.0, 90_000.0, 100_000.0])
    sanity_max_price = st.sidebar.selectbox("Sanity check Max Price",
                                            [0, 5_000, 10_000.0, 20_000.0, 30_000.0, 40_000.0, 50_000.0, 60_000.0,
                                             70_000.0, 80_000.0, 90_000.0, 100_000.0])
    if file is not None:
        try:
            import tempfile

            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
                tmp.write(file.read())
                tmp_path = tmp.name

            st.write(f"Processing file: {file.name}")

            filename = file.name

            timeframe_map = {"1m": "60s", "5m": "300s", "15m": "900s", "30m": "1800s", "1h": "3600s",
                             "2h": "7200s", "4h": "14400s", "12h": "43200s", "1d": "86400s"}

            input_data = BuildBarsFromTicksOutputData(tmp_path, filename, timeframe_map, sanity_min_price, sanity_max_price)

            bars = build_bars_from_ticks(input_data)

            product = st.selectbox("Select a timeframe", list(timeframe_map.keys()))
            selected_rule = timeframe_map[product]
            df = bars[selected_rule]

            if df.height > 0:
                candlestick = go.Candlestick(
                    x=df['datetime'],
                    open=df['open'],
                    high=df['high'],
                    low=df['low'],
                    close=df['close']
                )

                layout = go.Layout(
                    title=f'Candlestick Chart',
                    xaxis=dict(title='Date'),
                    yaxis=dict(title='Price')
                )

                fig = go.Figure(data=[candlestick], layout=layout)
                st.plotly_chart(fig)
            else:
                st.warning("No data after processing")
                st.warning("Please check if you forgot to select the sanity price range (upper left corner)")

        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            import traceback

            st.code(traceback.format_exc())
        finally:
            import os
            os.unlink(tmp_path)
    else:
        st.info("Please upload a CSV file to begin")
        st.markdown(
            "Before you upload a file select what price was the lowest and the highest in the sanity check box!")
        st.markdown("This will be used to filter out ticks that are too low or too high for data cleaning purposes.")
        st.markdown("The file should contain ticks for futures for 1 month AND the file should be named like this:")
        st.markdown("NQ_YYYY_MM.csv")
        st.markdown("Where YYYY is the year and MM is the month (e.g. NQ_2020_03.csv) this would be 2020 March")
        st.markdown("There are other formats that can be handled:")
        st.markdown("NQ_2020_03.csv, NQ-2020-03.csv, NQ_202003.csv")
        st.warning("Only 1 month of tick data is supported per file")

if __name__ == "__main__":
    main()