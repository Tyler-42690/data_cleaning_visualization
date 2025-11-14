import streamlit as st
import polars as pl
import altair as alt
from streamlit_tags import st_tags

# ------------------------------
# Load aggregated data
# ------------------------------
@st.cache_data
def load_agg1():
    return pl.read_parquet("agg1.parquet")  # avg_close per day/ticker

@st.cache_data
def load_agg2():
    return pl.read_parquet("agg2.parquet")  # avg volume per sector

@st.cache_data
def load_agg3():
    return pl.read_parquet("agg3.parquet")  # daily returns

agg1 = load_agg1()
agg2 = load_agg2()
agg3 = load_agg3()

# ------------------------------
# Ensure trade_date is a Date type
# ------------------------------

# For agg1
if agg1["trade_date"].dtype == pl.Utf8:
    agg1 = agg1.with_columns(
        pl.col("trade_date").str.strptime(pl.Date, "%Y-%m-%d")
    )

# For agg3
if agg3["trade_date"].dtype == pl.Utf8:
    agg3 = agg3.with_columns(
        pl.col("trade_date").str.strptime(pl.Date, "%Y-%m-%d")
    )

# ------------------------------
# Sidebar filters
# ------------------------------
st.sidebar.header("Filters")

# Date range filter (for agg1 + agg3)
min_date = agg1["trade_date"].min()
max_date = agg1["trade_date"].max()

date_range = st.sidebar.date_input(
    "Trade Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

start_date, end_date = date_range

# Ticker selection (dynamic, searchable tags)
tickers = agg1["ticker"].unique().to_list()
selected_tickers = st_tags(
    label="Select tickers",
    text="Type & press enter",
    value=tickers[:5],
    suggestions=tickers,
    key="ticker_selector"  # <--- fixed key
)

# ------------------------------
# Main dashboard
# ------------------------------
st.title("📊 Aggregated Market Dashboard")
tabs = st.tabs(["📈 Average Close (agg1)", "📦 Volume by Sector (agg2)", "📉 Daily Returns (agg3)"])

# ------------------------------
# TAB 1 — agg1: Daily Average Close
# ------------------------------
with tabs[0]:
    st.header("Daily Average Closing Price")

    filtered = agg1.filter(
        (pl.col("trade_date") >= start_date) &
        (pl.col("trade_date") <= end_date) &
        (pl.col("ticker").is_in(selected_tickers))
    )

    st.write(f"Filtered rows: **{filtered.height}**")

    col1, col2 = st.columns(2)
    col1.metric("Average Close (overall)", f"{filtered['avg_close'].mean():.2f}")
    col2.metric("Distinct Tickers", f"{filtered['ticker'].n_unique()}")

    chart = (
        alt.Chart(filtered.to_pandas())
        .mark_line()
        .encode(
            x="trade_date:T",
            y="avg_close:Q",
            color="ticker:N",
            tooltip=["trade_date", "ticker", "avg_close"]
        )
        .interactive()
    )
    st.altair_chart(chart, width="stretch")
    st.dataframe(filtered.to_pandas(), width="stretch")

# ------------------------------
# TAB 2 — agg2: Volume by Sector
# ------------------------------
with tabs[1]:
    st.header("Average Volume by Sector")

    if "sector" in agg2.columns:
        col1, col2 = st.columns(2)
        col1.metric("Total Avg Volume", f"{agg2['avg_volume'].sum():,.0f}")
        col2.metric("Number of Sectors", f"{agg2['sector'].n_unique()}")

        chart = (
            alt.Chart(agg2.to_pandas())
            .mark_bar()
            .encode(
                x="sector:N",
                y="avg_volume:Q",
                color="sector:N",
                tooltip=["sector", "avg_volume"]
            )
        )
        st.altair_chart(chart, width="stretch")
        st.dataframe(agg2.to_pandas(), width="stretch")
    else:
        st.error("agg2 is missing required column 'sector'.")

# ------------------------------
# TAB 3 — agg3: Daily Returns
# ------------------------------
with tabs[2]:
    st.header("Daily Returns")

    filtered_returns = agg3.filter(
        (pl.col("trade_date") >= start_date) &
        (pl.col("trade_date") <= end_date) &
        (pl.col("ticker").is_in(selected_tickers))
    )

    st.write(f"Filtered rows: **{filtered_returns.height}**")

    col1, col2 = st.columns(2)
    col1.metric("Average Return", f"{filtered_returns['return'].mean():.4f}")
    col2.metric("Distinct Tickers", f"{filtered_returns['ticker'].n_unique()}")

    chart = (
        alt.Chart(filtered_returns.to_pandas())
        .mark_line()
        .encode(
            x="trade_date:T",
            y="return:Q",
            color="ticker:N",
            tooltip=["trade_date", "ticker", "return"]
        )
        .interactive()
    )
    st.altair_chart(chart, width="stretch")
    st.dataframe(filtered_returns.to_pandas(), width="stretch")
