Polars based dataframe processor that handles repeating lines, missing data and more for a stocks dataset.

To start run the following command with uv:

```uv sync```

To rebuild the parquet files for the aggregates and clean format dataframe:

```uv run cleaning.py```

Then to run strealit dynamic chart visualization of charts (Stock Daily Average Close, Volume by Sector, and Daily Returns):

```streamlit run visualization.py```