# cleaning.py
import re
import polars as pl

RAW_PATH = "stock_market.csv"
CLEAN_PATH = "cleaned.parquet"
AGG1_PATH = "agg1.parquet"
AGG2_PATH = "agg2.parquet"
AGG3_PATH = "agg3.parquet"

# ---------- 1. LOADING ----------
print("Loading CSV...")
df = pl.read_csv(RAW_PATH)

# ---------- 2. NORMALIZE COLUMN HEADERS ----------
df = df.rename({
    col: re.sub(r'\W+', '_', col.strip().lower()) for col in df.columns
})

# ---------- 3. CLEAN TEXT COLUMNS ----------
# These values should be replaced with the literal string "null"
NULL_EQUIV = ["", " ", "na", "n/a", "null", "-"]

clean_cols = []
for col in df.columns:
    if df[col].dtype == pl.Utf8:
        clean_cols.append(
            pl.when(
                pl.col(col)
                .str.replace_all(r'^\s+|\s+$', '')        # trim
                .str.to_lowercase()                       # lowercase
                .is_in(NULL_EQUIV)                        # check null-like
            )
            .then(pl.lit("null"))                         # literal, for safe use
            .otherwise(
                pl.col(col)
                .str.replace_all(r'^\s+|\s+$', '')        # trim
                .str.to_lowercase()
            )
            .alias(col)
        )

df = df.with_columns(clean_cols)


# ---------- 4. FIX DATE COLUMNS ----------
date_cols = [c for c in df.columns if "trade" in c]

date_formats = [
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d/%m/%Y"
]

for col in date_cols:
    if df[col].dtype != pl.Utf8:
        continue

    print(f"Parsing date column: {col}")
    PARSED = None

    # Try each format until one works
    for fmt in date_formats:
        try:
            PARSED = df.with_columns(
                pl.col(col).str.strptime(pl.Date, format="%m/%d/%Y", strict=True).alias(col)
            )
            df = PARSED
            print(f"Parsed {col} using format {fmt}")
            break
        except pl.ComputeError:
            continue

    # If parsed, normalize to yyyy-MM-dd
    if PARSED is not None:
        df = df.with_columns(
            pl.col(col).dt.strftime("%Y-%m-%d").alias(col)
        )
    else:
        print(f"WARNING: could not parse {col}; leaving as string")

# ---------- 5. ENFORCE TARGET SCHEMA ----------
target_schema = {
    "trade_date": pl.Utf8,   # keep as string yyyy-MM-dd for safety
    "ticker": pl.Utf8,
    "sector": pl.Utf8,
    "open_price": pl.Float64,
    "close_price": pl.Float64,
    "volume": pl.Int64,
    "validated": pl.Utf8,
    "currency": pl.Utf8,
    "exchange": pl.Utf8,
    "notes": pl.Utf8,
}

casted = []
for col, dtype in target_schema.items():
    if col in df.columns:
        casted.append(pl.col(col).cast(dtype, strict=False))

df = df.with_columns(casted)
print(df)
# ---------- 6. SAVE CLEANED ----------
df = df.unique()
df.write_parquet(CLEAN_PATH)
print("Saved:", CLEAN_PATH)

# ---------- 7. AGGREGATES ----------
print("Creating aggregates...")

# Daily average close
agg1 = (
    df.filter(pl.col("close_price").is_not_null())
      .group_by(["trade_date", "ticker"])
      .agg(pl.col("close_price").mean().alias("avg_close"))
)
agg1.write_parquet(AGG1_PATH)

# Volume by sector
agg2 = (
    df.filter(pl.col("volume").is_not_null())
      .group_by("sector")
      .agg(pl.col("volume").mean().alias("avg_volume"))
)
agg2.write_parquet(AGG2_PATH)

# Daily returns
if {"ticker", "trade_date", "close_price"} <= set(df.columns):
    tmp = df.with_columns(
        pl.col("trade_date").str.strptime(pl.Date)
    ).sort(["ticker", "trade_date"])

    tmp = tmp.with_columns(
        (pl.col("close_price") / pl.col("close_price").shift(1) - 1)
        .over("ticker")
        .alias("return")
    )

    agg3 = tmp.select(["trade_date", "ticker", "return"]).drop_nulls()
    agg3.write_parquet(AGG3_PATH)

print("ETL complete.")
