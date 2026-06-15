# Using Call Report data with Python

This article shows how to use the Parquet files created by `ffiec.pq`
from Python with Polars.

## 1 Using the data with Python

### 1.1 Example 3: Plotting trends in total assets for the biggest banks

Lest you think that, because `ffiec.pq` is an R package, the processed
data are of no interest to others, I now provide some basic analysis
using Python. For this, I am going to use the Polars package rather than
pandas.

While pandas is the dominant data frame library in Python, it would
struggle to work with Parquet data on the scale of what `ffiec.pq` has
produced, even though it’s a fairly modest amount of data. Loading
50-100 GB of data into RAM is not fun for most people’s computer
set-ups. Even if you have RAM in the hundreds of GBs, not loading it
will save you time.

As we shall see, Polars does fine with the data and, if anything, is
noticeably faster than DuckDB (using `dplyr`) for the queries I use in
this note.

``` python
from pathlib import Path
import polars as pl
import os
```

Because there is no `ffiec.pq` package for Python, I mimic the
[`ffiec_scan_pqs()`](https://iangow.github.io/ffiec.pq/reference/ffiec_scan_pqs.md)
function using the following code.

``` python
def ffiec_scan_pqs(schedule=None, *, 
                   schema="ffiec", data_dir=None):
    if data_dir is None:
        data_dir = Path(os.environ["DATA_DIR"]).expanduser()

    path = data_dir / schema if schema else data_dir

    if schedule is None:
        raise ValueError("You must supply `schedule`.")
    files = list(path.glob(f"{schedule}_*.parquet"))
    if not files:
        raise FileNotFoundError(
          f"No Parquet files found for schedule '{schedule}' in {path}"
        )
    
    return pl.concat([pl.scan_parquet(f) for f in files])
```

Now I can “load” the data much as I did with R.

``` python
ffiec_float = ffiec_scan_pqs("ffiec_float")
por = ffiec_scan_pqs(schedule="por")
```

While I am going to focus on total assets in this analysis, I show the
parallels between the R code and the Polars code by collecting data on
the same items. I don’t need
[`ffiec_pivot()`](https://iangow.github.io/ffiec.pq/reference/ffiec_pivot.md)
with Polars because the built-in `.pivot()` method does everything I
need. Polars is even faster than R/DuckDB.

``` python
import time

bs_items = ["RCFD2170", "RCON2170",
            "RCFD2948", "RCON2948",
            "RCFD3210", "RCON3210",
            "RCFD3000", "RCON3000"]
start = time.perf_counter()
bs_data = (
    ffiec_float
    .filter(pl.col("item").is_in(bs_items))
    .pivot(
        on = "item",
        on_columns = bs_items,
        index = ["IDRSSD", "date"],
        values = "value")
    .with_columns(
        total_assets = pl.coalesce(pl.col("RCFD2170"), pl.col("RCON2170")),
        total_liabs = pl.coalesce(pl.col("RCFD2948"), pl.col("RCON2948")),
        equity = pl.coalesce(pl.col("RCFD3210"), pl.col("RCON3210")),
        nci = pl.coalesce(pl.col("RCFD3000"), pl.col("RCON3000")),
    )
    .with_columns(
        eq_liab = pl.col("total_liabs") + pl.col("equity") + pl.col("nci")
    )
    .collect()
)
end = time.perf_counter()
elapsed = end - start
print(f'Time taken: {elapsed:.6f} seconds')
```

We can peek at the data. So Polars has processed GBs of data and created
a table with over 600,000 rows in well under a second. Don’t try this at
home … if you’re using pandas.

Note that `ffiec_float` is a `pl.LazyFrame`, but `.collect()` creates a
non-lazy `pl.DataFrame`.

``` python
import polars.selectors as cs

bs_data.select(cs.exclude("^(RCON|RCFD).*$"))
```

I identify the top 5 banks by assets on `2025-09-30`. Because I will
want to merge this with information on `por`, which is a lazy data frame
(`pl.LazyFrame`), I make it `top_5` “lazy” by appending `.lazy()` at the
end.

``` python
top_5 = (
    bs_data
    .filter(pl.col("date") == pl.date(2025, 9, 30))
    .sort("total_assets", descending=True)
    .with_row_index("ta_rank", offset=1)
    .filter(pl.col("ta_rank") <= 5)
    .lazy()
)
```

I then grab the names of the banks from `por`.

``` python
top_5_names = (
    top_5
    .join(
        por.select(["IDRSSD", "date", "financial_institution_name"]),
        on=["IDRSSD", "date"],
        how="inner",
    )
    .sort("ta_rank")
    .select(["IDRSSD", "financial_institution_name", "ta_rank"])
    .rename({"financial_institution_name": "bank"})
    .collect()
)
```

``` python
top_5_names
```

I can combine the names with `bs_data` using `.join()`.

``` python
bs_panel_data = bs_data.join(top_5_names, on="IDRSSD", how="inner")
```

Users of pandas who are unfamiliar might be impressed by the performance
of Polars, but wonder how they can fit it into their workflows. Of
course, it is easy enough to call `.to_pandas()` and create a pandas
`pd.DataFrame`:

``` python
pdf = (
    bs_panel_data
    .filter(pl.col("date") >= pl.date(2020, 1, 1))
    .to_pandas()
)
```

This means a pandas user can use all the familiar tools used for
plotting or statistical analysis. Because the names are a little long
for plotting purposes, I use a little dictionary to replace them.

``` python
bank_names = {
    476810: "Citibank",
    504713: "US Bank",
    852218: "JPMorgan Chase",
    451965: "Wells Fargo",
    480228: "Bank of America",
}

pdf["bank"] = pdf["IDRSSD"].map(bank_names)
```

And then I use Seaborn and Matplotlib to make a small (but uninspiring)
plot.
