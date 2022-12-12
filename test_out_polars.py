import polars as pl
from datetime import datetime

d1 = datetime.now()
q = (
    pl.scan_csv("data/*.csv", parse_dates=False, dtypes={"start_station_id": pl.Utf8,
                                                        "end_station_id": pl.Utf8})
)

q = q.with_column(pl.col("started_at").str.strptime(pl.Date, fmt="%Y-%m-%d %H:%M:%S").alias("started_at"))
df = q.lazy().groupby(pl.col("started_at").dt.month()).agg(pl.count()).collect()
print(df)
d2 = datetime.now()
print(d2-d1)
