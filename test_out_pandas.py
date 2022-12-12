import pandas as pd
from datetime import datetime
import glob

d1 = datetime.now()
files = glob.glob('data/*.csv')
df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)
df['started_at'] = pd.to_datetime(df['started_at'])
df = df.groupby(df.started_at.dt.month)['ride_id'].count()
print(df)
d2 = datetime.now()
print(d2-d1)
