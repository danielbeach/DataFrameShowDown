from datafusion import SessionContext
from datetime import datetime

d1 = datetime.now()

ctx = SessionContext()

df = ctx.register_csv("trips", "data")

df = ctx.sql("""SELECT COUNT('transaction_id') as cnt,
    date_part('month', to_timestamp(started_at)) as month
  FROM trips
  GROUP BY date_part('month', to_timestamp(started_at))
""")

df.show()

d2 = datetime.now()
print(d2-d1)

