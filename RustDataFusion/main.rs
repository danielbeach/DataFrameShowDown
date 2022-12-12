use datafusion::prelude::*;
use std::time::Instant;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  let now = Instant::now();
  let ctx = SessionContext::new();
  ctx.register_csv("trips", "data/*.csv", CsvReadOptions::new()).await?;

  // create a plan to run a SQL query
  let df = ctx.sql("
  SELECT COUNT('transaction_id') as cnt,
    date_part('month', to_timestamp(started_at)) as month
  FROM trips
  GROUP BY date_part('month', to_timestamp(started_at))
  ").await?;

  df.show_limit(100).await?;
  let elapsed = now.elapsed();
  println!("Elapsed: {:.2?}", elapsed);
  Ok(())
}
