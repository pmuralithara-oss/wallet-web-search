"""Extract all net-loser wallets with >= 10 trades and >= $500 notional."""
import polars as pl

TRADES = "src/data/hf_download/trades.parquet"

# Compute signed amounts: buy = negative (spending), sell = positive (receiving)
maker = pl.scan_parquet(TRADES).select(
    pl.col("maker").str.to_lowercase().alias("address"),
    pl.col("usd_amount"),
    pl.col("maker_direction").alias("direction"),
).with_columns(
    pl.when(pl.col("direction") == "BUY")
      .then(pl.col("usd_amount"))
      .otherwise(pl.lit(0.0))
      .alias("buy_amt"),
    pl.when(pl.col("direction") == "SELL")
      .then(pl.col("usd_amount"))
      .otherwise(pl.lit(0.0))
      .alias("sell_amt"),
).group_by("address").agg(
    pl.col("usd_amount").sum().alias("notional"),
    pl.len().alias("n"),
    pl.col("buy_amt").sum().alias("buy_notional"),
    pl.col("sell_amt").sum().alias("sell_notional"),
).collect()

print(f"Maker aggregation done: {len(maker):,}")

taker = pl.scan_parquet(TRADES).select(
    pl.col("taker").str.to_lowercase().alias("address"),
    pl.col("usd_amount"),
    pl.col("taker_direction").alias("direction"),
).with_columns(
    pl.when(pl.col("direction") == "BUY")
      .then(pl.col("usd_amount"))
      .otherwise(pl.lit(0.0))
      .alias("buy_amt"),
    pl.when(pl.col("direction") == "SELL")
      .then(pl.col("usd_amount"))
      .otherwise(pl.lit(0.0))
      .alias("sell_amt"),
).group_by("address").agg(
    pl.col("usd_amount").sum().alias("notional"),
    pl.len().alias("n"),
    pl.col("buy_amt").sum().alias("buy_notional"),
    pl.col("sell_amt").sum().alias("sell_notional"),
).collect()

print(f"Taker aggregation done: {len(taker):,}")

combined = pl.concat([maker, taker]).group_by("address").agg(
    pl.col("notional").sum(),
    pl.col("n").sum().alias("n_trades"),
    pl.col("buy_notional").sum(),
    pl.col("sell_notional").sum(),
).with_columns(
    (pl.col("buy_notional") - pl.col("sell_notional")).alias("estimated_loss")
)

print(f"Total wallets: {len(combined):,}")

# Debug: check distribution
print(f"Loss stats: min={combined['estimated_loss'].min():.0f} max={combined['estimated_loss'].max():.0f} median={combined['estimated_loss'].median():.0f}")
print(f"Positive (winners): {combined.filter(pl.col('estimated_loss') > 0).height:,}")
print(f"Negative (losers): {combined.filter(pl.col('estimated_loss') < 0).height:,}")
print(f"Zero: {combined.filter(pl.col('estimated_loss') == 0).height:,}")

losers = combined.filter(
    (pl.col("n_trades") >= 10) &
    (pl.col("notional") >= 500) &
    (pl.col("estimated_loss") < 0)
)

print(f">= 10 trades + >= $500 + net loser: {len(losers):,}")
if len(losers) > 0:
    print(f"Median loss: ${losers['estimated_loss'].median():,.0f}")
    print(f"Mean loss: ${losers['estimated_loss'].mean():,.0f}")
    losers.write_parquet("src/data/processed/active_losers.parquet")
    print(f"Saved {len(losers):,} addresses")
else:
    print("No losers found — check buy/sell direction logic")
