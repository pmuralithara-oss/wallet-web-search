"""Stage 1: Load whale parquet, compute DSM-5 behavioral severity scores, rank and select."""
import argparse
import logging

import polars as pl
import numpy as np

from src.config import DATA_DIR, LOGS_DIR
from src.db import get_connection, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "stage1.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def percentile_score(series: pl.Series) -> pl.Series:
    """Convert a series to 0-1 percentile ranks. Higher = more extreme."""
    rank = series.rank(method="average")
    return (rank - 1) / max(rank.max() - 1, 1)


def run(limit: int = 10000, min_loss: float = 5000, parquet_path: str | None = None):
    init_db()

    path = parquet_path or str(DATA_DIR / "input_whales.parquet")
    log.info(f"Loading whale data from {path}")
    df = pl.read_parquet(path)
    log.info(f"Loaded {len(df):,} wallets")

    # Filter: non-bot, net losers above threshold
    df = df.filter(pl.col("bot_flag") == False)
    df = df.with_columns(
        (pl.col("buy_notional") - pl.col("sell_notional")).alias("estimated_loss")
    )
    df = df.filter(pl.col("estimated_loss") < -min_loss)
    log.info(f"After filters (non-bot, loss > ${min_loss:,.0f}): {len(df):,} wallets")

    # === DSM-5 GAMBLING DISORDER PROXY SCORES ===
    # Each score is 0-1 percentile. Higher = more pathological signal.

    # 1. LOSS SEVERITY: fraction of capital lost (loss / total_notional)
    #    DSM: "needs to gamble with increasing amounts to achieve desired excitement"
    #    Proxy: how much of their money they burned through
    df = df.with_columns(
        (pl.col("estimated_loss").abs() / pl.col("total_notional").clip(lower_bound=1))
        .clip(upper_bound=1.0)
        .alias("loss_fraction")
    )

    # 2. COMPULSIVE FREQUENCY: avg trades per day relative to population
    #    DSM: "preoccupation with gambling" + "restlessness when attempting to cut down"
    #    Proxy: abnormally high daily trading frequency
    #    Also factor in: trading on most days of the span (persistence)
    df = df.with_columns(
        (pl.col("n_days_active").cast(pl.Float64) /
         pl.col("trading_span_days").clip(lower_bound=1).cast(pl.Float64))
        .clip(upper_bound=1.0)
        .alias("active_day_ratio")
    )

    # 3. CHASING PROXY: continued trading despite losses
    #    DSM: "chasing losses — returning to get even"
    #    Proxy: high n_trades AND high loss = kept trading while losing
    #    Also: trades_last_30d relative to total (still active recently despite losses)
    df = df.with_columns(
        (pl.col("trades_last_30d").cast(pl.Float64) /
         pl.col("n_trades").clip(lower_bound=1).cast(pl.Float64))
        .alias("recent_trade_ratio")
    )

    # 4. CONCENTRATION RISK: betting on few markets
    #    DSM: "loss of control" — inability to diversify, fixated
    #    Proxy: low market count relative to trade count = compulsive focus
    df = df.with_columns(
        (1.0 / pl.col("n_markets").clip(lower_bound=1).cast(pl.Float64))
        .alias("concentration")
    )

    # 5. BURST TRADING: max daily trades vs average
    #    DSM: "unable to control, cut back, or stop gambling"
    #    Proxy: extreme burst days suggest loss-of-control episodes
    df = df.with_columns(
        (pl.col("max_daily_trades").cast(pl.Float64) /
         pl.col("avg_trades_per_day").clip(lower_bound=0.01))
        .alias("burst_ratio")
    )

    # 6. DIRECTIONAL STUBBORNNESS: strong one-way bias
    #    DSM: "chasing" — doubling down in one direction despite losses
    #    Proxy: high directional_bias = kept buying (or selling) without adjusting
    df = df.with_columns(
        pl.col("directional_bias").abs().alias("abs_directional_bias")
    )

    # === COMPOSITE SCORE ===
    # Convert each to percentile rank, then weighted average
    score_cols = {
        "loss_fraction": 0.25,        # how much they lost
        "active_day_ratio": 0.15,     # how obsessively they traded
        "avg_trades_per_day": 0.15,   # raw frequency
        "recent_trade_ratio": 0.15,   # still active despite losses (chasing)
        "concentration": 0.10,        # fixated on few markets
        "burst_ratio": 0.10,          # loss-of-control episodes
        "abs_directional_bias": 0.10, # one-directional stubbornness
    }

    for col in score_cols:
        pct_col = f"{col}_pct"
        values = df[col].to_numpy()
        # Handle nulls
        valid = ~np.isnan(values)
        ranks = np.zeros_like(values)
        if valid.sum() > 0:
            from scipy.stats import rankdata
            ranks[valid] = rankdata(values[valid]) / valid.sum()
        df = df.with_columns(pl.Series(pct_col, ranks))

    # Weighted composite
    composite = np.zeros(len(df))
    for col, weight in score_cols.items():
        composite += df[f"{col}_pct"].to_numpy() * weight

    df = df.with_columns(pl.Series("dsm5_severity_score", composite))

    # Rank by composite score descending
    df = df.sort("dsm5_severity_score", descending=True)
    df = df.with_row_index("severity_rank", offset=1)

    # Select top N
    selected = df.head(limit)

    log.info(f"\nDSM-5 severity score distribution (all {len(df):,} wallets):")
    log.info(f"  Max:    {df['dsm5_severity_score'][0]:.4f}")
    log.info(f"  P99:    {df['dsm5_severity_score'][int(len(df)*0.01)]:.4f}")
    log.info(f"  Median: {df['dsm5_severity_score'][len(df)//2]:.4f}")
    log.info(f"  Min:    {df['dsm5_severity_score'][-1]:.4f}")

    # Insert into DB
    conn = get_connection()
    conn.execute("DELETE FROM wallets")

    selected_addrs = set(selected["address"].to_list())
    rows = []
    for row in df.iter_rows(named=True):
        rows.append((
            row["address"],
            row["total_notional"],
            row["n_trades"],
            row["estimated_loss"],
            int(row["severity_rank"]),
            row["address"] in selected_addrs,
        ))

    conn.executemany(
        "INSERT OR REPLACE INTO wallets (address, total_notional, total_trades, estimated_loss, loss_rank, selected) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    log.info(f"Inserted {len(rows):,} wallets, {limit} selected")

    # Print top 20
    print("\n" + "=" * 110)
    print(f"TOP 20 WALLETS BY DSM-5 BEHAVIORAL SEVERITY (of {limit:,} selected)")
    print("=" * 110)
    print(f"{'Rank':>5}  {'Address':42}  {'Score':>7}  {'Est. Loss':>14}  {'Trades':>8}  {'Days':>5}  {'Mkts':>5}  {'LossFrac':>8}")
    print("-" * 110)
    for row in selected.head(20).iter_rows(named=True):
        print(f"{row['severity_rank']:>5}  {row['address']:42}  {row['dsm5_severity_score']:.4f}  ${row['estimated_loss']:>13,.2f}  {row['n_trades']:>8,}  {row['n_days_active']:>5}  {row['n_markets']:>5}  {row['loss_fraction']:>7.1%}")

    # Score component breakdown for top 5
    print(f"\nComponent breakdown (top 5):")
    for row in selected.head(5).iter_rows(named=True):
        print(f"  {row['address'][:20]}...")
        for col, weight in score_cols.items():
            pct = row[f"{col}_pct"]
            raw = row[col]
            print(f"    {col:25} raw={raw:>10.2f}  pct={pct:.3f}  weighted={pct*weight:.4f}")
        print()

    conn.close()
    print(f"Stage 1 complete. {limit:,} wallets selected.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--min-loss", type=float, default=5000)
    parser.add_argument("--parquet", type=str, default=None)
    args = parser.parse_args()
    run(limit=args.limit, min_loss=args.min_loss, parquet_path=args.parquet)
