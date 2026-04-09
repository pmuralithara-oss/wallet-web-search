# Wallet-First Web Search Pipeline (Track C)

Searches the public web for Polymarket whale wallet addresses, fetches hits, and classifies them for first-person gambling self-disclosure using Claude. Produces ground-truth validation labels for a prediction-market gambling disorder research project.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env  # add your BRAVE_API_KEY
python -m src.db      # init SQLite
```

## Stages

1. **Rank wallets** — load whale parquet, estimate losses, rank by worst loss
2. **Brave search** — search each wallet address, classify and prioritize hits
3. **Fetch pages** — download HTML for priority >= 2 hits
4. **Extract contexts** — find wallet address in page text, extract +/- 500 char windows
5. **Classify** — run Claude classifier on each context window
6. **Export** — produce final_output.csv, review_queue.csv, pipeline_summary.md
