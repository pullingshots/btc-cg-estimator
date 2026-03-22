Bitcoin capital gains estimator for Canada

Features:
  - Reads one or more BTC transaction CSV files (exported from Electrum)
  - Merges them into a single transaction stream
  - Deduplicates rows using oc_transaction_hash when available
  - Uses Canadian pooled ACB method
  - Bulk-prefills one BTC/CAD daily rate via CoinAPI historical timeseries
  - Stores rates in SQLite cache
  - Retries with backoff on 429 / 5xx
  - Writes:
      * capital_gains_detailed.csv
      * schedule3_crypto.csv
      * schedule3_crypto.txt

Assumptions:
  - positive BTC amount => acquisition
  - negative BTC amount => disposition
  - all transactions are against CAD
  - timestamps are UTC
  - sell-side network fees are outlays/expenses

Install deps:
```
cpanm DBI DBD::SQLite LWP::UserAgent JSON::PP Text::CSV_XS URI::Escape
```

Usage:
```
  export COINAPI_KEY='your_key_here'
  perl btc_cg_estimate.pl transactions1.csv transactions2.csv [...]
```

Optional env:
```
  export CG_DB=btc_rates.sqlite
  export DETAIL_CSV=capital_gains_detailed.csv
  export SCHEDULE3_CSV=schedule3_crypto.csv
  export SCHEDULE3_TXT=schedule3_crypto.txt
```
