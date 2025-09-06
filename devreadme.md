btc_sentiment/
├── README.md
├── pyproject.toml   # or requirements.txt
├── .env.example     # env variable names only (never your real keys)
├── .gitignore
├── notebooks/
│   └── 01_data_ingest_and_explore.ipynb
├── scripts/
│   └── run_pipeline.py          # cli entry for scripted runs
├── src/
│   └── btc_sentiment/
│       ├── __init__.py
│       ├── config.py            # loads env + shared config
│       ├── adapters/
│       │   ├── x_api_adapter.py         # Twitter / X
│       │   └── telegram_adapter.py     # Telethon
│       ├── services/
│       │   ├── sentiment_service.py    # VADER wrapper
│       │   └── aggregator.py           # resampling, normalization
│       ├── pipelines/
│       │   └── ingest_pipeline.py      # orchestrates adapters -> services
│       └── utils/
│           ├── io.py                   # csv / parquet helpers
│           └── viz.py                  # plotting helpers
└── tests/
    ├── test_telegram_adapter.py
    └── test_sentiment_service.py


    pip install -e .[dev]
