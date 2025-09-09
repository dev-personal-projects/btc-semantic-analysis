from datetime import datetime
from typing import List, Dict, Optional, Iterable

import pandas as pd
from pydantic import BaseModel


class DailySentiment(BaseModel):
    date: datetime         # midnight (naive) UTC-normalized
    source: str
    avg_score: float
    count: int
    label: str


class Aggregator:
    @staticmethod
    def _norm_day(ts: datetime) -> pd.Timestamp:
        """-> naive datetime64[ns] at 00:00:00, normalized in UTC."""
        return pd.to_datetime(ts, utc=True, errors="coerce").tz_convert(None).normalize()

    @staticmethod
    def aggregate(
        records: List[Dict],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        fill_missing: bool = False,
        neutral_fill: float = 50.0,
        no_data_label: str = "no_data",
        sources: Optional[Iterable[str]] = None,
    ) -> List[DailySentiment]:
        """
        Aggregate message-level `records` into daily sentiment per source.

        If `fill_missing=True` and a [start_date, end_date] is provided,
        returns one row per day per source across that range
        (missing days: count=0, label=no_data, avg_score=neutral_fill).
        """
        # Aggregate to per-day
        if records:
            df = pd.DataFrame(records)
            # convert to UTC -> drop tz -> normalize to midnight
            df["date"] = (
                pd.to_datetime(df["date"], utc=True, errors="coerce")
                  .dt.tz_convert(None)
                  .dt.normalize()
            )
            df = df.dropna(subset=["date", "source"])
            agg = (
                df.groupby(["source", "date"], as_index=False)
                  .agg(
                      avg_score=("norm_score", "mean"),
                      count=("norm_score", "size"),
                      label=("label", lambda s: s.mode().iat[0] if not s.mode().empty else "neutral"),
                  )
            )
        else:
            agg = pd.DataFrame(columns=["source", "date", "avg_score", "count", "label"])

        # No gap filling requested
        if not fill_missing or start_date is None or end_date is None:
            out: List[DailySentiment] = []
            for row in agg.itertuples(index=False):
                out.append(
                    DailySentiment(
                        date=(row.date.to_pydatetime()
                              if isinstance(row.date, pd.Timestamp)
                              else pd.to_datetime(row.date).to_pydatetime()),
                        source=row.source,
                        avg_score=float(row.avg_score),
                        count=int(row.count),
                        label=str(row.label),
                    )
                )
            return out

        # Build full calendar grid with consistent datetime64[ns]
        s_norm = Aggregator._norm_day(start_date)
        e_norm = Aggregator._norm_day(end_date)
        full_dates = pd.date_range(start=s_norm, end=e_norm, freq="D")

        all_sources = list(sources) if sources else (sorted(agg["source"].unique()) or ["telegram"])
        full = (
            pd.MultiIndex.from_product([all_sources, full_dates], names=["source", "date"])
            .to_frame(index=False)
        )

        merged = (
            full.merge(agg, on=["source", "date"], how="left")
                .sort_values(["source", "date"])
        )
        merged["count"] = merged["count"].fillna(0).astype(int)
        merged["avg_score"] = merged["avg_score"].astype(float).fillna(neutral_fill)
        merged["label"] = merged["label"].fillna(no_data_label)

        out: List[DailySentiment] = []
        for row in merged.itertuples(index=False):
            out.append(
                DailySentiment(
                    date=row.date.to_pydatetime(),
                    source=row.source,
                    avg_score=float(row.avg_score),
                    count=int(row.count),
                    label=str(row.label),
                )
            )
        return out
