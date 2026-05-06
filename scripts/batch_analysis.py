from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def load_jsonl(path: Path) -> pd.DataFrame:
    rows = []
    if not path.exists():
        return pd.DataFrame(rows)
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return pd.DataFrame(rows)


def compute_batch_features(events: pd.DataFrame, metadata: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if events.empty:
        return {}

    events = events.copy()
    events["timestamp"] = pd.to_datetime(events["timestamp"], utc=True)
    events["window_hour"] = events["timestamp"].dt.floor("1h")
    events["window_15m"] = events["timestamp"].dt.floor("15min")

    user_group = events.groupby(["user_id", "window_hour"], dropna=False)
    user_features = user_group.agg(
        total_events=("event_type", "count"),
        click_events=("event_type", lambda s: (s == "click").sum()),
        dwell_sum=("dwell_time_ms", "sum"),
    ).reset_index()
    user_features["click_rate"] = user_features.apply(
        lambda row: row["click_events"] / row["total_events"] if row["total_events"] else 0.0,
        axis=1,
    )
    user_features["avg_dwell_time"] = user_features.apply(
        lambda row: row["dwell_sum"] / row["total_events"] if row["total_events"] else 0.0,
        axis=1,
    )

    content_group = events.groupby(["content_id", "window_15m"], dropna=False)
    content_features = content_group.agg(
        views=("event_type", lambda s: (s == "view").sum()),
        likes=("event_type", lambda s: (s == "like").sum()),
        shares=("event_type", lambda s: (s == "share").sum()),
    ).reset_index()
    content_features["engagement_rate"] = content_features.apply(
        lambda row: ((row["likes"] + row["shares"]) / row["views"]) if row["views"] else 0.0,
        axis=1,
    )

    if not metadata.empty:
        merged = events.merge(metadata[["content_id", "category"]], on="content_id", how="left")
        merged["category"] = merged["category"].fillna("unknown")
        category_group = merged.groupby(["user_id", "category", "window_hour"], dropna=False).size().reset_index(name="category_affinity_score")
    else:
        category_group = pd.DataFrame()

    return {
        "user_features": user_features,
        "content_features": content_features,
        "category_affinity": category_group,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=False)
    args = parser.parse_args()

    events = load_jsonl(args.events)
    metadata = load_jsonl(args.metadata) if args.metadata else pd.DataFrame()
    features = compute_batch_features(events, metadata)

    for name, frame in features.items():
        print(f"## {name}")
        if frame.empty:
            print("No rows")
        else:
            print(frame.head(10).to_string(index=False))
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
