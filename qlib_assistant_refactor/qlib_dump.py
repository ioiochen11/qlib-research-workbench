from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DumpSummary:
    mode: str
    instrument_count: int
    calendar_count: int


def dump_csv_folder_to_qlib(data_dir: str | Path, qlib_dir: str | Path, freq: str = "day") -> DumpSummary:
    source_dir = Path(data_dir).expanduser()
    target_dir = Path(qlib_dir).expanduser()
    target_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(source_dir.glob("*.csv"))
    if not csv_files:
        raise RuntimeError(f"No CSV files found in {source_dir}")

    calendars_dir = target_dir / "calendars"
    instruments_dir = target_dir / "instruments"
    features_dir = target_dir / "features"
    calendar_path = calendars_dir / f"{freq}.txt"
    instruments_path = instruments_dir / "all.txt"

    existing_calendars = _read_calendars(calendar_path)
    existing_instruments = _read_instruments(instruments_path)
    mode = "update" if existing_calendars else "all"

    data_map = {path.stem.upper(): _read_symbol_csv(path) for path in csv_files}
    all_datetimes = sorted({dt for df in data_map.values() for dt in df["date"].tolist()})
    if not all_datetimes:
        raise RuntimeError(f"No valid rows found in {source_dir}")

    if mode == "all":
        calendars = all_datetimes
    else:
        last_existing = existing_calendars[-1]
        calendars = existing_calendars + [dt for dt in all_datetimes if dt > last_existing]

    _save_calendars(calendar_path, calendars)

    updated_instruments = dict(existing_instruments)
    for symbol, df in data_map.items():
        start_dt = df["date"].min()
        end_dt = df["date"].max()
        if symbol in updated_instruments:
            old_start, old_end = updated_instruments[symbol]
            updated_instruments[symbol] = (min(old_start, start_dt), max(old_end, end_dt))
        else:
            updated_instruments[symbol] = (start_dt, end_dt)

    _save_instruments(instruments_path, updated_instruments)

    for symbol, df in data_map.items():
        symbol_dir = features_dir / symbol.lower()
        symbol_dir.mkdir(parents=True, exist_ok=True)
        existing_range = existing_instruments.get(symbol)
        if mode == "update" and existing_range is not None:
            append_df = df[df["date"] > existing_range[1]].copy()
            if append_df.empty:
                continue
            _dump_symbol_data(append_df, append_df["date"].tolist(), symbol_dir, append=True)
        else:
            _dump_symbol_data(df, calendars, symbol_dir, append=False)

    return DumpSummary(mode=mode, instrument_count=len(data_map), calendar_count=len(calendars))


def _read_symbol_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "date" not in df.columns:
        raise RuntimeError(f"Missing date column in {path}")
    df["date"] = pd.to_datetime(df["date"])
    if "symbol" not in df.columns:
        df["symbol"] = path.stem.upper()
    for field in ["open", "close", "high", "low", "volume", "factor"]:
        if field not in df.columns:
            raise RuntimeError(f"Missing {field} column in {path}")
        df[field] = pd.to_numeric(df[field], errors="coerce")
    df = df.dropna(subset=["date", "open", "close", "high", "low", "volume", "factor"])
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return df


def _read_calendars(path: Path) -> list[pd.Timestamp]:
    if not path.exists():
        return []
    return [pd.Timestamp(line.strip()) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _save_calendars(path: Path, calendars: list[pd.Timestamp]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(pd.Timestamp(item).strftime("%Y-%m-%d") for item in calendars) + "\n",
        encoding="utf-8",
    )


def _read_instruments(path: Path) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    if not path.exists():
        return {}
    mapping: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        symbol, start, end = line.split("\t")
        mapping[symbol.upper()] = (pd.Timestamp(start), pd.Timestamp(end))
    return mapping


def _save_instruments(path: Path, instruments: dict[str, tuple[pd.Timestamp, pd.Timestamp]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for symbol in sorted(instruments):
        start, end = instruments[symbol]
        rows.append(f"{symbol}\t{pd.Timestamp(start).strftime('%Y-%m-%d')}\t{pd.Timestamp(end).strftime('%Y-%m-%d')}")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _dump_symbol_data(df: pd.DataFrame, calendars: list[pd.Timestamp], symbol_dir: Path, append: bool) -> None:
    if df.empty:
        return

    calendars_df = pd.DataFrame({"date": pd.to_datetime(calendars)})
    aligned = calendars_df.merge(df, on="date", how="left").set_index("date")
    aligned = aligned.loc[aligned.index >= df["date"].min()]
    if aligned.empty:
        return

    date_index = calendars.index(aligned.index.min())
    for field in ["open", "close", "high", "low", "volume", "factor"]:
        bin_path = symbol_dir / f"{field}.day.bin"
        values = aligned[field].astype("<f").to_numpy()
        if append and bin_path.exists():
            with bin_path.open("ab") as fp:
                values.tofile(fp)
        else:
            np.hstack([date_index, values]).astype("<f").tofile(str(bin_path))
