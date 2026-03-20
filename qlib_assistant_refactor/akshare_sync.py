from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import AppConfig
from .qlib_dump import dump_csv_folder_to_qlib
from .qlib_env import ensure_qlib, latest_local_data_date


@dataclass(frozen=True)
class AkshareSyncSummary:
    csv_dir: Path
    qlib_dir: Path
    start_date: str
    end_date: str
    symbol_count: int
    written_csv: int
    dump_mode: str
    calendar_count: int


@dataclass(frozen=True)
class UniverseRefreshSummary:
    universe_name: str
    source: str
    instrument_count: int
    instruments_path: Path
    cache_path: Path


class AkshareDailySync:
    def __init__(self, config: AppConfig):
        self.config = config
        self._name_map: dict[str, str] | None = None

    def sync(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
    ) -> AkshareSyncSummary:
        if self.config.sync_universe == "sse180":
            self.refresh_sse180_universe(as_of_date=end_date)

        base_symbols = list(self._iter_symbols_from_qlib())
        symbols = self._limit_symbols(base_symbols, limit=limit)
        if not base_symbols:
            raise RuntimeError("No symbols available for AkShare sync")

        if start_date is None:
            try:
                start_date = latest_local_data_date(self.config)
            except Exception:
                start_date = (pd.Timestamp.today() - pd.Timedelta(days=90)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = pd.Timestamp.today().strftime("%Y-%m-%d")

        csv_dir = Path(self.config.sync_dir).expanduser() / "akshare_daily"
        csv_dir.mkdir(parents=True, exist_ok=True)

        written = 0
        for symbol in symbols:
            try:
                df = self._fetch_symbol(symbol, start_date=start_date, end_date=end_date)
            except Exception:
                df = self._load_cached_symbol_csv(csv_dir=csv_dir, symbol=symbol, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                continue
            merged = self._merge_with_existing_csv(csv_dir=csv_dir, symbol=symbol, fresh=df)
            merged.to_csv(csv_dir / f"{symbol}.csv", index=False, encoding="utf-8")
            written += 1

        if written == 0:
            raise RuntimeError("AkShare sync produced no CSV files")

        dump_summary = dump_csv_folder_to_qlib(csv_dir, self.config.provider_uri)
        if limit is None:
            self._extend_universe_file(self.config.sync_universe, base_symbols, end_date)
        return AkshareSyncSummary(
            csv_dir=csv_dir,
            qlib_dir=Path(self.config.provider_uri).expanduser(),
            start_date=start_date,
            end_date=end_date,
            symbol_count=len(symbols),
            written_csv=written,
            dump_mode=dump_summary.mode,
            calendar_count=dump_summary.calendar_count,
        )

    def refresh_sse180_universe(self, as_of_date: str | None = None) -> UniverseRefreshSummary:
        latest_date = as_of_date or self._latest_calendar_date()
        cache_path = Path(self.config.sync_dir).expanduser() / "universes" / "sse180.csv"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        source = "akshare_csindex"

        try:
            df = self._fetch_sse180_constituents()
            if df.empty:
                raise RuntimeError("Empty SSE180 constituent table")
            df.to_csv(cache_path, index=False, encoding="utf-8-sig")
        except Exception:
            if not cache_path.exists():
                raise
            df = pd.read_csv(cache_path)
            source = "cached_csv"

        normalized = self._normalize_sse180_constituents(df)
        if normalized.empty:
            raise RuntimeError("No SSE180 constituents available after normalization")

        instruments_path = self._write_universe_file(
            universe_name="sse180",
            symbols=normalized["instrument"].tolist(),
            latest_date=latest_date,
        )
        self._save_cached_name_map(
            {
                str(row["instrument"]): str(row["name"]).strip()
                for _, row in normalized.iterrows()
                if str(row.get("name", "")).strip()
            }
        )
        return UniverseRefreshSummary(
            universe_name="sse180",
            source=source,
            instrument_count=len(normalized),
            instruments_path=instruments_path,
            cache_path=cache_path,
        )

    def _limit_symbols(self, symbols: list[str], limit: int | None = None) -> list[str]:
        symbols = list(symbols)
        benchmark_symbol = self.config.benchmark_symbol
        if benchmark_symbol and benchmark_symbol not in symbols:
            symbols.append(benchmark_symbol)
        symbols = sorted(dict.fromkeys(symbols))
        if limit is not None:
            symbols = symbols[:limit]
            if symbols and benchmark_symbol and benchmark_symbol not in symbols:
                symbols[-1] = benchmark_symbol
                symbols = sorted(dict.fromkeys(symbols))
        return symbols

    def _iter_symbols_from_qlib(self) -> Iterable[str]:
        from_file = self._iter_symbols_from_universe_file()
        if from_file:
            return from_file
        try:
            ensure_qlib(self.config)
            from qlib.data import D

            latest = latest_local_data_date(self.config)
            instruments = D.list_instruments(D.instruments(self.config.sync_universe), latest, latest)
        except Exception:
            return []
        if not instruments:
            return []
        if isinstance(instruments, dict):
            return sorted(instruments.keys())
        return sorted(instruments)

    def _iter_symbols_from_universe_file(self) -> list[str]:
        instruments_path = Path(self.config.provider_uri).expanduser() / "instruments" / f"{self.config.sync_universe}.txt"
        if not instruments_path.exists():
            return []

        rows = []
        for line in instruments_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            symbol, start, end = line.split("\t")
            rows.append((symbol, pd.Timestamp(start), pd.Timestamp(end)))
        if not rows:
            return []
        latest_end = max(end for _, _, end in rows)
        return sorted(symbol for symbol, start, end in rows if start <= latest_end <= end)

    def _extend_universe_file(self, universe_name: str, symbols: list[str], end_date: str) -> None:
        instruments_path = Path(self.config.provider_uri).expanduser() / "instruments" / f"{universe_name}.txt"
        if not instruments_path.exists():
            return

        active_symbols = set(symbols)
        rows = []
        for line in instruments_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            symbol, start, end = line.split("\t")
            if symbol in active_symbols and end < end_date:
                end = end_date
            rows.append(f"{symbol}\t{start}\t{end}")
        instruments_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    def _fetch_symbol(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            import akshare as ak
        except ImportError as exc:
            raise RuntimeError("AkShare is not installed. Install requirements.txt first.") from exc

        start = pd.Timestamp(start_date).strftime("%Y%m%d")
        end = pd.Timestamp(end_date).strftime("%Y%m%d")

        if self._is_index_symbol(symbol):
            raw = ak.index_zh_a_hist(symbol=symbol[2:], period="daily", start_date=start, end_date=end)
            return normalize_index_daily(raw, symbol=symbol, name=self._index_name(symbol))

        raw = ak.stock_zh_a_hist(
            symbol=symbol[2:],
            period="daily",
            start_date=start,
            end_date=end,
            adjust=self.config.sync_adjust,
        )
        return normalize_stock_daily(raw, symbol=symbol, name=self._load_name_map().get(symbol, ""))

    def _load_cached_symbol_csv(
        self,
        csv_dir: Path,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        cached = csv_dir / f"{symbol}.csv"
        if not cached.exists():
            return pd.DataFrame()
        try:
            df = pd.read_csv(cached)
        except Exception:
            return pd.DataFrame()
        if "date" not in df.columns:
            return pd.DataFrame()
        work = df.copy()
        work["date"] = pd.to_datetime(work["date"])
        mask = (work["date"] >= pd.Timestamp(start_date)) & (work["date"] <= pd.Timestamp(end_date))
        work = work.loc[mask].copy()
        if work.empty:
            return pd.DataFrame()
        work["date"] = work["date"].dt.strftime("%Y-%m-%d")
        return work

    def _fetch_sse180_constituents(self) -> pd.DataFrame:
        try:
            import akshare as ak
        except ImportError as exc:
            raise RuntimeError("AkShare is not installed. Install requirements.txt first.") from exc

        try:
            return ak.index_stock_cons_csindex(symbol="000010")
        except Exception:
            return ak.index_stock_cons(symbol="000010")

    def _normalize_sse180_constituents(self, raw: pd.DataFrame) -> pd.DataFrame:
        if raw is None or raw.empty:
            return pd.DataFrame(columns=["instrument", "name"])

        if {"成分券代码", "成分券名称"}.issubset(raw.columns):
            result = raw[["成分券代码", "成分券名称"]].copy()
            result.columns = ["code", "name"]
        elif {"品种代码", "品种名称"}.issubset(raw.columns):
            result = raw[["品种代码", "品种名称"]].copy()
            result.columns = ["code", "name"]
        else:
            return pd.DataFrame(columns=["instrument", "name"])

        result["code"] = result["code"].astype(str).str.zfill(6)
        result["instrument"] = "SH" + result["code"]
        result["name"] = result["name"].astype(str).str.strip()
        result = result[result["instrument"].str.startswith("SH")].drop_duplicates(subset=["instrument"], keep="last")
        return result[["instrument", "name"]].sort_values("instrument").reset_index(drop=True)

    def _write_universe_file(self, universe_name: str, symbols: list[str], latest_date: str) -> Path:
        instruments_path = Path(self.config.provider_uri).expanduser() / "instruments" / f"{universe_name}.txt"
        instruments_path.parent.mkdir(parents=True, exist_ok=True)
        start_date = self._earliest_calendar_date()
        rows = [f"{symbol}\t{start_date}\t{latest_date}" for symbol in sorted(dict.fromkeys(symbols))]
        instruments_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
        return instruments_path

    def _merge_with_existing_csv(self, csv_dir: Path, symbol: str, fresh: pd.DataFrame) -> pd.DataFrame:
        target = csv_dir / f"{symbol}.csv"
        if not target.exists():
            return fresh.sort_values("date").reset_index(drop=True)
        try:
            existing = pd.read_csv(target)
        except Exception:
            return fresh.sort_values("date").reset_index(drop=True)
        combined = pd.concat([existing, fresh], ignore_index=True, sort=False)
        if "date" not in combined.columns:
            return fresh.sort_values("date").reset_index(drop=True)
        combined["date"] = pd.to_datetime(combined["date"])
        combined = combined.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        combined["date"] = combined["date"].dt.strftime("%Y-%m-%d")
        preferred = ["date", "symbol", "name", "open", "close", "high", "low", "volume", "factor"]
        columns = [col for col in preferred if col in combined.columns]
        columns.extend([col for col in combined.columns if col not in columns])
        return combined.loc[:, columns]

    def _load_name_map(self) -> dict[str, str]:
        if self._name_map is None:
            self._name_map = self._load_cached_name_map()
            try:
                import akshare as ak

                for fetcher in [
                    lambda: ak.stock_info_sh_name_code(),
                    lambda: ak.stock_info_sz_name_code(),
                    lambda: ak.stock_info_bj_name_code(),
                    lambda: ak.stock_info_a_code_name(),
                ]:
                    try:
                        spot = fetcher()
                    except Exception:
                        continue
                    self._name_map.update(_extract_name_pairs(spot))
                self._save_cached_name_map(self._name_map)
            except Exception:
                pass
        return self._name_map

    def _name_cache_file(self) -> Path:
        return Path(self.config.sync_dir).expanduser() / "stock_names.csv"

    def _load_cached_name_map(self) -> dict[str, str]:
        cache_file = self._name_cache_file()
        if not cache_file.exists():
            return {}
        try:
            df = pd.read_csv(cache_file)
        except Exception:
            return {}
        required = {"instrument", "name"}
        if not required.issubset(df.columns):
            return {}
        mapping: dict[str, str] = {}
        for _, row in df[["instrument", "name"]].dropna().iterrows():
            instrument = str(row["instrument"]).strip()
            name = str(row["name"]).strip()
            if instrument and name:
                mapping[instrument] = name
        return mapping

    def _save_cached_name_map(self, mapping: dict[str, str]) -> None:
        if not mapping:
            return
        cache_file = self._name_cache_file()
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        rows = [{"instrument": instrument, "name": name} for instrument, name in sorted(mapping.items()) if name]
        pd.DataFrame(rows).to_csv(cache_file, index=False, encoding="utf-8-sig")

    def _earliest_calendar_date(self) -> str:
        day_path = Path(self.config.provider_uri).expanduser() / "calendars" / "day.txt"
        if not day_path.exists():
            return "2005-01-01"
        lines = [line.strip() for line in day_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return lines[0] if lines else "2005-01-01"

    def _latest_calendar_date(self) -> str:
        day_path = Path(self.config.provider_uri).expanduser() / "calendars" / "day.txt"
        if not day_path.exists():
            return pd.Timestamp.today().strftime("%Y-%m-%d")
        lines = [line.strip() for line in day_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return lines[-1] if lines else pd.Timestamp.today().strftime("%Y-%m-%d")

    @staticmethod
    def _is_index_symbol(symbol: str) -> bool:
        return symbol.startswith(("SH000", "SZ399"))

    @staticmethod
    def _index_name(symbol: str) -> str:
        mapping = {
            "SH000010": "上证180",
            "SH000016": "上证50",
            "SH000300": "沪深300",
        }
        return mapping.get(symbol, symbol)


def normalize_stock_daily(raw: pd.DataFrame, symbol: str, name: str = "") -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "symbol", "open", "close", "high", "low", "volume", "factor"])
    renamed = raw.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
        }
    )
    result = renamed[["date", "open", "close", "high", "low", "volume"]].copy()
    result["date"] = pd.to_datetime(result["date"]).dt.strftime("%Y-%m-%d")
    result["symbol"] = symbol
    if name:
        result["name"] = name
    result["factor"] = 1.0
    columns = ["date", "symbol"]
    if name:
        columns.append("name")
    columns.extend(["open", "close", "high", "low", "volume", "factor"])
    return result[columns]


def normalize_index_daily(raw: pd.DataFrame, symbol: str, name: str = "") -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "symbol", "open", "close", "high", "low", "volume", "factor"])
    renamed = raw.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
        }
    )
    result = renamed[["date", "open", "close", "high", "low"]].copy()
    result["date"] = pd.to_datetime(result["date"]).dt.strftime("%Y-%m-%d")
    result["symbol"] = symbol
    if name:
        result["name"] = name
    result["volume"] = 0.0
    result["factor"] = 1.0
    columns = ["date", "symbol"]
    if name:
        columns.append("name")
    columns.extend(["open", "close", "high", "low", "volume", "factor"])
    return result[columns]


def _extract_name_pairs(df: pd.DataFrame) -> dict[str, str]:
    if df is None or df.empty:
        return {}
    code_candidates = ["code", "代码", "证券代码", "A股代码"]
    name_candidates = ["name", "名称", "证券简称", "A股简称"]
    code_col = next((col for col in code_candidates if col in df.columns), None)
    name_col = next((col for col in name_candidates if col in df.columns), None)
    if code_col is None or name_col is None:
        return {}
    mapping: dict[str, str] = {}
    for _, row in df[[code_col, name_col]].dropna().iterrows():
        code = str(row[code_col]).strip()
        name = str(row[name_col]).strip()
        if len(code) != 6 or not name:
            continue
        if code.startswith(("5", "6", "9")):
            exchange = "SH"
        elif code.startswith(("4", "8")):
            exchange = "BJ"
        else:
            exchange = "SZ"
        mapping[f"{exchange}{code}"] = name
    return mapping
