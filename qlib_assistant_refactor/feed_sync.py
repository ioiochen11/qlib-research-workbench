from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, time
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests

from .akshare_sync import AkshareDailySync, normalize_index_daily, normalize_stock_daily
from .config import AppConfig
from .qlib_dump import dump_csv_folder_to_qlib


@dataclass(frozen=True)
class FeedManifest:
    feed_type: str
    source_name: str
    as_of_date: str
    fetched_at: str
    coverage_ratio: float
    record_count: int
    validation_status: str
    validation_errors: List[str] = field(default_factory=list)
    eligible_for_daily_run: bool = False
    output_path: str = ""
    raw_paths: List[str] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FeedSyncSummary:
    feed_type: str
    as_of_date: str
    output_path: Path
    manifest_path: Path
    record_count: int
    coverage_ratio: float
    eligible_for_daily_run: bool
    validation_status: str
    validation_errors: List[str] = field(default_factory=list)
    raw_paths: List[Path] = field(default_factory=list)


@dataclass(frozen=True)
class FreshnessSummary:
    as_of_date: str
    eligible_for_daily_run: bool
    validation_status: str
    validation_errors: List[str]
    manifest_paths: List[Path]
    fetched_at: str
    manifest_path: Path


class FeedSyncManager:
    def __init__(self, config: AppConfig):
        self.config = config
        self.akshare = AkshareDailySync(config)

    def sync_market(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
    ) -> FeedSyncSummary:
        if self.config.sync_universe == "sse180":
            self.akshare.refresh_sse180_universe(as_of_date=end_date)

        base_symbols = list(self.akshare._iter_symbols_from_qlib())
        symbols = self.akshare._limit_symbols(base_symbols, limit=limit)
        if not symbols:
            return self._failed_market_summary(
                as_of_date=end_date or pd.Timestamp.today().strftime("%Y-%m-%d"),
                validation_errors=["no_symbols_available_for_market_sync"],
            )

        latest_calendar = self.akshare._latest_calendar_date()
        start_date = start_date or latest_calendar
        end_date = end_date or pd.Timestamp.today().strftime("%Y-%m-%d")
        as_of_date = end_date

        raw_paths = []
        gold_dir = self._gold_dir("market") / "validated_daily"
        gold_dir.mkdir(parents=True, exist_ok=True)
        raw_ak_dir = self._raw_dir("market", "akshare")
        raw_em_dir = self._raw_dir("market", "eastmoney")
        valid_symbols: list[str] = []
        validation_errors: list[str] = []
        total_rows = 0

        for symbol in symbols:
            primary = self._fetch_market_frame(symbol, start_date, end_date, source_name="akshare", raw_dir=raw_ak_dir)
            backup = self._fetch_market_frame(symbol, start_date, end_date, source_name="eastmoney", raw_dir=raw_em_dir)
            raw_paths.extend([raw_ak_dir / f"{symbol}.csv", raw_em_dir / f"{symbol}.csv"])
            validated, symbol_errors = self._validate_market_pair(
                symbol=symbol,
                primary=primary,
                backup=backup,
                start_date=start_date,
                end_date=end_date,
            )
            if symbol_errors:
                validation_errors.extend(symbol_errors)
            if validated.empty:
                continue
            merged = self.akshare._merge_with_existing_csv(csv_dir=gold_dir, symbol=symbol, fresh=validated)
            merged.to_csv(gold_dir / f"{symbol}.csv", index=False, encoding="utf-8")
            valid_symbols.append(symbol)
            total_rows += len(validated)

        if not valid_symbols:
            return self._failed_market_summary(
                as_of_date=as_of_date,
                validation_errors=(validation_errors[:50] or ["market_sync_produced_no_validated_symbols"]),
                raw_paths=[path for path in raw_paths if path.exists()],
            )

        dump_summary = dump_csv_folder_to_qlib(gold_dir, self.config.provider_uri)
        if limit is None:
            self.akshare._extend_universe_file(self.config.sync_universe, base_symbols, end_date)
        coverage_ratio = len(valid_symbols) / len(symbols)
        if self.config.benchmark_symbol not in valid_symbols:
            validation_errors.append(f"missing_benchmark:{self.config.benchmark_symbol}")
        if coverage_ratio < float(self.config.min_universe_coverage):
            validation_errors.append(
                f"coverage_below_threshold:{coverage_ratio:.3f}<{self.config.min_universe_coverage:.3f}"
            )
        eligible = self.config.benchmark_symbol in valid_symbols and coverage_ratio >= float(self.config.min_universe_coverage)
        snapshot_path = self._gold_dir("market") / f"validated_snapshot_{as_of_date}.csv"
        self._build_market_snapshot(gold_dir=gold_dir, symbols=valid_symbols, as_of_date=as_of_date).to_csv(
            snapshot_path,
            index=False,
            encoding="utf-8-sig",
        )
        manifest = FeedManifest(
            feed_type="market",
            source_name="+".join(self.config.source_priority.get("market", ["akshare", "eastmoney"])),
            as_of_date=as_of_date,
            fetched_at=self._now_iso(),
            coverage_ratio=coverage_ratio,
            record_count=total_rows,
            validation_status="passed" if eligible else "failed",
            validation_errors=validation_errors[:50],
            eligible_for_daily_run=eligible,
            output_path=str(snapshot_path),
            raw_paths=[str(path) for path in raw_paths if path.exists()],
            extra={
                "validated_symbol_count": len(valid_symbols),
                "requested_symbol_count": len(symbols),
                "dump_mode": dump_summary.mode,
                "calendar_count": dump_summary.calendar_count,
            },
        )
        manifest_path = self._save_manifest(manifest)
        return FeedSyncSummary(
            feed_type="market",
            as_of_date=as_of_date,
            output_path=snapshot_path,
            manifest_path=manifest_path,
            record_count=total_rows,
            coverage_ratio=coverage_ratio,
            eligible_for_daily_run=eligible,
            validation_status=manifest.validation_status,
            validation_errors=manifest.validation_errors,
            raw_paths=[Path(item) for item in manifest.raw_paths],
        )

    def _failed_market_summary(
        self,
        as_of_date: str,
        validation_errors: list[str],
        raw_paths: list[Path] | None = None,
    ) -> FeedSyncSummary:
        manifest = FeedManifest(
            feed_type="market",
            source_name="+".join(self.config.source_priority.get("market", ["akshare", "eastmoney"])),
            as_of_date=as_of_date,
            fetched_at=self._now_iso(),
            coverage_ratio=0.0,
            record_count=0,
            validation_status="failed",
            validation_errors=validation_errors[:50],
            eligible_for_daily_run=False,
            output_path="",
            raw_paths=[str(path) for path in (raw_paths or []) if Path(path).exists()],
        )
        manifest_path = self._save_manifest(manifest)
        return FeedSyncSummary(
            feed_type="market",
            as_of_date=as_of_date,
            output_path=Path(manifest.output_path or self._gold_dir("market")),
            manifest_path=manifest_path,
            record_count=0,
            coverage_ratio=0.0,
            eligible_for_daily_run=False,
            validation_status="failed",
            validation_errors=manifest.validation_errors,
            raw_paths=[Path(item) for item in manifest.raw_paths],
        )

    def sync_fundamentals(self, as_of_date: str | None = None, limit: int | None = None) -> FeedSyncSummary:
        symbols = self._limit_equity_symbols(list(self.akshare._iter_equity_symbols()), limit=limit)
        as_of_date = as_of_date or pd.Timestamp.today().strftime("%Y-%m-%d")
        raw_dir = self._raw_dir("fundamentals", "eastmoney_individual")
        raw_paths: list[Path] = []
        rows: list[dict[str, Any]] = []
        available_count = 0

        report_frames = self._collect_report_frames(as_of_date=as_of_date)
        report_lookup = self._build_latest_report_lookup(report_frames)

        for instrument in symbols:
            info = self._fetch_individual_info(instrument, raw_dir=raw_dir)
            raw_paths.append(raw_dir / f"{instrument}.csv")
            report = report_lookup.get(instrument, {})
            if info or report:
                available_count += 1
            rows.append(self._build_fundamental_row(instrument, as_of_date=as_of_date, info=info, report=report))

        output = self._gold_dir("fundamentals") / f"fundamentals_{as_of_date}.csv"
        output.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(output, index=False, encoding="utf-8-sig")
        latest_output = self._gold_dir("fundamentals") / "latest_fundamentals.csv"
        pd.DataFrame(rows).to_csv(latest_output, index=False, encoding="utf-8-sig")
        coverage_ratio = available_count / len(symbols) if symbols else 0.0
        validation_errors: list[str] = []
        if coverage_ratio < float(self.config.min_universe_coverage):
            validation_errors.append(
                f"coverage_below_threshold:{coverage_ratio:.3f}<{self.config.min_universe_coverage:.3f}"
            )
        eligible = coverage_ratio >= float(self.config.min_universe_coverage) and bool(rows)
        manifest = FeedManifest(
            feed_type="fundamentals",
            source_name="+".join(self.config.source_priority.get("fundamentals", [])),
            as_of_date=as_of_date,
            fetched_at=self._now_iso(),
            coverage_ratio=coverage_ratio,
            record_count=len(rows),
            validation_status="passed" if eligible else "failed",
            validation_errors=validation_errors,
            eligible_for_daily_run=eligible,
            output_path=str(output),
            raw_paths=[str(path) for path in raw_paths if path.exists()],
        )
        manifest_path = self._save_manifest(manifest)
        return FeedSyncSummary(
            feed_type="fundamentals",
            as_of_date=as_of_date,
            output_path=output,
            manifest_path=manifest_path,
            record_count=len(rows),
            coverage_ratio=coverage_ratio,
            eligible_for_daily_run=eligible,
            validation_status=manifest.validation_status,
            validation_errors=manifest.validation_errors,
            raw_paths=[Path(item) for item in manifest.raw_paths],
        )

    def sync_events(
        self,
        as_of_date: str | None = None,
        lookback_days: int = 3,
        limit: int | None = None,
    ) -> FeedSyncSummary:
        as_of_date = as_of_date or pd.Timestamp.today().strftime("%Y-%m-%d")
        symbols = self._limit_equity_symbols(list(self.akshare._iter_equity_symbols()), limit=limit)
        raw_notice_dir = self._raw_dir("events", "eastmoney_notice")
        raw_news_dir = self._raw_dir("events", "eastmoney_news")
        raw_paths: list[Path] = []

        notice_df = self._fetch_notice_window(as_of_date=as_of_date, lookback_days=lookback_days, raw_dir=raw_notice_dir)
        raw_paths.extend(sorted(raw_notice_dir.glob("*.csv")))
        notice_summary = self._summarize_notice_events(notice_df, symbols=symbols)

        news_rows: list[pd.DataFrame] = []
        news_success = 0
        for instrument in symbols:
            frame, ok = self._fetch_news_for_symbol(
                instrument=instrument,
                as_of_date=as_of_date,
                lookback_days=lookback_days,
                raw_dir=raw_news_dir,
            )
            raw_paths.append(raw_news_dir / f"{instrument}.csv")
            if ok:
                news_success += 1
            if not frame.empty:
                news_rows.append(frame)
        news_df = pd.concat(news_rows, ignore_index=True) if news_rows else pd.DataFrame()
        news_summary = self._summarize_news_events(news_df, symbols=symbols)

        rows = []
        for instrument in symbols:
            notice = notice_summary.get(instrument, {})
            news = news_summary.get(instrument, {})
            rows.append(
                {
                    "instrument": instrument,
                    "as_of_date": as_of_date,
                    "notice_count_3d": int(notice.get("notice_count_3d", 0)),
                    "notice_tags": str(notice.get("notice_tags", "")),
                    "notice_summary": str(notice.get("notice_summary", "")),
                    "news_count_3d": int(news.get("news_count_3d", 0)),
                    "news_sentiment": str(news.get("news_sentiment", "中性")),
                    "news_risk_tags": str(news.get("news_risk_tags", "")),
                    "news_summary": str(news.get("news_summary", "")),
                    "event_risk_tag": self._merge_event_risk(notice, news),
                }
            )

        output = self._gold_dir("events") / f"events_{as_of_date}.csv"
        output.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(output, index=False, encoding="utf-8-sig")
        latest_output = self._gold_dir("events") / "latest_events.csv"
        pd.DataFrame(rows).to_csv(latest_output, index=False, encoding="utf-8-sig")
        coverage_ratio = news_success / len(symbols) if symbols else 0.0
        validation_errors: list[str] = []
        if coverage_ratio < float(self.config.min_universe_coverage):
            validation_errors.append(
                f"coverage_below_threshold:{coverage_ratio:.3f}<{self.config.min_universe_coverage:.3f}"
            )
        eligible = coverage_ratio >= float(self.config.min_universe_coverage)
        manifest = FeedManifest(
            feed_type="events",
            source_name="+".join(self.config.source_priority.get("events", [])),
            as_of_date=as_of_date,
            fetched_at=self._now_iso(),
            coverage_ratio=coverage_ratio,
            record_count=len(rows),
            validation_status="passed" if eligible else "failed",
            validation_errors=validation_errors,
            eligible_for_daily_run=eligible,
            output_path=str(output),
            raw_paths=[str(path) for path in raw_paths if path.exists()],
        )
        manifest_path = self._save_manifest(manifest)
        return FeedSyncSummary(
            feed_type="events",
            as_of_date=as_of_date,
            output_path=output,
            manifest_path=manifest_path,
            record_count=len(rows),
            coverage_ratio=coverage_ratio,
            eligible_for_daily_run=eligible,
            validation_status=manifest.validation_status,
            validation_errors=manifest.validation_errors,
            raw_paths=[Path(item) for item in manifest.raw_paths],
        )

    def verify_freshness(self, as_of_date: str | None = None) -> FreshnessSummary:
        as_of_date = as_of_date or pd.Timestamp.today().strftime("%Y-%m-%d")
        manifest_paths = []
        validation_errors: list[str] = []
        manifest_rows: list[FeedManifest] = []

        if not self._after_market_close(as_of_date):
            validation_errors.append(f"before_market_close_cutoff:{self.config.market_close_cutoff}")

        for feed in self.config.required_feeds:
            path = self._manifest_path(feed, as_of_date)
            manifest_paths.append(path)
            if not path.exists():
                validation_errors.append(f"missing_manifest:{feed}")
                continue
            manifest = self._load_manifest(path)
            manifest_rows.append(manifest)
            max_age = int(self.config.max_feed_age_hours.get(feed, 24))
            age_hours = self._age_hours(manifest.fetched_at)
            if age_hours > max_age:
                validation_errors.append(f"stale_feed:{feed}:{age_hours:.2f}h>{max_age}h")
            if manifest.validation_status != "passed":
                validation_errors.append(f"invalid_feed:{feed}:{manifest.validation_status}")
            if not manifest.eligible_for_daily_run:
                validation_errors.append(f"ineligible_feed:{feed}")

        eligible = not validation_errors
        summary = FeedManifest(
            feed_type="freshness",
            source_name="+".join(self.config.required_feeds),
            as_of_date=as_of_date,
            fetched_at=self._now_iso(),
            coverage_ratio=1.0 if eligible else 0.0,
            record_count=len(manifest_rows),
            validation_status="passed" if eligible else "failed",
            validation_errors=validation_errors,
            eligible_for_daily_run=eligible,
            output_path="",
            raw_paths=[str(path) for path in manifest_paths if path.exists()],
            extra={"required_feeds": self.config.required_feeds},
        )
        manifest_path = self._save_manifest(summary)
        return FreshnessSummary(
            as_of_date=as_of_date,
            eligible_for_daily_run=eligible,
            validation_status=summary.validation_status,
            validation_errors=validation_errors,
            manifest_paths=manifest_paths,
            fetched_at=summary.fetched_at,
            manifest_path=manifest_path,
        )

    def show_manifest(self, as_of_date: str | None = None) -> Dict[str, Any]:
        as_of_date = as_of_date or pd.Timestamp.today().strftime("%Y-%m-%d")
        manifest_dir = self._manifest_dir(as_of_date)
        items = []
        for path in sorted(manifest_dir.glob("*.json")):
            items.append(asdict(self._load_manifest(path)))
        return {"as_of_date": as_of_date, "manifest_dir": str(manifest_dir), "items": items}

    def _fetch_market_frame(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source_name: str,
        raw_dir: Path,
    ) -> pd.DataFrame:
        raw_dir.mkdir(parents=True, exist_ok=True)
        if source_name == "akshare":
            fetcher = lambda: self.akshare._fetch_symbol(symbol, start_date=start_date, end_date=end_date)
        elif source_name == "eastmoney":
            fetcher = lambda: self._fetch_eastmoney_symbol(symbol, start_date=start_date, end_date=end_date)
        else:
            raise ValueError(f"Unsupported market source: {source_name}")

        try:
            frame = fetcher()
        except Exception:
            frame = self.akshare._load_cached_symbol_csv(
                csv_dir=raw_dir,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            )
        if frame is None:
            frame = pd.DataFrame()
        if not frame.empty:
            merged = self.akshare._merge_with_existing_csv(csv_dir=raw_dir, symbol=symbol, fresh=frame)
            merged.to_csv(raw_dir / f"{symbol}.csv", index=False, encoding="utf-8")
            work = merged.copy()
            work["date"] = pd.to_datetime(work["date"])
            mask = (work["date"] >= pd.Timestamp(start_date)) & (work["date"] <= pd.Timestamp(end_date))
            work = work.loc[mask].copy()
            if not work.empty:
                work["date"] = work["date"].dt.strftime("%Y-%m-%d")
                return work
        return frame

    def _fetch_eastmoney_symbol(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        secid = self._eastmoney_secid(symbol)
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "klt": "101",
            "fqt": "1" if self.config.sync_adjust == "qfq" else "0",
            "beg": pd.Timestamp(start_date).strftime("%Y%m%d"),
            "end": pd.Timestamp(end_date).strftime("%Y%m%d"),
        }
        response = requests.get(
            "https://push2his.eastmoney.com/api/qt/stock/kline/get",
            params=params,
            timeout=(3, 8),
        )
        response.raise_for_status()
        data = response.json().get("data") or {}
        klines = data.get("klines") or []
        if not klines:
            return pd.DataFrame()
        rows = []
        for item in klines:
            parts = str(item).split(",")
            if len(parts) < 6:
                continue
            rows.append(
                {
                    "日期": parts[0],
                    "开盘": parts[1],
                    "收盘": parts[2],
                    "最高": parts[3],
                    "最低": parts[4],
                    "成交量": parts[5],
                }
            )
        raw = pd.DataFrame(rows)
        if self.akshare._is_index_symbol(symbol):
            return normalize_index_daily(raw, symbol=symbol, name=self.akshare._index_name(symbol))
        return normalize_stock_daily(raw, symbol=symbol, name=self.akshare._load_name_map().get(symbol, ""))

    def _validate_market_pair(
        self,
        symbol: str,
        primary: pd.DataFrame,
        backup: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> tuple[pd.DataFrame, list[str]]:
        errors: list[str] = []
        if primary is None or primary.empty:
            if symbol == self.config.benchmark_symbol and backup is not None and not backup.empty:
                return backup.reset_index(drop=True), [f"benchmark_backup_only:{symbol}"]
            return pd.DataFrame(), [f"missing_primary:{symbol}"]
        if backup is None or backup.empty:
            return pd.DataFrame(), [f"missing_backup:{symbol}"]

        p = primary.copy()
        b = backup.copy()
        p["date"] = p["date"].astype(str)
        b["date"] = b["date"].astype(str)
        merged = p.merge(
            b,
            on=["date", "symbol"],
            how="inner",
            suffixes=("_primary", "_backup"),
        )
        if merged.empty:
            return pd.DataFrame(), [f"no_overlapping_dates:{symbol}"]

        bad_dates = set()
        tolerance = float(self.config.price_conflict_tolerance)
        volume_tolerance = max(0.2, tolerance * 6)
        for _, row in merged.iterrows():
            for field in ["open", "close", "high", "low"]:
                a = float(row[f"{field}_primary"])
                bval = float(row[f"{field}_backup"])
                if not self._within_relative_tolerance(a, bval, tolerance):
                    bad_dates.add(str(row["date"]))
                    errors.append(f"price_conflict:{symbol}:{row['date']}:{field}:{a}:{bval}")
                    break
            else:
                a_vol = float(row["volume_primary"])
                b_vol = float(row["volume_backup"])
                if not self._within_relative_tolerance(a_vol, b_vol, volume_tolerance):
                    bad_dates.add(str(row["date"]))
                    errors.append(f"volume_conflict:{symbol}:{row['date']}:{a_vol}:{b_vol}")

        expected_dates = set(pd.date_range(start_date, end_date, freq="D").strftime("%Y-%m-%d"))
        available_dates = set(merged["date"].astype(str))
        missing_dates = sorted(expected_dates.intersection(set(primary["date"].astype(str))) - available_dates)
        if missing_dates:
            errors.append(f"missing_backup_dates:{symbol}:{','.join(missing_dates[:5])}")
        validated = p[~p["date"].isin(bad_dates)].copy()
        validated = validated[(validated["date"] >= start_date) & (validated["date"] <= end_date)].copy()
        if missing_dates and not validated.empty:
            validated = validated[~validated["date"].isin(missing_dates)].copy()
        if validated.empty:
            return pd.DataFrame(), errors[:20]
        return validated.reset_index(drop=True), errors[:20]

    def _build_market_snapshot(self, gold_dir: Path, symbols: list[str], as_of_date: str) -> pd.DataFrame:
        rows = []
        for symbol in symbols:
            path = gold_dir / f"{symbol}.csv"
            if not path.exists():
                continue
            try:
                df = pd.read_csv(path)
            except Exception:
                continue
            if "date" not in df.columns or df.empty:
                continue
            row = df[df["date"].astype(str) == as_of_date]
            if row.empty:
                row = df.tail(1)
            last = row.iloc[-1]
            rows.append(
                {
                    "date": str(last["date"]),
                    "symbol": symbol,
                    "name": str(last["name"]) if "name" in row.columns else self.akshare._load_name_map().get(symbol, symbol),
                    "open": last.get("open"),
                    "close": last.get("close"),
                    "high": last.get("high"),
                    "low": last.get("low"),
                    "volume": last.get("volume"),
                    "factor": last.get("factor", 1.0),
                }
            )
        return pd.DataFrame(rows)

    def _collect_report_frames(self, as_of_date: str) -> list[pd.DataFrame]:
        try:
            import akshare as ak
        except ImportError:
            return []
        frames = []
        for report_period in self._candidate_report_periods(as_of_date):
            for source_name, fetcher in [
                ("eastmoney_yjbb", ak.stock_yjbb_em),
                ("eastmoney_yjyg", ak.stock_yjyg_em),
                ("eastmoney_yjkb", ak.stock_yjkb_em),
            ]:
                raw_dir = self._raw_dir("fundamentals", source_name)
                raw_dir.mkdir(parents=True, exist_ok=True)
                raw_path = raw_dir / f"{report_period}.csv"
                try:
                    frame = fetcher(date=report_period)
                    if frame is None or frame.empty:
                        raise RuntimeError("empty")
                    frame.to_csv(raw_path, index=False, encoding="utf-8-sig")
                except Exception:
                    if raw_path.exists():
                        frame = pd.read_csv(raw_path)
                    else:
                        continue
                if frame is None or frame.empty:
                    continue
                normalized = frame.copy()
                normalized["report_period"] = report_period
                normalized["report_source"] = source_name
                frames.append(normalized)
        return frames

    def _build_latest_report_lookup(self, frames: list[pd.DataFrame]) -> dict[str, dict[str, Any]]:
        if not frames:
            return {}
        combined = pd.concat(frames, ignore_index=True, sort=False)
        code_col = next((col for col in ["股票代码", "代码", "证券代码"] if col in combined.columns), None)
        name_col = next((col for col in ["股票简称", "简称", "证券简称"] if col in combined.columns), None)
        if code_col is None:
            return {}
        combined[code_col] = combined[code_col].astype(str).str.zfill(6)
        combined["instrument"] = combined[code_col].map(self._code_to_instrument)
        source_priority = {name: idx for idx, name in enumerate(self.config.source_priority.get("fundamentals", []))}
        combined["source_rank"] = combined["report_source"].map(lambda item: source_priority.get(str(item), 99))
        combined = combined.sort_values(["report_period", "source_rank"], ascending=[False, True])
        latest = combined.drop_duplicates(subset=["instrument"], keep="first")
        lookup = {}
        for _, row in latest.iterrows():
            record = row.to_dict()
            record["name"] = str(row.get(name_col, "")).strip() if name_col else ""
            lookup[str(row["instrument"])] = record
        return lookup

    def _fetch_individual_info(self, instrument: str, raw_dir: Path) -> dict[str, str]:
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_path = raw_dir / f"{instrument}.csv"
        try:
            import akshare as ak

            df = ak.stock_individual_info_em(symbol=instrument[2:], timeout=5)
            if df is None or df.empty:
                raise RuntimeError("empty")
            df.to_csv(raw_path, index=False, encoding="utf-8-sig")
        except Exception:
            if raw_path.exists():
                df = pd.read_csv(raw_path)
            else:
                return {}
        if df is None or df.empty or not {"item", "value"}.issubset(df.columns):
            return {}
        mapping = {str(row["item"]).strip(): str(row["value"]).strip() for _, row in df.iterrows()}
        try:
            import akshare as ak

            intro_df = ak.stock_zyjs_ths(symbol=instrument[2:])
            if intro_df is not None and not intro_df.empty:
                mapping["公司简介"] = self._compact_intro(intro_df)
        except Exception:
            pass
        return mapping

    def _build_fundamental_row(
        self,
        instrument: str,
        as_of_date: str,
        info: dict[str, str],
        report: dict[str, Any],
    ) -> dict[str, Any]:
        revenue_yoy = self._to_float(self._pick(report, ["营业收入-同比增长", "营业收入同比增长", "营收同比", "营业收入同比"]))
        profit_yoy = self._to_float(self._pick(report, ["净利润-同比增长", "净利润同比增长", "净利润同比", "归母净利润同比增长"]))
        roe = self._to_float(self._pick(report, ["净资产收益率", "ROE", "加权净资产收益率"]))
        eps = self._to_float(self._pick(report, ["每股收益", "基本每股收益"]))
        gross_margin = self._to_float(self._pick(report, ["销售毛利率", "毛利率"]))
        name = str(info.get("股票简称") or report.get("name") or self.akshare._load_name_map().get(instrument, instrument))
        industry = str(info.get("行业") or "行业待补充")
        latest_price = self._to_float(info.get("最新"))
        if latest_price is None:
            latest_price = self._sync_latest_close(instrument=instrument, as_of_date=as_of_date)
        total_mv = self._to_float(info.get("总市值"))
        float_mv = self._to_float(info.get("流通市值"))
        report_period = str(report.get("report_period") or "")
        report_source = str(report.get("report_source") or "")
        fundamental_risk_tag = self._fundamental_risk_tag(revenue_yoy, profit_yoy, roe)
        valuation_tag = self._valuation_tag(latest_price=latest_price, eps=eps, roe=roe, gross_margin=gross_margin)
        summary = self._fundamental_summary(
            report_period=report_period,
            revenue_yoy=revenue_yoy,
            profit_yoy=profit_yoy,
            roe=roe,
            gross_margin=gross_margin,
            eps=eps,
            latest_price=latest_price,
        )
        return {
            "instrument": instrument,
            "as_of_date": as_of_date,
            "name": name,
            "industry": industry,
            "latest_price": latest_price,
            "total_market_value": total_mv,
            "float_market_value": float_mv,
            "report_period": report_period,
            "report_source": report_source,
            "revenue_yoy": revenue_yoy,
            "profit_yoy": profit_yoy,
            "roe": roe,
            "eps": eps,
            "gross_margin": gross_margin,
            "fundamental_risk_tag": fundamental_risk_tag,
            "valuation_tag": valuation_tag,
            "fundamental_summary": summary,
            "company_intro": info.get("公司简介", ""),
        }

    def _fetch_notice_window(self, as_of_date: str, lookback_days: int, raw_dir: Path) -> pd.DataFrame:
        try:
            import akshare as ak
        except ImportError:
            return pd.DataFrame()
        frames = []
        raw_dir.mkdir(parents=True, exist_ok=True)
        for offset in range(max(lookback_days, 1)):
            date = (pd.Timestamp(as_of_date) - pd.Timedelta(days=offset)).strftime("%Y%m%d")
            raw_path = raw_dir / f"{date}.csv"
            try:
                df = ak.stock_notice_report(symbol="全部", date=date)
                if df is None or df.empty:
                    raise RuntimeError("empty")
                df.to_csv(raw_path, index=False, encoding="utf-8-sig")
            except Exception:
                if raw_path.exists():
                    df = pd.read_csv(raw_path)
                else:
                    continue
            if df is None or df.empty:
                continue
            frames.append(df.assign(fetch_date=date))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _fetch_news_for_symbol(
        self,
        instrument: str,
        as_of_date: str,
        lookback_days: int,
        raw_dir: Path,
    ) -> tuple[pd.DataFrame, bool]:
        try:
            import akshare as ak
        except ImportError:
            return pd.DataFrame(), False
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_path = raw_dir / f"{instrument}.csv"
        ok = False
        try:
            df = ak.stock_news_em(symbol=instrument[2:])
            if df is None:
                df = pd.DataFrame()
            df.to_csv(raw_path, index=False, encoding="utf-8-sig")
            ok = True
        except Exception:
            if raw_path.exists():
                df = pd.read_csv(raw_path)
                ok = True
            else:
                return pd.DataFrame(), False
        if df.empty:
            return pd.DataFrame(), ok
        time_col = next((col for col in ["发布时间", "时间", "日期"] if col in df.columns), None)
        title_col = next((col for col in ["新闻标题", "标题", "资讯标题"] if col in df.columns), None)
        source_col = next((col for col in ["文章来源", "来源", "媒体名称"] if col in df.columns), None)
        if time_col is None or title_col is None:
            return pd.DataFrame(), ok
        work = df.copy()
        work["published_at"] = pd.to_datetime(work[time_col], errors="coerce")
        window_start = pd.Timestamp(as_of_date) - pd.Timedelta(days=max(lookback_days - 1, 0))
        work = work[work["published_at"].notna()]
        work = work[(work["published_at"] >= window_start) & (work["published_at"] <= pd.Timestamp(as_of_date) + pd.Timedelta(days=1))]
        if work.empty:
            return pd.DataFrame(), ok
        work["instrument"] = instrument
        work["title"] = work[title_col].astype(str)
        work["source_name"] = work[source_col].astype(str) if source_col else "东方财富"
        keep_cols = ["instrument", "published_at", "title", "source_name"]
        return work[keep_cols].copy(), ok

    def _summarize_notice_events(self, df: pd.DataFrame, symbols: list[str]) -> dict[str, dict[str, Any]]:
        if df.empty:
            return {}
        code_col = next((col for col in ["代码", "股票代码", "证券代码"] if col in df.columns), None)
        title_col = next((col for col in ["公告标题", "标题", "公告名称"] if col in df.columns), None)
        if code_col is None or title_col is None:
            return {}
        work = df.copy()
        work[code_col] = work[code_col].astype(str).str.zfill(6)
        work["instrument"] = work[code_col].map(self._code_to_instrument)
        work = work[work["instrument"].isin(symbols)]
        summary = {}
        for instrument, group in work.groupby("instrument"):
            titles = group[title_col].astype(str).tolist()
            tags = self._extract_event_tags(titles)
            summary[instrument] = {
                "notice_count_3d": len(group),
                "notice_tags": "、".join(tags),
                "notice_summary": self._build_notice_summary(tags=tags, titles=titles),
            }
        return summary

    def _summarize_news_events(self, df: pd.DataFrame, symbols: list[str]) -> dict[str, dict[str, Any]]:
        if df.empty:
            return {}
        summary = {}
        for instrument, group in df.groupby("instrument"):
            if instrument not in symbols:
                continue
            titles = group["title"].astype(str).tolist()
            positive = sum(self._news_score(title) > 0 for title in titles)
            negative = sum(self._news_score(title) < 0 for title in titles)
            if negative > positive:
                sentiment = "偏负面"
            elif positive > negative:
                sentiment = "偏正面"
            else:
                sentiment = "中性"
            risk_tags = self._extract_event_tags(titles)
            summary[instrument] = {
                "news_count_3d": len(group),
                "news_sentiment": sentiment,
                "news_risk_tags": "、".join(risk_tags),
                "news_summary": self._build_news_summary(sentiment=sentiment, risk_tags=risk_tags, titles=titles),
            }
        return summary

    def _merge_event_risk(self, notice: dict[str, Any], news: dict[str, Any]) -> str:
        tags = " ".join([str(notice.get("notice_tags", "")), str(news.get("news_risk_tags", ""))])
        negative = any(keyword in tags for keyword in ["减持", "处罚", "诉讼", "问询", "解禁", "亏损", "下滑", "预减", "违约"])
        positive = any(keyword in tags for keyword in ["回购", "中标", "增持", "签约", "分红", "预增", "增长", "新高"])
        if negative and positive:
            return "公告多空交织"
        if negative:
            return "公告偏利空"
        if positive:
            return "公告偏利多"
        return "事件中性"

    def _sync_latest_close(self, instrument: str, as_of_date: str) -> float | None:
        csv_path = Path(self.config.sync_dir).expanduser() / "akshare_daily" / f"{instrument}.csv"
        if not csv_path.exists():
            return None
        try:
            frame = pd.read_csv(csv_path)
        except Exception:
            return None
        if frame.empty or "date" not in frame.columns or "close" not in frame.columns:
            return None
        matched = frame[frame["date"].astype(str) == as_of_date]
        row = matched.iloc[-1] if not matched.empty else frame.iloc[-1]
        return self._to_float(row.get("close"))

    def _candidate_report_periods(self, as_of_date: str, count: int = 6) -> list[str]:
        base = pd.Timestamp(as_of_date)
        periods = []
        cursor = pd.Timestamp(base.year, ((base.month - 1) // 3 + 1) * 3, 1) + pd.offsets.MonthEnd(0)
        for _ in range(count):
            periods.append(cursor.strftime("%Y%m%d"))
            cursor = (cursor - pd.offsets.QuarterEnd(startingMonth=12))
        return periods

    def _manifest_dir(self, as_of_date: str) -> Path:
        path = Path(self.config.sync_dir).expanduser() / "manifests" / as_of_date
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _limit_equity_symbols(symbols: list[str], limit: int | None = None) -> list[str]:
        symbols = sorted(dict.fromkeys(symbols))
        if limit is not None:
            return symbols[:limit]
        return symbols

    def _manifest_path(self, feed_type: str, as_of_date: str) -> Path:
        return self._manifest_dir(as_of_date) / f"{feed_type}.json"

    def _save_manifest(self, manifest: FeedManifest) -> Path:
        path = self._manifest_path(manifest.feed_type, manifest.as_of_date)
        path.write_text(json.dumps(asdict(manifest), ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def _load_manifest(self, path: Path) -> FeedManifest:
        data = json.loads(path.read_text(encoding="utf-8"))
        return FeedManifest(**data)

    def _raw_dir(self, feed_type: str, source_name: str) -> Path:
        path = Path(self.config.sync_dir).expanduser() / "raw" / feed_type / source_name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _gold_dir(self, feed_type: str) -> Path:
        path = Path(self.config.sync_dir).expanduser() / "gold" / feed_type
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _after_market_close(self, as_of_date: str) -> bool:
        cutoff = self._cutoff_time(self.config.market_close_cutoff)
        if pd.Timestamp(as_of_date).date() != pd.Timestamp.today().date():
            return True
        return datetime.now().time() >= cutoff

    @staticmethod
    def _cutoff_time(value: str) -> time:
        hour, minute = value.split(":")
        return time(int(hour), int(minute))

    @staticmethod
    def _within_relative_tolerance(a: float, b: float, tolerance: float) -> bool:
        denominator = max(abs(a), abs(b), 1.0)
        return abs(a - b) / denominator <= tolerance

    @staticmethod
    def _eastmoney_secid(symbol: str) -> str:
        if symbol.startswith("SH"):
            return f"1.{symbol[2:]}"
        if symbol.startswith("SZ"):
            return f"0.{symbol[2:]}"
        if symbol.startswith("BJ"):
            return f"0.{symbol[2:]}"
        return symbol

    @staticmethod
    def _code_to_instrument(code: str) -> str:
        code = str(code).strip().zfill(6)
        if code.startswith(("5", "6", "9")):
            return f"SH{code}"
        if code.startswith(("4", "8")):
            return f"BJ{code}"
        return f"SZ{code}"

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value in {None, "", "-", "--"}:
            return None
        try:
            if pd.isna(value):
                return None
            text = str(value).replace("%", "").replace(",", "").strip()
            if not text:
                return None
            result = float(text)
            if pd.isna(result):
                return None
            return result
        except Exception:
            return None

    @staticmethod
    def _pick(record: dict[str, Any], candidates: Iterable[str]) -> Any:
        for key in candidates:
            if key in record and record[key] not in {None, "", "-", "--"}:
                return record[key]
        return None

    @staticmethod
    def _compact_intro(df: pd.DataFrame) -> str:
        if df is None or df.empty:
            return ""
        values = []
        for col in df.columns:
            series = df[col].dropna().astype(str).str.strip()
            series = series[series != ""]
            if not series.empty:
                values.append(series.iloc[0])
        return "；".join(values[:3])

    @staticmethod
    def _fundamental_risk_tag(revenue_yoy: float | None, profit_yoy: float | None, roe: float | None) -> str:
        tags: list[str] = []
        if profit_yoy is not None and profit_yoy < -20:
            tags.append("利润明显承压")
        elif profit_yoy is not None and profit_yoy < 0:
            tags.append("利润承压")
        if revenue_yoy is not None and revenue_yoy < -10:
            tags.append("营收下滑")
        elif revenue_yoy is not None and revenue_yoy >= 15:
            tags.append("营收增长较快")
        if profit_yoy is not None and profit_yoy >= 20:
            tags.append("利润增长较快")
        if revenue_yoy is not None and revenue_yoy > 0 and profit_yoy is not None and profit_yoy < 0:
            tags.append("增收不增利")
        if roe is not None and roe < 5:
            tags.append("回报偏弱")
        elif roe is not None and roe >= 12:
            tags.append("盈利质量较好")
        if revenue_yoy is None and profit_yoy is None:
            return "财报信息有限"
        if not tags:
            return "基本面中性"
        return "、".join(tags[:2])

    @staticmethod
    def _valuation_tag(
        latest_price: float | None,
        eps: float | None,
        roe: float | None,
        gross_margin: float | None,
    ) -> str:
        tags: list[str] = []
        if latest_price is not None and eps is not None and eps > 0:
            pe = latest_price / eps
            if pe < 12:
                tags.append("估值偏低")
            elif pe > 35:
                tags.append("估值偏高")
            else:
                tags.append("估值中性")
        elif eps is not None and eps <= 0:
            tags.append("盈利支撑较弱")

        if roe is not None and roe >= 15 and "估值偏高" not in tags:
            tags.append("质地较好")
        elif gross_margin is not None and gross_margin < 10:
            tags.append("利润空间偏薄")

        if not tags:
            return "估值信息有限"
        return "、".join(tags[:2])

    @staticmethod
    def _fundamental_summary(
        report_period: str,
        revenue_yoy: float | None,
        profit_yoy: float | None,
        roe: float | None,
        gross_margin: float | None,
        eps: float | None,
        latest_price: float | None,
    ) -> str:
        parts = []
        if report_period:
            parts.append(f"报告期 {report_period}")
        if revenue_yoy is not None:
            parts.append(f"营收同比 {revenue_yoy:.2f}%")
        if profit_yoy is not None:
            parts.append(f"利润同比 {profit_yoy:.2f}%")
        if roe is not None:
            parts.append(f"ROE {roe:.2f}%")
        if gross_margin is not None:
            parts.append(f"毛利率 {gross_margin:.2f}%")
        if eps is not None:
            parts.append(f"每股收益 {eps:.4g}")
        if latest_price is not None and eps is not None and eps > 0:
            parts.append(f"估算PE {latest_price / eps:.2f}")
        return "；".join(parts) if parts else "暂无有效财报摘要"

    @staticmethod
    def _build_notice_summary(tags: list[str], titles: list[str]) -> str:
        parts: list[str] = []
        if tags:
            parts.append(f"公告标签 {('、'.join(tags[:4]))}")
        if titles:
            parts.append("；".join(str(title) for title in titles[:3]))
        return "；".join(parts) if parts else "近三日无重点公告"

    @staticmethod
    def _build_news_summary(sentiment: str, risk_tags: list[str], titles: list[str]) -> str:
        parts = [f"情绪 {sentiment}"]
        if risk_tags:
            parts.append(f"关键词 {('、'.join(risk_tags[:4]))}")
        if titles:
            parts.append("；".join(str(title) for title in titles[:3]))
        return "；".join(parts) if parts else "近三日无重点新闻"

    @staticmethod
    def _extract_event_tags(titles: Iterable[str]) -> list[str]:
        keywords = {
            "回购": "回购",
            "增持": "增持",
            "减持": "减持",
            "解禁": "解禁",
            "问询": "问询",
            "处罚": "处罚",
            "诉讼": "诉讼",
            "回函": "问询回复",
            "中标": "中标",
            "合同": "重大合同",
            "分红": "分红",
            "亏损": "亏损",
            "预增": "预增",
            "预减": "预减",
        }
        tags: list[str] = []
        for title in titles:
            for keyword, tag in keywords.items():
                if keyword in str(title) and tag not in tags:
                    tags.append(tag)
        return tags[:6]

    @staticmethod
    def _news_score(title: str) -> int:
        positive = ["回购", "增持", "中标", "签约", "增长", "新高", "突破", "分红", "预增", "订单", "回暖", "扩产"]
        negative = ["减持", "处罚", "问询", "诉讼", "下滑", "亏损", "暴跌", "解禁", "违约", "风险", "下修", "预减"]
        if any(key in title for key in negative):
            return -1
        if any(key in title for key in positive):
            return 1
        return 0

    @staticmethod
    def _now_iso() -> str:
        return datetime.now().isoformat(timespec="seconds")

    @staticmethod
    def _age_hours(value: str) -> float:
        return (datetime.now() - datetime.fromisoformat(value)).total_seconds() / 3600
