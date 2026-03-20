from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
import re
from datetime import datetime

import numpy as np
import pandas as pd

from .backup_utils import BackupManager
from .config import AppConfig
from .qlib_env import ensure_qlib, latest_local_data_date, mlruns_path


PARAMS_FILE = "params.pkl"
PRED_FILE = "pred.pkl"
SIG_ANALYSIS_DIR = "sig_analysis"
DEFAULT_EXP_NAME = "Default"
TOP_NUM_LIST = [10, 20, 30, 50, 80, 100]


@dataclass
class ModelContext:
    exp_name: str
    rid: List[str] = field(default_factory=list)


class ModelCLI:
    """Minimal model analysis layer for saved qlib recorders."""

    def __init__(self, config: AppConfig):
        self.config = config
        self._qlib_ready = False
        self.rid_rank_icir: Dict[str, float] = {}
        self.rid_weight: Dict[str, float] = {}

    def list_models(self, include_recorders: bool = False) -> dict[str, object]:
        R = self._get_R()
        model_list = self.get_model_list()
        items = []
        for mc in model_list:
            exp = R.get_exp(experiment_name=mc.exp_name)
            item = {
                "experiment_name": mc.exp_name,
                "experiment_id": exp.id,
                "recorder_count": len(mc.rid),
            }
            if include_recorders:
                item["recorders"] = [self.recorder_summary(exp.get_recorder(recorder_id=rid)) for rid in mc.rid]
            items.append(item)
        return {"count": len(items), "items": items}

    def get_model_list(self) -> List[ModelContext]:
        R = self._get_R()
        patterns = self.config.model_filter or [".*"]
        ret: List[ModelContext] = []
        self.rid_rank_icir = {}

        for name in R.list_experiments():
            if name == DEFAULT_EXP_NAME or not self._matches_any(name, patterns):
                continue
            exp = R.get_exp(experiment_name=name)
            mc = ModelContext(name)
            for rid in exp.list_recorders():
                rec = exp.get_recorder(recorder_id=rid)
                if self._is_valid_recorder(rec) and self.filter_rec(rec):
                    mc.rid.append(rid)
                    _, ic_list = self.get_ic_info(rec)
                    self.rid_rank_icir[rid] = float(np.around(ic_list[3], 6))
            if mc.rid:
                ret.append(mc)

        self._assign_weights(ret)
        return ret

    def filter_rec(self, rec) -> bool:
        thresholds = self.config.rec_filter or []
        if not thresholds:
            return True
        _, ic_list = self.get_ic_info(rec)
        return all(val > list(rule.values())[0] for val, rule in zip(ic_list, thresholds))

    def get_ic_info(self, rec) -> tuple[dict[str, float], list[float]]:
        ic_pkl = rec.load_object(f"{SIG_ANALYSIS_DIR}/ic.pkl")
        ric_pkl = rec.load_object(f"{SIG_ANALYSIS_DIR}/ric.pkl")
        ic = float(ic_pkl.mean())
        rank_ic = float(ric_pkl.mean())
        icir = ic / float(ic_pkl.std()) if float(ic_pkl.std()) != 0 else 0.0
        rank_icir = rank_ic / float(ric_pkl.std()) if float(ric_pkl.std()) != 0 else 0.0
        info = {
            "IC": float(np.around(ic, 3)),
            "ICIR": float(np.around(icir, 3)),
            "Rank IC": float(np.around(rank_ic, 3)),
            "Rank ICIR": float(np.around(rank_icir, 3)),
        }
        return info, [ic, icir, rank_ic, rank_icir]

    def recorder_summary(self, rec) -> dict[str, object]:
        task = rec.load_object("task")
        ic_info, _ = self.get_ic_info(rec)
        return {
            "recorder_id": rec.id,
            "model": task["model"]["class"],
            "dataset": task["dataset"]["kwargs"]["handler"]["class"],
            "segments": task["dataset"]["kwargs"]["segments"],
            "ic_info": ic_info,
            "weight": self.rid_weight.get(rec.id, 0.0),
        }

    def top_predictions(self, limit: int = 20, date: str | None = None) -> pd.DataFrame:
        R = self._get_R()
        rows = []
        model_list = self.get_model_list()
        for mc in model_list:
            exp = R.get_exp(experiment_name=mc.exp_name)
            for rid in mc.rid:
                rec = exp.get_recorder(recorder_id=rid)
                pred = rec.load_object(PRED_FILE).reset_index()
                pred["rid"] = rid
                pred["exp_name"] = mc.exp_name
                pred["weight"] = self.rid_weight.get(rid, 0.0)
                rows.append(pred)

        if not rows:
            return pd.DataFrame(columns=["instrument", "datetime", "avg_score", "pos_ratio", "model_count"])

        df = pd.concat(rows, ignore_index=True)
        df["datetime"] = pd.to_datetime(df["datetime"])
        target_date = pd.Timestamp(date) if date else df["datetime"].max()
        daily = df[df["datetime"] == target_date].copy()
        if daily.empty:
            return pd.DataFrame(columns=["instrument", "datetime", "avg_score", "pos_ratio", "model_count"])

        daily["weighted_score"] = daily["score"] * daily["weight"]
        agg = (
            daily.groupby(["datetime", "instrument"], as_index=False)
            .agg(
                weighted_score_sum=("weighted_score", "sum"),
                weight_sum=("weight", "sum"),
                score_mean=("score", "mean"),
                pos_ratio=("score", lambda s: float((s > 0).mean())),
                model_count=("score", "size"),
            )
        )
        agg["avg_score"] = np.where(
            agg["weight_sum"] != 0,
            agg["weighted_score_sum"] / agg["weight_sum"],
            agg["score_mean"],
        )
        agg = (
            agg[["datetime", "instrument", "avg_score", "pos_ratio", "model_count"]]
            .sort_values(by="avg_score", ascending=False)
            .head(limit)
        )
        return agg

    def save_top_predictions(self, limit: int = 20, date: str | None = None) -> Path:
        result = self.top_predictions(limit=limit, date=date)
        output_dir = Path(self.config.analysis_folder).expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)
        target_date = date or (
            result["datetime"].iloc[0].strftime("%Y-%m-%d") if not result.empty else "unknown"
        )
        output = output_dir / f"top_predictions_{target_date}.csv"
        result.to_csv(output, index=False, encoding="utf-8-sig")
        return output

    def list_backups(self) -> dict[str, object]:
        manager = self._backup_manager()
        items = [{"path": str(info.path), "size_bytes": info.size_bytes} for info in manager.list_backups()]
        return {"count": len(items), "items": items}

    def backup_mlruns(self) -> Path:
        stamp = self._local_data_date()
        return self._backup_manager().backup(stamp=stamp)

    def restore_mlruns(self, archive_name: str | None = None, restore_all: bool = False) -> list[Path]:
        return self._backup_manager().restore(archive_name=archive_name, restore_all=restore_all)

    def selection_report(self, output_dir: str | None = None) -> Path:
        merged = self.collect_predictions()
        if merged.empty:
            raise RuntimeError("No saved predictions available to build a report")

        target_dir = (
            Path(output_dir).expanduser()
            if output_dir
            else Path(self.config.analysis_folder).expanduser()
            / f"selection_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        target_dir.mkdir(parents=True, exist_ok=True)

        merged.to_csv(target_dir / "total.csv", index=False, encoding="utf-8-sig")

        for dt_value, group in merged.groupby("datetime"):
            day_str = pd.Timestamp(dt_value).strftime("%Y-%m-%d")
            ret_df = (
                group.sort_values(by="avg_score", ascending=False)
                .reset_index(drop=True)
            )
            ret_df.to_csv(target_dir / f"{day_str}_ret.csv", index=False, encoding="utf-8-sig")
            filter_df = self.filter_ret_df(ret_df).reset_index(drop=True)
            filter_df.to_csv(
                target_dir / f"{day_str}_filter_ret.csv",
                index=False,
                encoding="utf-8-sig",
            )

        return target_dir

    def latest_selection_dir(self) -> Path:
        base = Path(self.config.analysis_folder).expanduser()
        candidates = [p for p in base.glob("selection_*") if p.is_dir()]
        if not candidates:
            raise RuntimeError(f"No selection_* directories found under {base}")
        return sorted(candidates)[-1]

    def review_report(self, selection_dir: str | None = None) -> Path:
        self._ensure_qlib()
        base_dir = Path(selection_dir).expanduser() if selection_dir else self.latest_selection_dir()
        date_list = self._selection_dates(base_dir)
        if not date_list:
            raise RuntimeError(f"No *_ret.csv files found in {base_dir}")

        out_dir = base_dir / "review"
        out_dir.mkdir(parents=True, exist_ok=True)
        market_cache = self._build_review_market_cache(date_list)

        for mode in ["ret", "filter_ret"]:
            all_daily = []
            for date_str in date_list:
                review_df = self._review_one_day(base_dir, date_str, mode, market_cache=market_cache)
                if review_df is not None:
                    review_df.to_csv(out_dir / f"{date_str}_{mode}_review.csv", index=False, encoding="utf-8-sig")
                    all_daily.append(review_df.assign(date=date_str))
            if all_daily:
                pd.concat(all_daily, ignore_index=True).to_csv(
                    out_dir / f"summary_{mode}.csv", index=False, encoding="utf-8-sig"
                )
        return out_dir

    def backtest_report(self, selection_dir: str | None = None) -> Path:
        self._ensure_qlib()
        base_dir = Path(selection_dir).expanduser() if selection_dir else self.latest_selection_dir()
        date_list = self._selection_dates(base_dir)
        if not date_list:
            raise RuntimeError(f"No *_ret.csv files found in {base_dir}")

        out_dir = base_dir / "backtest"
        out_dir.mkdir(parents=True, exist_ok=True)

        csi300_df = self._get_csi300_label_frame(date_list[0], date_list[-1])
        for mode in ["ret", "filter_ret"]:
            all_frames = {}
            for top_num in TOP_NUM_LIST:
                frame = self._backtest_topk(base_dir, date_list, mode, top_num, csi300_df)
                frame.to_csv(out_dir / f"top{top_num}_{mode}.csv", index=False, encoding="utf-8-sig")
                all_frames[top_num] = frame
            summary = self._backtest_summary(all_frames)
            summary.to_csv(out_dir / f"summary_{mode}.csv", index=False, encoding="utf-8-sig")
        return out_dir

    def collect_predictions(self) -> pd.DataFrame:
        self._ensure_qlib()
        from qlib.workflow import R

        frames = []
        model_list = self.get_model_list()
        for mc in model_list:
            exp = R.get_exp(experiment_name=mc.exp_name)
            for rid in mc.rid:
                rec = exp.get_recorder(recorder_id=rid)
                pred = rec.load_object(PRED_FILE).reset_index()
                pred["rid"] = rid
                pred["exp_name"] = mc.exp_name
                pred["weight"] = self.rid_weight.get(rid, 0.0)
                frames.append(pred)

        if not frames:
            return pd.DataFrame()

        raw = pd.concat(frames, ignore_index=True)
        raw["datetime"] = pd.to_datetime(raw["datetime"])
        aggregated = self._aggregate_predictions(raw)
        labels = self._get_real_label_frame(
            start_date=aggregated["datetime"].min().strftime("%Y-%m-%d"),
            end_date=aggregated["datetime"].max().strftime("%Y-%m-%d"),
        )
        market = self._get_market_feature_frame(
            start_date=aggregated["datetime"].min().strftime("%Y-%m-%d"),
            end_date=aggregated["datetime"].max().strftime("%Y-%m-%d"),
        )

        merged = aggregated.merge(labels, on=["datetime", "instrument"], how="left")
        merged["error"] = merged["avg_score"] - merged["real_label"]
        merged["abs_error"] = merged["error"].abs()
        merged = merged.merge(market, on=["datetime", "instrument"], how="left")
        return merged.sort_values(by=["datetime", "avg_score"], ascending=[True, False]).reset_index(drop=True)

    def _assign_weights(self, model_list: List[ModelContext]) -> None:
        all_rids = [rid for mc in model_list for rid in mc.rid]
        total = sum(max(self.rid_rank_icir.get(rid, 0.0), 0.0) for rid in all_rids)
        self.rid_weight = {}
        for rid in all_rids:
            score = max(self.rid_rank_icir.get(rid, 0.0), 0.0)
            if total > 0:
                self.rid_weight[rid] = float(np.around(score / total, 6))
            else:
                self.rid_weight[rid] = float(np.around(1.0 / len(all_rids), 6)) if all_rids else 0.0

    def _is_valid_recorder(self, recorder) -> bool:
        artifacts = recorder.list_artifacts()
        required = {PARAMS_FILE, PRED_FILE, SIG_ANALYSIS_DIR}
        return bool(artifacts) and required.issubset(set(artifacts))

    def _aggregate_predictions(self, raw: pd.DataFrame) -> pd.DataFrame:
        work = raw.copy()
        work["weighted_score"] = work["score"] * work["weight"]
        agg = (
            work.groupby(["datetime", "instrument"], as_index=False)
            .agg(
                weighted_score_sum=("weighted_score", "sum"),
                weight_sum=("weight", "sum"),
                score_mean=("score", "mean"),
                pos_ratio=("score", lambda s: float((s > 0).mean())),
                model_count=("score", "size"),
            )
        )
        agg["avg_score"] = np.where(
            agg["weight_sum"] != 0,
            agg["weighted_score_sum"] / agg["weight_sum"],
            agg["score_mean"],
        )
        return agg[["datetime", "instrument", "avg_score", "pos_ratio", "model_count"]]

    def _get_real_label_frame(self, start_date: str, end_date: str, instruments: str = "csi300") -> pd.DataFrame:
        from qlib.data import D

        df = D.features(
            D.instruments(instruments),
            ["Ref($close, -2)/Ref($close, -1) - 1"],
            start_time=start_date,
            end_time=end_date,
            freq="day",
        )
        df.columns = ["real_label"]
        return df.reset_index()

    def _get_market_feature_frame(self, start_date: str, end_date: str, instruments: str = "csi300") -> pd.DataFrame:
        from qlib.data import D

        history_start = (pd.Timestamp(start_date) - pd.Timedelta(days=120)).strftime("%Y-%m-%d")
        close_df = D.features(
            D.instruments(instruments),
            ["$close * $factor"],
            start_time=history_start,
            end_time=end_date,
            freq="day",
        )
        close_df.columns = ["adj_close"]
        close_df = close_df.reset_index().sort_values(by=["instrument", "datetime"])

        close_df["ret1"] = close_df.groupby("instrument")["adj_close"].pct_change(fill_method=None)
        for window in [5, 20, 60]:
            close_df[f"STD{window}"] = (
                close_df.groupby("instrument")["ret1"].transform(lambda s: s.rolling(window).std())
            )
        for window in [10, 20, 60]:
            close_df[f"ROC{window}"] = (
                close_df.groupby("instrument")["adj_close"].transform(lambda s: s / s.shift(window))
            )

        snapshot = close_df[
            (close_df["datetime"] >= pd.Timestamp(start_date)) & (close_df["datetime"] <= pd.Timestamp(end_date))
        ][["datetime", "instrument", "STD5", "STD20", "STD60", "ROC10", "ROC20", "ROC60"]]
        return snapshot

    def _get_original_data_frame(self, start_date: str, end_date: str, instruments: str = "csi300") -> pd.DataFrame:
        from qlib.data import D

        df = D.features(
            D.instruments(instruments),
            ["$close * $factor", "$open * $factor", "$high * $factor", "$low * $factor"],
            start_time=start_date,
            end_time=end_date,
            freq="day",
        )
        df.columns = ["close", "open", "high", "low"]
        return df.reset_index()

    def _get_csi300_label_frame(self, start_date: str, end_date: str) -> pd.DataFrame:
        from qlib.data import D

        df = D.features(
            ["SH000300"],
            ["Ref($close, -2)/Ref($close, -1) - 1"],
            start_time=start_date,
            end_time=end_date,
            freq="day",
        )
        df.columns = ["csi300_real_label"]
        return df.reset_index()

    def _trade_dates(self) -> List[str]:
        day_file = Path(self.config.provider_uri).expanduser() / "calendars" / "day.txt"
        return day_file.read_text(encoding="utf-8").splitlines()

    def _next_trade_date(self, date_str: str, offset: int) -> str | None:
        dates = self._trade_dates()
        try:
            idx = dates.index(date_str)
        except ValueError:
            return None
        next_idx = idx + offset
        if next_idx >= len(dates):
            return None
        return dates[next_idx]

    def _selection_dates(self, base_dir: Path) -> List[str]:
        files = sorted(base_dir.glob("*_ret.csv"))
        date_list = []
        for file in files:
            if file.name.endswith("_filter_ret.csv"):
                continue
            date_list.append(file.name.replace("_ret.csv", ""))
        return date_list

    def _build_review_market_cache(self, date_list: List[str]) -> dict[str, pd.Series]:
        next_dates = []
        for date_str in date_list:
            n1 = self._next_trade_date(date_str, 1)
            n2 = self._next_trade_date(date_str, 2)
            if n1:
                next_dates.append(n1)
            if n2:
                next_dates.append(n2)
        if not next_dates:
            return {"close": pd.Series(dtype=float), "high": pd.Series(dtype=float)}

        market_df = self._get_original_data_frame(min(next_dates), max(next_dates))
        market_df["datetime"] = pd.to_datetime(market_df["datetime"]).dt.strftime("%Y-%m-%d")
        return {
            "close": market_df.set_index(["datetime", "instrument"])["close"],
            "high": market_df.set_index(["datetime", "instrument"])["high"],
        }

    def _review_one_day(
        self,
        base_dir: Path,
        date_str: str,
        mode: str,
        market_cache: dict[str, pd.Series],
    ) -> pd.DataFrame | None:
        file_path = base_dir / f"{date_str}_{mode}.csv"
        if not file_path.exists():
            return None

        next1 = self._next_trade_date(date_str, 1)
        next2 = self._next_trade_date(date_str, 2)
        if next1 is None or next2 is None:
            return None

        df = pd.read_csv(file_path, parse_dates=["datetime"])
        close_map = market_cache["close"]
        high_map = market_cache["high"]

        profit_num_list = [0.01 * i for i in range(1, 11)]
        rows = []
        for top_num in TOP_NUM_LIST:
            top_df = df.sort_values(by="avg_score", ascending=False).head(top_num).copy()
            if top_df.empty:
                continue
            top_df["n1close"] = top_df["instrument"].map(lambda inst: close_map.get((next1, inst), np.nan))
            top_df["n2high"] = top_df["instrument"].map(lambda inst: high_map.get((next2, inst), np.nan))
            row = {
                "date": date_str,
                "mode": mode,
                "top_num": top_num,
                "avg_real_label": top_df["real_label"].mean(),
                "positive_sign_match_ratio": float(((top_df["real_label"] * top_df["avg_score"]) > 0).mean()),
            }
            for profit_num in profit_num_list:
                hit = (
                    top_df["n2high"] > top_df["n1close"] * (1 + profit_num)
                ).mean()
                row[f"take_profit_{int(profit_num * 100)}pct"] = float(hit)
            rows.append(row)
        return pd.DataFrame(rows)

    def _backtest_topk(
        self,
        base_dir: Path,
        date_list: List[str],
        mode: str,
        top_num: int,
        csi300_df: pd.DataFrame,
    ) -> pd.DataFrame:
        rows = []
        csi300_map = csi300_df.set_index("datetime")["csi300_real_label"]

        for idx, date_str in enumerate(date_list):
            file_path = base_dir / f"{date_str}_{mode}.csv"
            df = pd.read_csv(file_path)
            top_df = df.head(top_num).copy()
            top_inst = top_df["instrument"].tolist()
            row = {
                "date": date_str,
                "avg_real_label": top_df["real_label"].mean(),
                "csi300_real_label": csi300_map.get(pd.Timestamp(date_str), np.nan),
            }
            for pos in range(top_num):
                row[f"top{pos+1}"] = top_inst[pos] if pos < len(top_inst) else None

            if idx == len(date_list) - 1:
                row["turnover_rate"] = np.nan
            else:
                next_df = pd.read_csv(base_dir / f"{date_list[idx+1]}_{mode}.csv")
                next_top = set(next_df.head(top_num)["instrument"].tolist())
                curr_top = set(top_inst)
                row["turnover_rate"] = len(curr_top - next_top) / top_num if top_num else np.nan
            rows.append(row)

        result = pd.DataFrame(rows)
        return self._calculate_daily_equity(result)

    def _calculate_daily_equity(
        self,
        df: pd.DataFrame,
        initial_cash: float = 1.0,
        fee_rate: float = 0.002,
    ) -> pd.DataFrame:
        work = df.dropna(subset=["avg_real_label"]).copy()
        work["daily_net_ret"] = work["avg_real_label"] - (work["turnover_rate"] * fee_rate)
        work["strategy_equity"] = initial_cash * (1 + work["daily_net_ret"]).cumprod()
        work["csi300_equity"] = initial_cash * (1 + work["csi300_real_label"]).cumprod()
        work["max_equity"] = work["strategy_equity"].cummax()
        work["drawdown"] = (work["strategy_equity"] - work["max_equity"]) / work["max_equity"]
        return work

    def _backtest_summary(self, backtest_frames: Dict[int, pd.DataFrame]) -> pd.DataFrame:
        rows = []
        for top_num, df in backtest_frames.items():
            if df.empty:
                continue
            rows.append(
                {
                    "top_num": top_num,
                    "final_equity": float(df["strategy_equity"].iloc[-1]),
                    "benchmark_equity": float(df["csi300_equity"].iloc[-1]),
                    "max_drawdown": float(df["drawdown"].min()),
                    "avg_daily_ret": float(df["daily_net_ret"].mean()),
                }
            )
        return pd.DataFrame(rows).sort_values(by="top_num")

    def filter_ret_df(self, df: pd.DataFrame) -> pd.DataFrame:
        needed = {"STD5", "STD20", "STD60", "ROC10", "ROC20", "ROC60"}
        if not needed.issubset(df.columns):
            return df
        filtered = df.copy()
        filtered = filtered[(filtered["STD5"] < 0.10) & (filtered["STD20"] < 0.10) & (filtered["STD60"] < 0.10)]
        filtered = filtered[(filtered["STD60"] < 0.05) & (filtered["STD5"] < 0.06)]
        filtered = filtered[filtered["STD5"] < (filtered["STD60"] * 2)]
        filtered = filtered[(filtered["ROC10"] > 0.80) & (filtered["ROC20"] > 0.80) & (filtered["ROC60"] > 0.80)]
        filtered = filtered[filtered["ROC20"] < 1.30]
        return filtered

    def _get_R(self):
        self._ensure_qlib()
        from qlib.workflow import R

        return R

    def _local_data_date(self) -> str:
        return latest_local_data_date(self.config)

    def _backup_manager(self) -> BackupManager:
        source_dir = mlruns_path(self.config)
        target_parent = source_dir.parent
        return BackupManager(
            source_dir=source_dir,
            backup_dir=self.config.backup_folder,
            target_parent=target_parent,
        )

    @staticmethod
    def _matches_any(target: str, patterns: List[str]) -> bool:
        return any(re.search(pattern, target) for pattern in patterns)

    def _ensure_qlib(self) -> None:
        if self._qlib_ready:
            return
        try:
            import qlib  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "qlib is not installed in the current environment. "
                "Use .venv/bin/pip install pyqlib first."
            ) from exc

        ensure_qlib(self.config)
        self._qlib_ready = True
