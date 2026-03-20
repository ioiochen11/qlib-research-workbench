from __future__ import annotations

from dataclasses import dataclass, field
import html
import json
from pathlib import Path
from typing import Dict, List, Optional
import re
from datetime import datetime

import numpy as np
import pandas as pd
import requests

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
        self._name_cache: Dict[str, str] = {}
        self._industry_cache: Dict[str, str] = {}
        self._sync_price_cache: Dict[str, pd.DataFrame] = {}
        self._spot_name_map: Optional[Dict[str, str]] = None

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

    def entry_plan(
        self,
        limit: int = 10,
        date: str | None = None,
        selection_dir: str | None = None,
        filtered: bool = True,
        max_price: float | None = None,
    ) -> pd.DataFrame:
        max_price = self._resolved_max_price(max_price)
        candidates = self._load_entry_candidates(
            limit=self._candidate_fetch_limit(limit=limit, max_price=max_price),
            date=date,
            selection_dir=selection_dir,
            filtered=filtered,
        )
        columns = [
            "datetime",
            "score_rank",
            "instrument",
            "name",
            "avg_score",
            "close",
            "ma5",
            "ma10",
            "buy_low",
            "buy_high",
            "breakout_price",
            "stop_loss",
            "take_profit_1",
            "take_profit_2",
            "risk_pct",
            "reward_pct_1",
            "reward_pct_2",
            "price_source",
            "setup",
            "action_plan",
            "signal_reason",
            "validation_date",
            "validation_window_end",
            "day1_open",
            "day1_high",
            "day1_low",
            "day1_close",
            "entry_zone_hit",
            "breakout_hit",
            "stop_loss_hit_2d",
            "take_profit_1_hit_2d",
            "take_profit_2_hit_2d",
            "validation_status",
            "validation_note",
        ]
        if candidates.empty:
            return pd.DataFrame(columns=columns)

        target_date = pd.Timestamp(candidates["datetime"].iloc[0]).strftime("%Y-%m-%d")
        price_history = self._get_entry_price_history(
            instruments=candidates["instrument"].tolist(),
            target_date=target_date,
        )

        rows = []
        for _, row in candidates.iterrows():
            instrument = row["instrument"]
            history = price_history[price_history["instrument"] == instrument].sort_values("datetime")
            if history.empty:
                continue
            rows.append(self._build_entry_row(row, history))
        plan = pd.DataFrame(rows, columns=columns)
        plan = self._apply_price_filter(plan, max_price=max_price)
        if not plan.empty:
            plan = plan.head(limit).reset_index(drop=True)
        return plan

    def recommendation_sheet(
        self,
        limit: int = 10,
        date: str | None = None,
        selection_dir: str | None = None,
        filtered: bool = True,
        max_price: float | None = None,
    ) -> pd.DataFrame:
        max_price = self._resolved_max_price(max_price)
        plan = self.entry_plan(
            limit=limit,
            date=date,
            selection_dir=selection_dir,
            filtered=filtered,
            max_price=max_price,
        )
        if plan.empty:
            return plan
        target_date = pd.Timestamp(plan["datetime"].iloc[0]).strftime("%Y-%m-%d")
        plan = self._attach_feed_context(plan, as_of_date=target_date)
        columns = [
            "datetime",
            "validation_date",
            "score_rank",
            "instrument",
            "name",
            "avg_score",
            "close",
            "buy_low",
            "buy_high",
            "breakout_price",
            "stop_loss",
            "take_profit_1",
            "take_profit_2",
            "action_plan",
            "signal_reason",
            "entry_zone_hit",
            "breakout_hit",
            "stop_loss_hit_2d",
            "take_profit_1_hit_2d",
            "take_profit_2_hit_2d",
            "validation_status",
            "validation_note",
            "price_source",
            "fundamental_risk_tag",
            "valuation_tag",
            "fundamental_summary",
            "event_risk_tag",
            "notice_summary",
            "news_sentiment",
            "news_summary",
            "data_as_of_date",
            "data_fetched_at",
            "data_sources",
            "data_validation_status",
            "data_gate_status",
        ]
        return plan.loc[:, columns]

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

    def save_entry_plan(
        self,
        limit: int = 10,
        date: str | None = None,
        selection_dir: str | None = None,
        filtered: bool = True,
        max_price: float | None = None,
    ) -> Path:
        max_price = self._resolved_max_price(max_price)
        plan = self.entry_plan(
            limit=limit,
            date=date,
            selection_dir=selection_dir,
            filtered=filtered,
            max_price=max_price,
        )
        output_dir = Path(self.config.analysis_folder).expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)
        target_date = date or (
            pd.Timestamp(plan["datetime"].iloc[0]).strftime("%Y-%m-%d") if not plan.empty else "unknown"
        )
        suffix = "filtered" if filtered else "raw"
        output = output_dir / f"entry_plan_{target_date}_{suffix}{self._price_suffix(max_price)}.csv"
        plan.to_csv(output, index=False, encoding="utf-8-sig")
        return output

    def save_recommendation_sheet(
        self,
        limit: int = 10,
        date: str | None = None,
        selection_dir: str | None = None,
        filtered: bool = True,
        max_price: float | None = None,
    ) -> Path:
        max_price = self._resolved_max_price(max_price)
        sheet = self.recommendation_sheet(
            limit=limit,
            date=date,
            selection_dir=selection_dir,
            filtered=filtered,
            max_price=max_price,
        )
        output_dir = Path(self.config.analysis_folder).expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)
        target_date = date or (
            pd.Timestamp(sheet["datetime"].iloc[0]).strftime("%Y-%m-%d") if not sheet.empty else "unknown"
        )
        suffix = "filtered" if filtered else "raw"
        output = output_dir / f"recommendations_{target_date}_{suffix}{self._price_suffix(max_price)}.csv"
        self._recommendation_sheet_zh(sheet).to_csv(output, index=False, encoding="utf-8-sig")
        return output

    def recommendation_report(
        self,
        limit: int = 10,
        date: str | None = None,
        selection_dir: str | None = None,
        filtered: bool = True,
        max_price: float | None = None,
    ) -> str:
        max_price = self._resolved_max_price(max_price)
        sheet = self.recommendation_sheet(
            limit=limit,
            date=date,
            selection_dir=selection_dir,
            filtered=filtered,
            max_price=max_price,
        )
        target_date = date or (
            str(sheet["datetime"].iloc[0]) if not sheet.empty else "unknown"
        )
        lines = [
            f"# 推荐验证日报（{target_date}）",
            "",
            f"- 股票池：`{self._zh_stock_pool(self.config.stock_pool)}`",
            f"- 使用筛选结果：`{'是' if filtered else '否'}`",
            f"- 行数：`{len(sheet)}`",
        ]
        if max_price is not None:
            lines.append(f"- 价格上限：`{max_price:.2f}`")
        lines.extend(["", "## 数据可信度摘要", ""])
        lines.extend(self._credibility_lines(str(target_date)))
        if sheet.empty:
            lines.extend(["", "没有符合条件的推荐结果。"])
            return "\n".join(lines) + "\n"

        status_counts = (
            sheet["validation_status"].fillna("unknown").value_counts().sort_index()
        )
        lines.extend(["", "## 验证摘要", ""])
        for status, count in status_counts.items():
            lines.append(f"- `{self._zh_validation_status(status)}`：{int(count)}")

        if "avg_score" in sheet.columns:
            lines.extend(
                [
                    "",
                    "## 分数摘要",
                    "",
                    f"- 最高平均分：`{float(sheet['avg_score'].max()):.4f}`",
                    f"- 最低平均分：`{float(sheet['avg_score'].min()):.4f}`",
                    f"- 平均平均分：`{float(sheet['avg_score'].mean()):.4f}`",
                ]
            )

        display = sheet.copy()
        display["候选股票"] = display.apply(
            lambda row: f"{row['instrument']} {row['name']}" if str(row.get("name", "")).strip() else str(row["instrument"]),
            axis=1,
        )
        display["验证状态中文"] = display["validation_status"].map(self._zh_validation_status)
        display["验证说明中文"] = display["validation_note"].map(self._zh_validation_note)
        table = display[
            [
                "score_rank",
                "候选股票",
                "avg_score",
                "close",
                "buy_low",
                "buy_high",
                "breakout_price",
                "验证状态中文",
                "验证说明中文",
            ]
        ].copy()
        table.columns = [
            "排名",
            "候选股票",
            "平均分",
            "收盘价",
            "买入下沿",
            "买入上沿",
            "突破价",
            "验证状态",
            "验证说明",
        ]
        lines.extend(["", "## 推荐明细", "", self._markdown_table(table)])
        lines.extend(
            [
                "",
                "## 结构化标签摘要",
                "",
                f"- 财报风险标签：`{self._join_unique_labels(sheet.get('fundamental_risk_tag', pd.Series(dtype=object)))}`",
                f"- 公告/事件风险标签：`{self._join_unique_labels(sheet.get('event_risk_tag', pd.Series(dtype=object)))}`",
                f"- 新闻情绪：`{self._join_unique_labels(sheet.get('news_sentiment', pd.Series(dtype=object)))}`",
            ]
        )

        first = sheet.iloc[0]
        candidate_name = str(first["name"]).strip()
        candidate_label = f"{first['instrument']} {candidate_name}".strip()
        lines.extend(
            [
                "",
                "## 第一候选股",
                "",
                f"- 股票：`{candidate_label}`",
                f"- 平均分：`{float(first['avg_score']):.4f}`",
                f"- 收盘价：`{float(first['close']):.4f}`",
                f"- 买入区间：`{float(first['buy_low']):.4f} - {float(first['buy_high']):.4f}`",
                f"- 突破价：`{float(first['breakout_price']):.4f}`",
                f"- 止损价：`{float(first['stop_loss']):.4f}`",
                f"- 止盈一：`{float(first['take_profit_1']):.4f}`",
                f"- 止盈二：`{float(first['take_profit_2']):.4f}`",
                f"- 验证状态：`{self._zh_validation_status(str(first['validation_status']))}`",
                f"- 验证说明：`{self._zh_validation_note(str(first['validation_note']))}`",
                f"- 财报摘要：`{str(first.get('fundamental_summary', '暂无有效财报摘要'))}`",
                f"- 公告摘要：`{str(first.get('notice_summary', '近三日无重点公告'))}`",
                f"- 新闻摘要：`{str(first.get('news_summary', '近三日无重点新闻'))}`",
            ]
        )
        return "\n".join(lines) + "\n"

    def save_recommendation_report(
        self,
        limit: int = 10,
        date: str | None = None,
        selection_dir: str | None = None,
        filtered: bool = True,
        max_price: float | None = None,
    ) -> Path:
        max_price = self._resolved_max_price(max_price)
        report = self.recommendation_report(
            limit=limit,
            date=date,
            selection_dir=selection_dir,
            filtered=filtered,
            max_price=max_price,
        )
        output_dir = Path(self.config.analysis_folder).expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)
        target_date = date or "unknown"
        suffix = "filtered" if filtered else "raw"
        output = output_dir / f"recommendation_report_{target_date}_{suffix}{self._price_suffix(max_price)}.md"
        output.write_text(report, encoding="utf-8")
        return output

    def recommendation_html(
        self,
        limit: int = 10,
        date: str | None = None,
        selection_dir: str | None = None,
        filtered: bool = True,
        max_price: float | None = None,
    ) -> str:
        max_price = self._resolved_max_price(max_price)
        sheet = self.recommendation_sheet(
            limit=limit,
            date=date,
            selection_dir=selection_dir,
            filtered=filtered,
            max_price=max_price,
        )
        target_date = date or (
            str(sheet["datetime"].iloc[0]) if not sheet.empty else "unknown"
        )
        title = f"推荐验证日报 - {target_date}"
        credibility_items = self._credibility_lines(str(target_date))
        status_counts = (
            sheet["validation_status"].fillna("unknown").value_counts().sort_index()
            if not sheet.empty
            else pd.Series(dtype=int)
        )
        top_candidate_html = "<p class=\"muted\">没有符合条件的推荐结果。</p>"
        if not sheet.empty:
            first = sheet.iloc[0]
            candidate_name = str(first["name"]).strip()
            candidate_label = f"{first['instrument']} {candidate_name}".strip()
            top_candidate_html = """
<div class="candidate-grid">
  <div class="candidate-item"><span>候选股票</span><strong>{candidate}</strong></div>
  <div class="candidate-item"><span>平均分</span><strong>{avg_score}</strong></div>
  <div class="candidate-item"><span>收盘价</span><strong>{close}</strong></div>
  <div class="candidate-item"><span>买入区间</span><strong>{buy_zone}</strong></div>
  <div class="candidate-item"><span>突破价</span><strong>{breakout}</strong></div>
  <div class="candidate-item"><span>止损价</span><strong>{stop_loss}</strong></div>
  <div class="candidate-item"><span>止盈一</span><strong>{tp1}</strong></div>
  <div class="candidate-item"><span>止盈二</span><strong>{tp2}</strong></div>
  <div class="candidate-item"><span>验证状态</span><strong>{status}</strong></div>
  <div class="candidate-item"><span>验证说明</span><strong>{note}</strong></div>
</div>
""".format(
                candidate=html.escape(candidate_label),
                avg_score=f"{float(first['avg_score']):.4f}",
                close=f"{float(first['close']):.4f}",
                buy_zone=html.escape(f"{float(first['buy_low']):.4f} - {float(first['buy_high']):.4f}"),
                breakout=f"{float(first['breakout_price']):.4f}",
                stop_loss=f"{float(first['stop_loss']):.4f}",
                tp1=f"{float(first['take_profit_1']):.4f}",
                tp2=f"{float(first['take_profit_2']):.4f}",
                status=html.escape(self._zh_validation_status(str(first["validation_status"]))),
                note=html.escape(self._zh_validation_note(str(first["validation_note"]))),
            )

        summary_cards = [
            self._summary_card("股票池", self._zh_stock_pool(self.config.stock_pool)),
            self._summary_card("使用筛选", "是" if filtered else "否"),
            self._summary_card("结果数量", str(len(sheet))),
        ]
        if max_price is not None:
            summary_cards.append(self._summary_card("价格上限", f"{max_price:.2f} 元"))
        if not sheet.empty:
            summary_cards.extend(
                [
                    self._summary_card("最高平均分", f"{float(sheet['avg_score'].max()):.4f}"),
                    self._summary_card("平均平均分", f"{float(sheet['avg_score'].mean()):.4f}"),
                    self._summary_card("验证日期", html.escape(str(sheet['validation_date'].iloc[0]))),
                    self._summary_card("门禁状态", html.escape(str(sheet.get('data_gate_status', pd.Series(['未知'])).iloc[0]))),
                ]
            )

        status_html = "".join(
            self._summary_card(self._zh_validation_status(status), str(int(count)), compact=True)
            for status, count in status_counts.items()
        ) or self._summary_card("状态", "无结果", compact=True)
        credibility_html = "".join(
            f"<li>{html.escape(item.replace('- ', '', 1).replace('`', ''))}</li>" for item in credibility_items
        )

        table_html = self._recommendation_table_html(sheet)
        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --bg: #f4f1e8;
      --panel: #fffdf8;
      --ink: #1f1a15;
      --muted: #6a6259;
      --line: #d7cdbf;
      --accent: #a34a28;
      --accent-soft: #f3d7ca;
      --good: #1c6b4a;
      --warn: #9b6c12;
      --bad: #8d2a2a;
      --shadow: 0 20px 40px rgba(64, 39, 20, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "PingFang SC", "Hiragino Sans GB", sans-serif;
      background: radial-gradient(circle at top left, #fff8ef 0%, var(--bg) 48%, #efe7d9 100%);
      color: var(--ink);
    }}
    .page {{
      max-width: 1280px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }}
    .hero {{
      background: linear-gradient(135deg, rgba(163, 74, 40, 0.14), rgba(255, 253, 248, 0.92));
      border: 1px solid rgba(163, 74, 40, 0.18);
      border-radius: 28px;
      padding: 28px;
      box-shadow: var(--shadow);
    }}
    h1, h2 {{
      margin: 0 0 12px;
      letter-spacing: -0.03em;
    }}
    h1 {{
      font-size: clamp(30px, 4vw, 48px);
      line-height: 1;
    }}
    h2 {{
      font-size: 22px;
      margin-top: 34px;
    }}
    .subtitle {{
      color: var(--muted);
      max-width: 820px;
      line-height: 1.6;
      font-size: 15px;
    }}
    .cards, .status-cards {{
      display: grid;
      gap: 14px;
      margin-top: 20px;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px 18px;
      box-shadow: var(--shadow);
    }}
    .card.compact {{
      padding: 14px 16px;
    }}
    .card-label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .card-value {{
      margin-top: 8px;
      font-size: 24px;
      font-weight: 700;
    }}
    .candidate-panel {{
      margin-top: 28px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 22px;
      box-shadow: var(--shadow);
    }}
    .candidate-grid {{
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }}
    .candidate-item {{
      border: 1px solid rgba(163, 74, 40, 0.12);
      border-radius: 16px;
      padding: 14px;
      background: linear-gradient(180deg, rgba(255,255,255,0.92), rgba(249,242,233,0.96));
    }}
    .candidate-item span {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 6px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .candidate-item strong {{
      font-size: 15px;
      line-height: 1.4;
    }}
    .table-wrap {{
      margin-top: 18px;
      overflow-x: auto;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 980px;
    }}
    thead th {{
      position: sticky;
      top: 0;
      background: #f7ecdf;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    th, td {{
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
      font-size: 14px;
    }}
    tbody tr:hover {{
      background: rgba(163, 74, 40, 0.05);
    }}
    .badge {{
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.03em;
      white-space: nowrap;
    }}
    .badge.buy_zone_touched {{ background: #e4f4ea; color: var(--good); }}
    .badge.breakout_triggered,
    .badge.take_profit_1_hit,
    .badge.take_profit_2_hit {{ background: #eef5df; color: #507113; }}
    .badge.stop_loss_hit,
    .badge.both_stop_and_target_hit {{ background: #f8dfdf; color: var(--bad); }}
    .badge.watchlist,
    .badge.closed_above_buy_zone,
    .badge.closed_below_buy_zone,
    .badge.pending_future_data,
    .badge.missing_validation_prices {{ background: #f6ecd8; color: var(--warn); }}
    .muted {{ color: var(--muted); }}
    .small {{ font-size: 12px; color: var(--muted); }}
    .credibility-list {{ margin: 0; padding-left: 18px; line-height: 1.8; }}
    @media (max-width: 720px) {{
      .page {{ padding: 20px 14px 40px; }}
      .hero {{ padding: 20px; border-radius: 20px; }}
      .candidate-panel, .table-wrap {{ border-radius: 18px; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <h1>{html.escape(title)}</h1>
      <p class="subtitle">这是一份便于人工核对的推荐验证日报，用来检查推荐名单、计划买入区间与下一交易日的真实价格行为是否一致。</p>
      <div class="cards">
        {''.join(summary_cards)}
      </div>
    </section>
    <section>
      <h2>数据可信度摘要</h2>
      <div class="card">
        <ul class="credibility-list">
          {credibility_html}
        </ul>
      </div>
    </section>
    <section>
      <h2>验证摘要</h2>
      <div class="status-cards">
        {status_html}
      </div>
    </section>
    <section class="candidate-panel">
      <h2>第一候选股</h2>
      {top_candidate_html}
    </section>
    <section>
      <h2>推荐明细</h2>
      <div class="table-wrap">
        {table_html}
      </div>
    </section>
  </main>
</body>
</html>
"""

    def save_recommendation_html(
        self,
        limit: int = 10,
        date: str | None = None,
        selection_dir: str | None = None,
        filtered: bool = True,
        max_price: float | None = None,
    ) -> Path:
        max_price = self._resolved_max_price(max_price)
        report = self.recommendation_html(
            limit=limit,
            date=date,
            selection_dir=selection_dir,
            filtered=filtered,
            max_price=max_price,
        )
        output_dir = Path(self.config.analysis_folder).expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)
        target_date = date or "unknown"
        suffix = "filtered" if filtered else "raw"
        output = output_dir / f"recommendation_report_{target_date}_{suffix}{self._price_suffix(max_price)}.html"
        output.write_text(report, encoding="utf-8")
        return output

    def recommendation_spotlight(
        self,
        limit: int = 3,
        date: str | None = None,
        selection_dir: str | None = None,
        filtered: bool = True,
        max_price: float | None = None,
    ) -> str:
        max_price = self._resolved_max_price(max_price)
        sheet = self.recommendation_sheet(
            limit=limit,
            date=date,
            selection_dir=selection_dir,
            filtered=filtered,
            max_price=max_price,
        )
        target_date = date or (str(sheet["datetime"].iloc[0]) if not sheet.empty else "unknown")
        lines = [
            f"# 前三候选解读（{target_date}）",
            "",
            f"- 股票池：`{self._zh_stock_pool(self.config.stock_pool)}`",
            f"- 使用筛选结果：`{'是' if filtered else '否'}`",
            f"- 解读数量：`{min(limit, len(sheet))}`",
        ]
        if max_price is not None:
            lines.append(f"- 价格上限：`{max_price:.2f}`")
        lines.extend(["", "## 数据可信度摘要", ""])
        lines.extend(self._credibility_lines(str(target_date)))
        if sheet.empty:
            lines.extend(["", "没有符合条件的候选股票。"])
            return "\n".join(lines) + "\n"

        if "data_gate_status" in sheet.columns and str(sheet["data_gate_status"].iloc[0]) != "通过":
            lines.extend(["", "## 正式出报状态", "", "本日不具备正式推荐条件，以下内容仅供内部排查和调试。"])

        lines.extend(
            [
                "",
                "## 总览",
                "",
                "这份解读页只保留最值得人工核对的前三只股票，重点看行业、计划价位、风险回报和下一步验证要点。",
            ]
        )
        for _, row in sheet.head(limit).iterrows():
            candidate_name = str(row.get("name", "")).strip()
            candidate_label = f"{row['instrument']} {candidate_name}".strip()
            lines.extend(
                [
                    "",
                    f"## 第 {int(row['score_rank'])} 名：{candidate_label}",
                    "",
                    f"- 行业：`{self._lookup_instrument_industry(str(row['instrument']))}`",
                    f"- 平均分：`{float(row['avg_score']):.4f}`",
                    f"- 收盘价：`{float(row['close']):.4f}`",
                    f"- 买入区间：`{float(row['buy_low']):.4f} - {float(row['buy_high']):.4f}`",
                    f"- 突破确认价：`{float(row['breakout_price']):.4f}`",
                    f"- 止损价：`{float(row['stop_loss']):.4f}`",
                    f"- 止盈位：`{float(row['take_profit_1']):.4f} / {float(row['take_profit_2']):.4f}`",
                    f"- 操作计划：`{self._zh_action_plan(str(row['action_plan']))}`",
                    f"- 信号解读：`{self._zh_signal_reason(str(row['signal_reason']))}`",
                    f"- 当前验证状态：`{self._zh_validation_status(str(row['validation_status']))}`",
                    f"- 验证说明：`{self._zh_validation_note(str(row['validation_note']))}`",
                    f"- 财报摘要：`{str(row.get('fundamental_summary', '暂无有效财报摘要'))}`",
                    f"- 最近三日公告：`{str(row.get('notice_summary', '近三日无重点公告'))}`",
                    f"- 最近三日新闻：`{str(row.get('news_summary', '近三日无重点新闻'))}`",
                    f"- 观察重点：`{self._validation_focus(row)}`",
                ]
            )
        return "\n".join(lines) + "\n"

    def save_recommendation_spotlight(
        self,
        limit: int = 3,
        date: str | None = None,
        selection_dir: str | None = None,
        filtered: bool = True,
        max_price: float | None = None,
    ) -> Path:
        max_price = self._resolved_max_price(max_price)
        report = self.recommendation_spotlight(
            limit=limit,
            date=date,
            selection_dir=selection_dir,
            filtered=filtered,
            max_price=max_price,
        )
        output_dir = Path(self.config.analysis_folder).expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)
        target_date = date or "unknown"
        suffix = "filtered" if filtered else "raw"
        output = output_dir / f"recommendation_spotlight_{target_date}_{suffix}{self._price_suffix(max_price)}.md"
        output.write_text(report, encoding="utf-8")
        return output

    def recommendation_spotlight_html(
        self,
        limit: int = 3,
        date: str | None = None,
        selection_dir: str | None = None,
        filtered: bool = True,
        max_price: float | None = None,
    ) -> str:
        max_price = self._resolved_max_price(max_price)
        sheet = self.recommendation_sheet(
            limit=limit,
            date=date,
            selection_dir=selection_dir,
            filtered=filtered,
            max_price=max_price,
        )
        target_date = date or (str(sheet["datetime"].iloc[0]) if not sheet.empty else "unknown")
        credibility_items = self._credibility_lines(str(target_date))
        cards = []
        for _, row in sheet.head(limit).iterrows():
            candidate_name = str(row.get("name", "")).strip()
            candidate_label = f"{row['instrument']} {candidate_name}".strip()
            cards.append(
                """
<article class="spot-card">
  <div class="spot-rank">第 {rank} 名</div>
  <h2>{candidate}</h2>
  <p class="spot-subtitle">{industry}</p>
  <div class="spot-grid">
    <div><span>平均分</span><strong>{avg_score}</strong></div>
    <div><span>收盘价</span><strong>{close}</strong></div>
    <div><span>买入区间</span><strong>{buy_zone}</strong></div>
    <div><span>突破确认价</span><strong>{breakout}</strong></div>
    <div><span>止损价</span><strong>{stop_loss}</strong></div>
    <div><span>止盈位</span><strong>{take_profit}</strong></div>
  </div>
  <ul class="spot-list">
    <li><strong>操作计划：</strong>{action_plan}</li>
    <li><strong>信号解读：</strong>{signal_reason}</li>
    <li><strong>验证状态：</strong>{validation_status}</li>
    <li><strong>验证说明：</strong>{validation_note}</li>
    <li><strong>财报摘要：</strong>{fundamental_summary}</li>
    <li><strong>最近三日公告：</strong>{notice_summary}</li>
    <li><strong>最近三日新闻：</strong>{news_summary}</li>
    <li><strong>观察重点：</strong>{validation_focus}</li>
  </ul>
</article>
""".format(
                    rank=int(row["score_rank"]),
                    candidate=html.escape(candidate_label),
                    industry=html.escape(self._lookup_instrument_industry(str(row["instrument"]))),
                    avg_score=f"{float(row['avg_score']):.4f}",
                    close=f"{float(row['close']):.4f}",
                    buy_zone=html.escape(f"{float(row['buy_low']):.4f} - {float(row['buy_high']):.4f}"),
                    breakout=f"{float(row['breakout_price']):.4f}",
                    stop_loss=f"{float(row['stop_loss']):.4f}",
                    take_profit=html.escape(f"{float(row['take_profit_1']):.4f} / {float(row['take_profit_2']):.4f}"),
                    action_plan=html.escape(self._zh_action_plan(str(row["action_plan"]))),
                    signal_reason=html.escape(self._zh_signal_reason(str(row["signal_reason"]))),
                    validation_status=html.escape(self._zh_validation_status(str(row["validation_status"]))),
                    validation_note=html.escape(self._zh_validation_note(str(row["validation_note"]))),
                    fundamental_summary=html.escape(str(row.get("fundamental_summary", "暂无有效财报摘要"))),
                    notice_summary=html.escape(str(row.get("notice_summary", "近三日无重点公告"))),
                    news_summary=html.escape(str(row.get("news_summary", "近三日无重点新闻"))),
                    validation_focus=html.escape(self._validation_focus(row)),
                )
            )
        if not cards:
            cards.append('<article class="spot-card"><p>没有符合条件的候选股票。</p></article>')
        credibility_html = "".join(
            f"<li>{html.escape(item.replace('- ', '', 1).replace('`', ''))}</li>" for item in credibility_items
        )
        gate_banner = ""
        if not sheet.empty and "data_gate_status" in sheet.columns and str(sheet["data_gate_status"].iloc[0]) != "通过":
            gate_banner = '<section class="warn-banner">本日不具备正式推荐条件，以下内容仅供内部排查和调试。</section>'
        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(f"前三候选解读 - {target_date}")}</title>
  <style>
    :root {{
      --bg: #f6f1e8;
      --panel: #fffdf9;
      --ink: #1f1a15;
      --muted: #655d55;
      --line: #d8cdbd;
      --accent: #9e4d2d;
      --accent-soft: #f4ddd1;
      --shadow: 0 24px 48px rgba(75, 47, 28, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "PingFang SC", "Hiragino Sans GB", sans-serif;
      color: var(--ink);
      background: linear-gradient(180deg, #fbf6ef 0%, var(--bg) 100%);
    }}
    .page {{ max-width: 1180px; margin: 0 auto; padding: 32px 18px 48px; }}
    .hero {{
      background: linear-gradient(135deg, rgba(158, 77, 45, 0.14), rgba(255, 253, 249, 0.96));
      border: 1px solid rgba(158, 77, 45, 0.18);
      border-radius: 28px;
      padding: 28px;
      box-shadow: var(--shadow);
    }}
    h1 {{ margin: 0 0 10px; font-size: clamp(30px, 4vw, 46px); }}
    .subtitle {{ margin: 0; color: var(--muted); line-height: 1.7; }}
    .meta {{ display: flex; gap: 12px; flex-wrap: wrap; margin-top: 18px; }}
    .pill {{
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.82);
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 13px;
      color: var(--muted);
    }}
    .spots {{ display: grid; gap: 18px; margin-top: 26px; }}
    .spot-card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 22px;
      box-shadow: var(--shadow);
    }}
    .spot-rank {{
      display: inline-block;
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 12px;
      color: var(--accent);
      background: var(--accent-soft);
      margin-bottom: 10px;
      font-weight: 700;
    }}
    .spot-card h2 {{ margin: 0; font-size: 26px; }}
    .spot-subtitle {{ margin: 6px 0 0; color: var(--muted); }}
    .spot-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin-top: 18px;
    }}
    .spot-grid div {{
      border: 1px solid rgba(158, 77, 45, 0.12);
      border-radius: 16px;
      padding: 14px;
      background: linear-gradient(180deg, rgba(255,255,255,0.92), rgba(248,240,231,0.96));
    }}
    .spot-grid span {{
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    .spot-grid strong {{ font-size: 16px; }}
    .spot-list {{ margin: 18px 0 0; padding-left: 18px; line-height: 1.75; }}
    .spot-list li + li {{ margin-top: 4px; }}
    .credibility {{ margin-top: 20px; background: rgba(255,255,255,0.78); border: 1px solid var(--line); border-radius: 18px; padding: 16px 18px; }}
    .credibility ul {{ margin: 0; padding-left: 18px; line-height: 1.8; }}
    .warn-banner {{ margin-top: 20px; border: 1px solid rgba(158, 77, 45, 0.22); background: #f9eadf; color: #7f3a20; border-radius: 18px; padding: 14px 16px; font-weight: 600; }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <h1>前三候选解读</h1>
      <p class="subtitle">这份页面把最值得人工核对的前三只候选股票单独展开，方便快速判断行业分布、计划价位、风险收益比，以及下一个交易日最该盯的验证点。</p>
      <div class="meta">
        <div class="pill">信号日期：{html.escape(str(target_date))}</div>
        <div class="pill">股票池：{html.escape(self._zh_stock_pool(self.config.stock_pool))}</div>
        <div class="pill">价格上限：{html.escape("不限" if max_price is None else f"{max_price:.2f} 元")}</div>
      </div>
      <div class="credibility">
        <ul>
          {credibility_html}
        </ul>
      </div>
    </section>
    {gate_banner}
    <section class="spots">
      {''.join(cards)}
    </section>
  </main>
</body>
</html>
"""

    def save_recommendation_spotlight_html(
        self,
        limit: int = 3,
        date: str | None = None,
        selection_dir: str | None = None,
        filtered: bool = True,
        max_price: float | None = None,
    ) -> Path:
        max_price = self._resolved_max_price(max_price)
        report = self.recommendation_spotlight_html(
            limit=limit,
            date=date,
            selection_dir=selection_dir,
            filtered=filtered,
            max_price=max_price,
        )
        output_dir = Path(self.config.analysis_folder).expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)
        target_date = date or "unknown"
        suffix = "filtered" if filtered else "raw"
        output = output_dir / f"recommendation_spotlight_{target_date}_{suffix}{self._price_suffix(max_price)}.html"
        output.write_text(report, encoding="utf-8")
        return output

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
            ["$close / $factor", "$open / $factor", "$high / $factor", "$low / $factor"],
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

    def _load_entry_candidates(
        self,
        limit: int,
        date: str | None,
        selection_dir: str | None,
        filtered: bool,
    ) -> pd.DataFrame:
        base_dir: Path | None = None
        if selection_dir:
            base_dir = Path(selection_dir).expanduser()
        else:
            try:
                base_dir = self.latest_selection_dir()
            except RuntimeError:
                base_dir = None

        if base_dir is not None:
            date_list = self._selection_dates(base_dir)
            if date_list:
                target_date = date or date_list[-1]
                suffix = "filter_ret" if filtered else "ret"
                file_path = base_dir / f"{target_date}_{suffix}.csv"
                if file_path.exists():
                    df = pd.read_csv(file_path, parse_dates=["datetime"])
                    keep = [col for col in ["datetime", "instrument", "avg_score"] if col in df.columns]
                    result = df.loc[:, keep].head(limit).reset_index(drop=True)
                    result["score_rank"] = range(1, len(result) + 1)
                    return result

        result = self.top_predictions(limit=limit, date=date).reset_index(drop=True)
        if not result.empty:
            result["score_rank"] = range(1, len(result) + 1)
        return result

    def _get_entry_price_history(self, instruments: List[str], target_date: str) -> pd.DataFrame:
        from qlib.data import D

        self._ensure_qlib()
        start_date = (pd.Timestamp(target_date) - pd.Timedelta(days=40)).strftime("%Y-%m-%d")
        df = D.features(
            instruments,
            ["$close / $factor", "$open / $factor", "$high / $factor", "$low / $factor"],
            start_time=start_date,
            end_time=target_date,
            freq="day",
        )
        df.columns = ["close", "open", "high", "low"]
        return df.reset_index().sort_values(["instrument", "datetime"])

    def _build_entry_row(self, candidate: pd.Series, history: pd.DataFrame) -> dict[str, object]:
        work = history.copy().sort_values("datetime").reset_index(drop=True)
        current = work.iloc[-1]
        close = float(current["close"])
        ma5 = float(work["close"].tail(5).mean())
        ma10 = float(work["close"].tail(10).mean())
        atr_pct = float(((work["high"] - work["low"]) / work["close"]).tail(5).mean())
        atr_pct = max(atr_pct, 0.008)

        reference = min(close, ma5)
        buy_low = reference * (1 - atr_pct * 0.6)
        buy_high = reference * (1 + atr_pct * 0.15)
        recent_high = float(work["high"].tail(10).max())
        breakout_price = recent_high * 1.002
        stop_loss = buy_low * (1 - max(atr_pct, 0.015))
        take_profit_1 = close * (1 + max(atr_pct * 2.0, 0.03))
        take_profit_2 = close * (1 + max(atr_pct * 4.0, 0.06))
        action_plan = self._determine_action_plan(close=close, ma5=ma5, ma10=ma10)

        scale = self._raw_price_scale(
            instrument=str(candidate["instrument"]),
            date_str=pd.Timestamp(candidate["datetime"]).strftime("%Y-%m-%d"),
            adjusted_close=close,
        )
        close *= scale
        ma5 *= scale
        ma10 *= scale
        buy_low *= scale
        buy_high *= scale
        breakout_price *= scale
        stop_loss *= scale
        take_profit_1 *= scale
        take_profit_2 *= scale
        risk_pct = (buy_low - stop_loss) / buy_low if buy_low else 0.0
        reward_pct_1 = (take_profit_1 - buy_high) / buy_high if buy_high else 0.0
        reward_pct_2 = (take_profit_2 - buy_high) / buy_high if buy_high else 0.0

        if close >= ma5 >= ma10:
            setup = "trend_follow"
        elif close >= ma10:
            setup = "buy_on_pullback"
        else:
            setup = "wait_breakout"
        name = self._lookup_instrument_name(str(candidate["instrument"]))
        signal_reason = self._build_signal_reason(setup=setup, avg_score=float(candidate["avg_score"]))
        validation = self._build_validation_snapshot(
            instrument=str(candidate["instrument"]),
            date_str=pd.Timestamp(candidate["datetime"]).strftime("%Y-%m-%d"),
            buy_low=buy_low,
            buy_high=buy_high,
            breakout_price=breakout_price,
            stop_loss=stop_loss,
            take_profit_1=take_profit_1,
            take_profit_2=take_profit_2,
        )

        return {
            "datetime": pd.Timestamp(candidate["datetime"]).strftime("%Y-%m-%d"),
            "score_rank": int(candidate.get("score_rank", 0)) if not pd.isna(candidate.get("score_rank", 0)) else 0,
            "instrument": candidate["instrument"],
            "name": name,
            "avg_score": float(candidate["avg_score"]),
            "close": round(close, 4),
            "ma5": round(ma5, 4),
            "ma10": round(ma10, 4),
            "buy_low": round(buy_low, 4),
            "buy_high": round(buy_high, 4),
            "breakout_price": round(breakout_price, 4),
            "stop_loss": round(stop_loss, 4),
            "take_profit_1": round(take_profit_1, 4),
            "take_profit_2": round(take_profit_2, 4),
            "risk_pct": round(risk_pct, 4),
            "reward_pct_1": round(reward_pct_1, 4),
            "reward_pct_2": round(reward_pct_2, 4),
            "price_source": "akshare_sync_csv" if scale != 1.0 else "qlib_raw_by_factor",
            "setup": setup,
            "action_plan": action_plan,
            "signal_reason": signal_reason,
            **validation,
        }

    def _raw_price_scale(self, instrument: str, date_str: str, adjusted_close: float) -> float:
        if adjusted_close == 0:
            return 1.0
        df = self._load_sync_price_frame(instrument)
        if df.empty:
            return 1.0
        row = df[df["date"] == date_str]
        if row.empty or "close" not in row.columns:
            return 1.0
        raw_close = float(row.iloc[-1]["close"])
        if raw_close <= 0:
            return 1.0
        return raw_close / adjusted_close

    def _load_sync_price_frame(self, instrument: str) -> pd.DataFrame:
        if instrument not in self._sync_price_cache:
            sync_file = Path(self.config.sync_dir).expanduser() / "akshare_daily" / f"{instrument}.csv"
            if not sync_file.exists():
                self._sync_price_cache[instrument] = pd.DataFrame()
            else:
                try:
                    frame = pd.read_csv(sync_file)
                except Exception:
                    frame = pd.DataFrame()
                self._sync_price_cache[instrument] = frame
        return self._sync_price_cache[instrument]

    def _lookup_instrument_name(self, instrument: str) -> str:
        if instrument in self._name_cache:
            return self._name_cache[instrument]

        name = self._lookup_name_from_sync(instrument)
        if not name:
            name = self._lookup_name_from_akshare_spot(instrument)
        if not name:
            name = self._lookup_name_from_eastmoney(instrument)
        if not name:
            name = instrument

        self._name_cache[instrument] = name
        return name

    def _lookup_name_from_sync(self, instrument: str) -> str:
        frame = self._load_sync_price_frame(instrument)
        if frame.empty or "name" not in frame.columns:
            return ""
        series = frame["name"].dropna().astype(str).str.strip()
        series = series[series != ""]
        return str(series.iloc[-1]) if not series.empty else ""

    def _lookup_name_from_akshare_spot(self, instrument: str) -> str:
        if instrument == "SH000300":
            return "沪深300"
        if self._spot_name_map is None:
            self._spot_name_map = self._load_cached_name_map()
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
                    self._spot_name_map.update(self._extract_name_pairs(spot))
                self._save_cached_name_map(self._spot_name_map)
            except Exception:
                pass
        return self._spot_name_map.get(instrument, "")

    def _name_cache_file(self) -> Path:
        return Path(self.config.sync_dir).expanduser() / "stock_names.csv"

    def _load_cached_name_map(self) -> Dict[str, str]:
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
        mapping: Dict[str, str] = {}
        for _, row in df[["instrument", "name"]].dropna().iterrows():
            instrument = str(row["instrument"]).strip()
            name = str(row["name"]).strip()
            if instrument and name:
                mapping[instrument] = name
        return mapping

    def _save_cached_name_map(self, mapping: Dict[str, str]) -> None:
        if not mapping:
            return
        cache_file = self._name_cache_file()
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        rows = [{"instrument": instrument, "name": name} for instrument, name in sorted(mapping.items()) if name]
        pd.DataFrame(rows).to_csv(cache_file, index=False, encoding="utf-8-sig")

    @staticmethod
    def _extract_name_pairs(df: pd.DataFrame) -> Dict[str, str]:
        if df is None or df.empty:
            return {}
        code_candidates = ["code", "代码", "证券代码", "A股代码"]
        name_candidates = ["name", "名称", "证券简称", "A股简称"]
        code_col = next((col for col in code_candidates if col in df.columns), None)
        name_col = next((col for col in name_candidates if col in df.columns), None)
        if code_col is None or name_col is None:
            return {}
        mapping: Dict[str, str] = {}
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

    def _lookup_name_from_eastmoney(self, instrument: str) -> str:
        secid = ("1." if instrument.startswith("SH") else "0.") + instrument[2:]
        try:
            response = requests.get(
                "https://push2.eastmoney.com/api/qt/stock/get",
                params={"secid": secid, "fields": "f58"},
                timeout=(3, 5),
            )
            response.raise_for_status()
            data = response.json().get("data") or {}
            name = str(data.get("f58") or "")
        except Exception:
            name = ""
        return name

    def _lookup_instrument_industry(self, instrument: str) -> str:
        if instrument in self._industry_cache:
            return self._industry_cache[instrument]
        mapping = self._load_cached_industry_map()
        if mapping:
            self._industry_cache.update(mapping)
        if instrument in self._industry_cache:
            return self._industry_cache[instrument]
        industry = ""
        try:
            import akshare as ak

            df = ak.stock_individual_info_em(symbol=instrument[2:], timeout=5)
            if not df.empty and {"item", "value"}.issubset(df.columns):
                row = df[df["item"].astype(str) == "行业"]
                if not row.empty:
                    industry = str(row.iloc[0]["value"]).strip()
        except Exception:
            industry = ""
        if not industry:
            industry = "行业待补充"
        self._industry_cache[instrument] = industry
        self._save_cached_industry_map(self._industry_cache)
        return industry

    def _industry_cache_file(self) -> Path:
        return Path(self.config.sync_dir).expanduser() / "stock_industries.csv"

    def _load_cached_industry_map(self) -> Dict[str, str]:
        cache_file = self._industry_cache_file()
        if not cache_file.exists():
            return {}
        try:
            df = pd.read_csv(cache_file)
        except Exception:
            return {}
        required = {"instrument", "industry"}
        if not required.issubset(df.columns):
            return {}
        mapping: Dict[str, str] = {}
        for _, row in df[["instrument", "industry"]].dropna().iterrows():
            instrument = str(row["instrument"]).strip()
            industry = str(row["industry"]).strip()
            if instrument and industry:
                mapping[instrument] = industry
        return mapping

    def _save_cached_industry_map(self, mapping: Dict[str, str]) -> None:
        if not mapping:
            return
        cache_file = self._industry_cache_file()
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        rows = [
            {"instrument": instrument, "industry": industry}
            for instrument, industry in sorted(mapping.items())
            if industry
        ]
        pd.DataFrame(rows).to_csv(cache_file, index=False, encoding="utf-8-sig")

    @staticmethod
    def _build_signal_reason(setup: str, avg_score: float) -> str:
        if setup == "trend_follow":
            posture = "price_above_ma5_ma10"
        elif setup == "buy_on_pullback":
            posture = "holding_above_ma10"
        else:
            posture = "below_short_ma_wait_breakout"
        return f"score_{avg_score:.4f}; {posture}"

    @staticmethod
    def _determine_action_plan(close: float, ma5: float, ma10: float) -> str:
        if close >= ma5 >= ma10:
            return "buy_pullback_or_breakout"
        if close >= ma10:
            return "prefer_pullback_entry"
        return "wait_for_breakout_confirmation"

    def _build_validation_snapshot(
        self,
        instrument: str,
        date_str: str,
        buy_low: float,
        buy_high: float,
        breakout_price: float,
        stop_loss: float,
        take_profit_1: float,
        take_profit_2: float,
    ) -> dict[str, object]:
        validation_date = self._next_trade_date(date_str, 1)
        validation_end = self._next_trade_date(date_str, 2)
        if validation_date is None:
            return {
                "validation_date": None,
                "validation_window_end": None,
                "day1_open": np.nan,
                "day1_high": np.nan,
                "day1_low": np.nan,
                "day1_close": np.nan,
                "entry_zone_hit": False,
                "breakout_hit": False,
                "stop_loss_hit_2d": False,
                "take_profit_1_hit_2d": False,
                "take_profit_2_hit_2d": False,
                "validation_status": "pending_future_data",
                "validation_note": "next_trade_day_not_available",
            }

        day1 = self._lookup_raw_daily_bar(instrument, validation_date)
        window_end = validation_end or validation_date
        window_bars = self._lookup_raw_window(instrument, validation_date, window_end)

        if day1 is None:
            return {
                "validation_date": validation_date,
                "validation_window_end": window_end,
                "day1_open": np.nan,
                "day1_high": np.nan,
                "day1_low": np.nan,
                "day1_close": np.nan,
                "entry_zone_hit": False,
                "breakout_hit": False,
                "stop_loss_hit_2d": False,
                "take_profit_1_hit_2d": False,
                "take_profit_2_hit_2d": False,
                "validation_status": "missing_validation_prices",
                "validation_note": "raw_daily_bar_not_found",
            }

        day1_open = float(day1["open"])
        day1_high = float(day1["high"])
        day1_low = float(day1["low"])
        day1_close = float(day1["close"])
        entry_zone_hit = bool(day1_low <= buy_high and day1_high >= buy_low)
        breakout_hit = bool(day1_high >= breakout_price)

        if window_bars.empty:
            window_low = day1_low
            window_high = day1_high
        else:
            window_low = float(window_bars["low"].min())
            window_high = float(window_bars["high"].max())
        stop_loss_hit = bool(window_low <= stop_loss)
        tp1_hit = bool(window_high >= take_profit_1)
        tp2_hit = bool(window_high >= take_profit_2)
        validation_status = self._validation_status(
            day1_close=day1_close,
            buy_low=buy_low,
            buy_high=buy_high,
            breakout_hit=breakout_hit,
            entry_zone_hit=entry_zone_hit,
            stop_loss_hit=stop_loss_hit,
            tp1_hit=tp1_hit,
            tp2_hit=tp2_hit,
        )

        return {
            "validation_date": validation_date,
            "validation_window_end": window_end,
            "day1_open": round(day1_open, 4),
            "day1_high": round(day1_high, 4),
            "day1_low": round(day1_low, 4),
            "day1_close": round(day1_close, 4),
            "entry_zone_hit": entry_zone_hit,
            "breakout_hit": breakout_hit,
            "stop_loss_hit_2d": stop_loss_hit,
            "take_profit_1_hit_2d": tp1_hit,
            "take_profit_2_hit_2d": tp2_hit,
            "validation_status": validation_status,
            "validation_note": self._validation_note(
                validation_status=validation_status,
                day1_close=day1_close,
                buy_low=buy_low,
                buy_high=buy_high,
                breakout_price=breakout_price,
            ),
        }

    def _lookup_raw_daily_bar(self, instrument: str, date_str: str) -> dict[str, float] | None:
        frame = self._load_sync_price_frame(instrument)
        if frame.empty or "date" not in frame.columns:
            return None
        row = frame[frame["date"].astype(str) == date_str]
        if row.empty:
            return None
        last = row.iloc[-1]
        return {
            "open": float(last["open"]),
            "high": float(last["high"]),
            "low": float(last["low"]),
            "close": float(last["close"]),
        }

    def _lookup_raw_window(self, instrument: str, start_date: str, end_date: str) -> pd.DataFrame:
        frame = self._load_sync_price_frame(instrument)
        if frame.empty or "date" not in frame.columns:
            return pd.DataFrame()
        work = frame.copy()
        work["date"] = pd.to_datetime(work["date"])
        mask = (work["date"] >= pd.Timestamp(start_date)) & (work["date"] <= pd.Timestamp(end_date))
        return work.loc[mask].copy()

    @staticmethod
    def _validation_status(
        day1_close: float,
        buy_low: float,
        buy_high: float,
        breakout_hit: bool,
        entry_zone_hit: bool,
        stop_loss_hit: bool,
        tp1_hit: bool,
        tp2_hit: bool,
    ) -> str:
        if stop_loss_hit and tp1_hit:
            return "both_stop_and_target_hit"
        if tp2_hit:
            return "take_profit_2_hit"
        if tp1_hit:
            return "take_profit_1_hit"
        if stop_loss_hit:
            return "stop_loss_hit"
        if breakout_hit:
            return "breakout_triggered"
        if entry_zone_hit:
            return "buy_zone_touched"
        if day1_close < buy_low:
            return "closed_below_buy_zone"
        if day1_close > buy_high:
            return "closed_above_buy_zone"
        return "watchlist"

    @staticmethod
    def _validation_note(
        validation_status: str,
        day1_close: float,
        buy_low: float,
        buy_high: float,
        breakout_price: float,
    ) -> str:
        if validation_status == "take_profit_2_hit":
            return "two_day_window_reached_take_profit_2"
        if validation_status == "take_profit_1_hit":
            return "two_day_window_reached_take_profit_1"
        if validation_status == "stop_loss_hit":
            return "two_day_window_breached_stop_loss"
        if validation_status == "both_stop_and_target_hit":
            return "two_day_window_hit_stop_and_target"
        if validation_status == "breakout_triggered":
            return f"day1_high_broke_{breakout_price:.2f}"
        if validation_status == "buy_zone_touched":
            return f"day1_range_touched_{buy_low:.2f}_{buy_high:.2f}"
        if validation_status == "closed_below_buy_zone":
            return "day1_close_finished_below_buy_zone"
        if validation_status == "closed_above_buy_zone":
            return "day1_close_finished_above_buy_zone"
        return "day1_price_stayed_between_plan_levels"

    @staticmethod
    def _markdown_table(df: pd.DataFrame) -> str:
        if df.empty:
            return "_没有结果_"
        headers = [str(col) for col in df.columns]
        rows = []
        for row in df.itertuples(index=False, name=None):
            rows.append([str(item) for item in row])
        lines = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
        ]
        for row in rows:
            lines.append("| " + " | ".join(row) + " |")
        return "\n".join(lines)

    @staticmethod
    def _summary_card(label: str, value: str, compact: bool = False) -> str:
        class_name = "card compact" if compact else "card"
        return (
            f'<div class="{class_name}">'
            f'<div class="card-label">{html.escape(str(label))}</div>'
            f'<div class="card-value">{html.escape(str(value))}</div>'
            f"</div>"
        )

    def _recommendation_table_html(self, sheet: pd.DataFrame) -> str:
        if sheet.empty:
            return '<div class="card"><p class="muted">没有符合条件的推荐结果。</p></div>'
        rows = []
        for _, row in sheet.iterrows():
            candidate_name = str(row.get("name", "")).strip()
            candidate_label = f"{row['instrument']} {candidate_name}".strip()
            status = str(row.get("validation_status", "unknown"))
            action_plan = self._zh_action_plan(str(row.get("action_plan", "")))
            rows.append(
                "<tr>"
                f"<td>{int(row['score_rank'])}</td>"
                f"<td><strong>{html.escape(candidate_label)}</strong><div class=\"small\">{html.escape(action_plan)}</div></td>"
                f"<td>{float(row['avg_score']):.4f}</td>"
                f"<td>{float(row['close']):.4f}</td>"
                f"<td>{float(row['buy_low']):.4f} - {float(row['buy_high']):.4f}</td>"
                f"<td>{float(row['breakout_price']):.4f}</td>"
                f"<td>{float(row['stop_loss']):.4f}</td>"
                f"<td>{float(row['take_profit_1']):.4f} / {float(row['take_profit_2']):.4f}</td>"
                f"<td><span class=\"badge {html.escape(status)}\">{html.escape(self._zh_validation_status(status))}</span></td>"
                f"<td>{html.escape(self._zh_validation_note(str(row.get('validation_note', ''))))}</td>"
                "</tr>"
            )
        return """
<table>
  <thead>
    <tr>
      <th>排名</th>
      <th>候选股票</th>
      <th>平均分</th>
      <th>收盘价</th>
      <th>买入区间</th>
      <th>突破价</th>
      <th>止损价</th>
      <th>止盈位</th>
      <th>验证状态</th>
      <th>验证说明</th>
    </tr>
  </thead>
  <tbody>
    {rows}
  </tbody>
</table>
""".format(rows="".join(rows))

    def _attach_feed_context(self, sheet: pd.DataFrame, as_of_date: str) -> pd.DataFrame:
        if sheet.empty:
            return sheet
        enriched = sheet.copy()
        fundamentals = self._load_feed_frame("fundamentals", as_of_date)
        events = self._load_feed_frame("events", as_of_date)
        manifest_market = self._load_feed_manifest("market", as_of_date)
        manifest_fundamentals = self._load_feed_manifest("fundamentals", as_of_date)
        manifest_events = self._load_feed_manifest("events", as_of_date)
        manifest_freshness = self._load_feed_manifest("freshness", as_of_date)

        if not fundamentals.empty and "instrument" in fundamentals.columns:
            keep = [
                "instrument",
                "fundamental_risk_tag",
                "valuation_tag",
                "fundamental_summary",
            ]
            enriched = enriched.merge(fundamentals[[col for col in keep if col in fundamentals.columns]], on="instrument", how="left")
        if not events.empty and "instrument" in events.columns:
            keep = [
                "instrument",
                "event_risk_tag",
                "notice_summary",
                "news_sentiment",
                "news_summary",
            ]
            enriched = enriched.merge(events[[col for col in keep if col in events.columns]], on="instrument", how="left")

        data_sources = " / ".join(
            [
                item
                for item in [
                    str(manifest_market.get("source_name", "")),
                    str(manifest_fundamentals.get("source_name", "")),
                    str(manifest_events.get("source_name", "")),
                ]
                if item
            ]
        )
        data_fetched_at = manifest_freshness.get("fetched_at") or manifest_market.get("fetched_at") or ""
        data_validation_status = manifest_freshness.get("validation_status") or manifest_market.get("validation_status") or "unknown"
        data_gate_status = "通过" if bool(manifest_freshness.get("eligible_for_daily_run")) else "未通过"
        enriched["fundamental_risk_tag"] = enriched.get("fundamental_risk_tag", pd.Series(dtype=object)).fillna("财报信息有限")
        enriched["valuation_tag"] = enriched.get("valuation_tag", pd.Series(dtype=object)).fillna("估值信息有限")
        enriched["fundamental_summary"] = enriched.get("fundamental_summary", pd.Series(dtype=object)).fillna("暂无有效财报摘要")
        enriched["event_risk_tag"] = enriched.get("event_risk_tag", pd.Series(dtype=object)).fillna("事件中性")
        enriched["notice_summary"] = enriched.get("notice_summary", pd.Series(dtype=object)).fillna("近三日无重点公告")
        enriched["news_sentiment"] = enriched.get("news_sentiment", pd.Series(dtype=object)).fillna("中性")
        enriched["news_summary"] = enriched.get("news_summary", pd.Series(dtype=object)).fillna("近三日无重点新闻")
        enriched["data_as_of_date"] = as_of_date
        enriched["data_fetched_at"] = data_fetched_at
        enriched["data_sources"] = data_sources or "本地校验快照"
        enriched["data_validation_status"] = data_validation_status
        enriched["data_gate_status"] = data_gate_status
        return enriched

    def _load_feed_frame(self, feed_type: str, as_of_date: str) -> pd.DataFrame:
        sync_dir = Path(self.config.sync_dir).expanduser()
        if feed_type == "fundamentals":
            dated = sync_dir / "gold" / "fundamentals" / f"fundamentals_{as_of_date}.csv"
            fallback = sync_dir / "gold" / "fundamentals" / "latest_fundamentals.csv"
        elif feed_type == "events":
            dated = sync_dir / "gold" / "events" / f"events_{as_of_date}.csv"
            fallback = sync_dir / "gold" / "events" / "latest_events.csv"
        elif feed_type == "market":
            dated = sync_dir / "gold" / "market" / f"validated_snapshot_{as_of_date}.csv"
            fallback = dated
        else:
            return pd.DataFrame()
        for path in [dated, fallback]:
            if path.exists():
                try:
                    return pd.read_csv(path)
                except Exception:
                    continue
        return pd.DataFrame()

    def _load_feed_manifest(self, feed_type: str, as_of_date: str) -> dict[str, object]:
        path = Path(self.config.sync_dir).expanduser() / "manifests" / as_of_date / f"{feed_type}.json"
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _credibility_lines(self, as_of_date: str) -> list[str]:
        freshness = self._load_feed_manifest("freshness", as_of_date)
        market = self._load_feed_manifest("market", as_of_date)
        fundamentals = self._load_feed_manifest("fundamentals", as_of_date)
        events = self._load_feed_manifest("events", as_of_date)
        source_text = " / ".join(
            item
            for item in [
                str(market.get("source_name", "")),
                str(fundamentals.get("source_name", "")),
                str(events.get("source_name", "")),
            ]
            if item
        ) or "本地校验快照"
        gate_text = "通过" if bool(freshness.get("eligible_for_daily_run")) else "未通过"
        status_text = self._zh_data_validation_status(str(freshness.get("validation_status", "unknown")))
        fetched_at = str(freshness.get("fetched_at") or market.get("fetched_at") or "")
        errors = freshness.get("validation_errors") or []
        lines = [
            f"- 数据日期：`{as_of_date}`",
            f"- 抓取时间：`{fetched_at or '未知'}`",
            f"- 使用来源：`{source_text}`",
            f"- 校验状态：`{status_text}`",
            f"- 正式出报门禁：`{gate_text}`",
        ]
        if errors:
            lines.append(f"- 门禁说明：`{' | '.join(str(item) for item in errors[:5])}`")
        return lines

    @staticmethod
    def _candidate_fetch_limit(limit: int, max_price: float | None) -> int:
        if max_price is None:
            return limit
        return max(limit * 10, 100)

    def _recommendation_sheet_zh(self, sheet: pd.DataFrame) -> pd.DataFrame:
        columns = [
            ("datetime", "信号日期"),
            ("validation_date", "验证日期"),
            ("score_rank", "排名"),
            ("instrument", "股票代码"),
            ("name", "股票名称"),
            ("avg_score", "平均分"),
            ("close", "收盘价"),
            ("buy_low", "买入下沿"),
            ("buy_high", "买入上沿"),
            ("breakout_price", "突破价"),
            ("stop_loss", "止损价"),
            ("take_profit_1", "止盈一"),
            ("take_profit_2", "止盈二"),
            ("action_plan", "操作计划"),
            ("signal_reason", "信号说明"),
            ("entry_zone_hit", "触及买入区间"),
            ("breakout_hit", "触发突破"),
            ("stop_loss_hit_2d", "两日止损命中"),
            ("take_profit_1_hit_2d", "两日止盈一命中"),
            ("take_profit_2_hit_2d", "两日止盈二命中"),
            ("validation_status", "验证状态"),
            ("validation_note", "验证说明"),
            ("price_source", "价格来源"),
            ("fundamental_risk_tag", "财报风险标签"),
            ("valuation_tag", "估值标签"),
            ("fundamental_summary", "财报摘要"),
            ("event_risk_tag", "公告风险标签"),
            ("notice_summary", "近三日公告摘要"),
            ("news_sentiment", "新闻情绪"),
            ("news_summary", "近三日新闻摘要"),
            ("data_as_of_date", "数据日期"),
            ("data_fetched_at", "抓取时间"),
            ("data_sources", "使用来源"),
            ("data_validation_status", "校验状态"),
            ("data_gate_status", "正式出报门禁"),
        ]
        display = pd.DataFrame(columns=[label for _, label in columns]) if sheet.empty else sheet.copy()
        if not display.empty:
            display["action_plan"] = display["action_plan"].map(self._zh_action_plan)
            display["signal_reason"] = display["signal_reason"].map(self._zh_signal_reason)
            display["validation_status"] = display["validation_status"].map(self._zh_validation_status)
            display["validation_note"] = display["validation_note"].map(self._zh_validation_note)
            display["price_source"] = display["price_source"].map(self._zh_price_source)
            if "data_validation_status" in display.columns:
                display["data_validation_status"] = display["data_validation_status"].map(self._zh_data_validation_status)
            for key in [
                "entry_zone_hit",
                "breakout_hit",
                "stop_loss_hit_2d",
                "take_profit_1_hit_2d",
                "take_profit_2_hit_2d",
            ]:
                display[key] = display[key].map(lambda value: "是" if bool(value) else "否")
        rename_map = {src: dst for src, dst in columns}
        available = [src for src, _ in columns if src in display.columns]
        display = display.loc[:, available].rename(columns=rename_map)
        for _, label in columns:
            if label not in display.columns:
                display[label] = pd.Series(dtype=object)
        return display[[label for _, label in columns]]

    @staticmethod
    def _apply_price_filter(plan: pd.DataFrame, max_price: float | None) -> pd.DataFrame:
        if max_price is None or plan.empty:
            return plan
        return plan[plan["close"] <= float(max_price)].copy()

    def _resolved_max_price(self, max_price: float | None) -> float | None:
        if max_price is not None:
            return float(max_price)
        return self.config.max_price

    @staticmethod
    def _price_suffix(max_price: float | None) -> str:
        if max_price is None:
            return ""
        normalized = str(int(max_price)) if float(max_price).is_integer() else str(max_price).replace(".", "_")
        return f"_maxprice{normalized}"

    @staticmethod
    def _zh_stock_pool(stock_pool: str) -> str:
        mapping = {
            "csi300": "沪深300",
            "csi500": "中证500",
            "csi1000": "中证1000",
            "csi800": "中证800",
            "csiall": "全市场",
            "sse180": "上证180",
        }
        return mapping.get(stock_pool, stock_pool)

    @staticmethod
    def _zh_action_plan(action_plan: str) -> str:
        mapping = {
            "buy_pullback_or_breakout": "回踩分批买入或突破确认后介入",
            "prefer_pullback_entry": "优先等回踩买入",
            "wait_for_breakout_confirmation": "先等突破确认",
        }
        return mapping.get(action_plan, action_plan)

    @staticmethod
    def _zh_validation_status(status: str) -> str:
        mapping = {
            "buy_zone_touched": "触及买入区间",
            "breakout_triggered": "触发突破条件",
            "take_profit_1_hit": "触发止盈一",
            "take_profit_2_hit": "触发止盈二",
            "stop_loss_hit": "触发止损",
            "both_stop_and_target_hit": "止损止盈同时触发",
            "closed_below_buy_zone": "收盘跌破买入区间",
            "closed_above_buy_zone": "收盘高于买入区间",
            "watchlist": "继续观察",
            "pending_future_data": "等待后续数据",
            "missing_validation_prices": "缺少验证价格",
            "unknown": "未知",
        }
        return mapping.get(status, status)

    def _zh_validation_note(self, note: str) -> str:
        if note.startswith("day1_range_touched_"):
            _, low, high = note.split("_")[-3:]
            return f"次日价格区间触及买入区间 {low}-{high}"
        if note.startswith("day1_high_broke_"):
            return f"次日最高价突破 {note.split('_')[-1]}"
        mapping = {
            "two_day_window_reached_take_profit_2": "两日窗口内达到止盈二",
            "two_day_window_reached_take_profit_1": "两日窗口内达到止盈一",
            "two_day_window_breached_stop_loss": "两日窗口内跌破止损",
            "two_day_window_hit_stop_and_target": "两日窗口内同时触发止损与止盈",
            "day1_close_finished_below_buy_zone": "次日收盘落在买入区间下方",
            "day1_close_finished_above_buy_zone": "次日收盘落在买入区间上方",
            "day1_price_stayed_between_plan_levels": "次日价格在计划区间之间运行",
            "next_trade_day_not_available": "下一交易日数据暂不可用",
            "raw_daily_bar_not_found": "未找到原始日线验证数据",
        }
        return mapping.get(note, note)

    @staticmethod
    def _zh_price_source(source: str) -> str:
        mapping = {
            "akshare_sync_csv": "AkShare 同步原始日线",
            "qlib_raw_by_factor": "Qlib 原始价格换算",
        }
        return mapping.get(source, source)

    @staticmethod
    def _zh_data_validation_status(status: str) -> str:
        mapping = {
            "passed": "校验通过",
            "failed": "校验失败",
            "unknown": "未知",
        }
        return mapping.get(status, status)

    @staticmethod
    def _join_unique_labels(series: pd.Series) -> str:
        values = []
        for item in series.fillna("").astype(str):
            normalized = item.strip()
            if normalized and normalized not in values:
                values.append(normalized)
        return "、".join(values) if values else "暂无"

    def _validation_focus(self, row: pd.Series) -> str:
        action_plan = str(row.get("action_plan", ""))
        status = str(row.get("validation_status", ""))
        buy_low = float(row.get("buy_low", 0.0))
        buy_high = float(row.get("buy_high", 0.0))
        breakout_price = float(row.get("breakout_price", 0.0))
        stop_loss = float(row.get("stop_loss", 0.0))
        if status == "pending_future_data":
            if action_plan == "prefer_pullback_entry":
                return f"优先观察是否回踩 {buy_low:.2f}-{buy_high:.2f}，若未回踩则不要主动追高，跌破 {stop_loss:.2f} 放弃。"
            if action_plan == "wait_for_breakout_confirmation":
                return f"先看是否有效突破 {breakout_price:.2f}，若没有突破，再看是否回到 {buy_low:.2f}-{buy_high:.2f} 一带。"
            return f"先看是否在 {buy_low:.2f}-{buy_high:.2f} 附近给出回踩机会，突破 {breakout_price:.2f} 后再确认强弱。"
        if status == "buy_zone_touched":
            return "次日已经给到计划买区，后续重点转为观察是否守住止损线，以及能否继续向止盈位推进。"
        if status == "breakout_triggered":
            return "次日已经触发突破条件，后续重点看突破后是否站稳，以及是否出现回落失守。"
        if status == "stop_loss_hit":
            return "次日或两日窗口内已触发止损，这类信号后续更适合作为失败样本复盘。"
        if status in {"take_profit_1_hit", "take_profit_2_hit"}:
            return "次日或两日窗口内已触发止盈，后续重点是评估分批止盈节奏是否合理。"
        return "重点观察价格是否在计划区间附近运行，以及下一交易日是否出现新的确认信号。"

    def _zh_signal_reason(self, signal_reason: str) -> str:
        if not signal_reason:
            return ""
        parts = [part.strip() for part in signal_reason.split(";") if part.strip()]
        zh_parts = []
        for part in parts:
            if part.startswith("score_"):
                zh_parts.append(f"模型平均分 {part.split('_', 1)[1]}")
                continue
            mapping = {
                "price_above_ma5_ma10": "价格位于 5 日线和 10 日线上方，趋势相对更强",
                "holding_above_ma10": "价格仍在 10 日线之上，偏向回踩型机会",
                "below_short_ma_wait_breakout": "价格暂时弱于短期均线，更适合等突破确认",
            }
            zh_parts.append(mapping.get(part, part))
        return "；".join(zh_parts)

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
