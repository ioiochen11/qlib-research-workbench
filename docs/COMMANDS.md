# 命令参考

## 数据命令

如果你只需要做数据验证和本地数据检查，优先用这个轻量入口。

```bash
python3 -m qlib_assistant_refactor probe
python3 -m qlib_assistant_refactor status
python3 -m qlib_assistant_refactor verify
python3 -m qlib_assistant_refactor qlib-check
python3 -m qlib_assistant_refactor download --output ~/tmp/qlib_bin.tar.gz
python3 -m qlib_assistant_refactor extract --archive ~/tmp/qlib_bin.tar.gz --target-dir ~/.qlib/qlib_data/cn_data --strip-components 1
```

## 兼容 `roll.py` 的 CLI

如果你要跑完整工作流，就用这个兼容原始 `roll.py` 风格的入口。

```bash
python3 roll.py show-config
python3 roll.py data status
python3 roll.py data update --proxy A
python3 roll.py data refresh-sse180
python3 roll.py data sync-akshare --start-date 2026-03-19 --end-date 2026-03-20
python3 roll.py data sync-market --start-date 2026-03-19 --end-date 2026-03-20
python3 roll.py data sync-fundamentals --date 2026-03-20
python3 roll.py data sync-events --date 2026-03-20
python3 roll.py data verify-freshness --date 2026-03-20
python3 roll.py data show-manifest --date 2026-03-20
python3 roll.py data qlib-check
python3 roll.py daily-run
python3 roll.py clawteam-runner
```

`sync-market` 会先把原始快照写到 `raw/market/`，再做 AkShare 和 Eastmoney 的交叉校验，只有通过后才会更新本地 Qlib provider 和 `gold/market/`。

`sync-fundamentals` 和 `sync-events` 会把结构化 feed 快照写到 `gold/fundamentals/` 和 `gold/events/`。

`verify-freshness` 会检查收盘后门禁。如果某个必需 feed 过期、无效或缺失，`daily-run` 会跳过正式训练和日报生成。

`clawteam-runner` 是更适合日常自动化的任务追踪版本。它会创建一个 ClawTeam 看板，在本地执行每个业务步骤，保存逐任务日志，并把 run summary JSON 写到 `.clawteam-workbench/runs/<team>/`。

## 训练

```bash
.venv/bin/python roll.py train plan
.venv/bin/python roll.py train smoke
.venv/bin/python roll.py train start --limit 1
.venv/bin/python roll.py train list-experiments
```

## 模型分析

```bash
.venv/bin/python roll.py model ls --all
.venv/bin/python roll.py model top --limit 10
.venv/bin/python roll.py model entry-plan --limit 10 --max-price 30
.venv/bin/python roll.py model save-entry-plan --limit 10 --max-price 30
.venv/bin/python roll.py model recommendations --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model save-recommendations --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model recommendation-report --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model save-recommendation-report --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model recommendation-html --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model save-recommendation-html --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model save-top --limit 20
.venv/bin/python roll.py model report
.venv/bin/python roll.py model review
.venv/bin/python roll.py model backtest
```

`model recommendations` 是最适合人工核对的视图，会同时给出：

- 推荐股票和名称
- 计划买入区间、突破价、止损和止盈
- 下一交易日的原始 OHLC 价格
- 是否真正触及买入区间或突破位
- 一个便于快速人工核对的验证状态

`model recommendation-report` 会把同一份数据导出成紧凑的 Markdown 日报，比控制台宽表更适合阅读和归档。

`model recommendation-html` 会把这份日报导出成一个自包含 HTML 页面，带摘要卡片和表格样式，可以直接在浏览器里打开。

`save-recommendations` 现在会导出全中文 CSV，`save-recommendation-report` 和 `save-recommendation-html` 也都已经使用中文标题、验证状态和说明。

`daily-run` 是默认的收盘后一键流程，针对的是 `沪深300 + 30 元以下` 这套默认策略。它会同步并校验 `market / fundamentals / events`，执行 freshness gate，训练模型，生成 selection report，并同时写 dated 文件和 `latest_*` 推荐日报。如果切换成 `上证180` 股票池，它会先刷新本地 `sse180.txt`。

如果 freshness gate 失败，`daily-run` 会直接退出，不会改动旧的 `latest_*` 文件。详细原因会写到当日 manifest 目录里。

## 备份

```bash
.venv/bin/python roll.py model list-backups
.venv/bin/python roll.py model backup
.venv/bin/python roll.py model restore
.venv/bin/python roll.py model restore --archive-name mlruns_2026-03-19.tar.gz
```

## Makefile 快捷命令

```bash
make test
make doctor
make probe
make refresh-sse180
make sync-akshare
make train-smoke
make model-recommendations
make model-recommendation-report
make model-recommendation-html
make model-report
make model-review
make model-backtest
make model-backup
make daily-run
make clawteam-runner
make clean-local
```
