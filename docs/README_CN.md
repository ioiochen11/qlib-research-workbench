# 中文说明

`qlib-research-workbench` 是一个基于 `Qlib` 的中文量化研究工作台，目标不是做实盘交易执行，而是把“数据同步、滚动训练、候选推荐、验证日报、复盘回测”串成一条每天都能复用的本地流程。

当前默认流程是有明确偏好的：

- 股票池默认使用 `沪深300`
- 训练和推荐都只保留 `30 元以下` 的股票
- 收盘后运行 `daily-run`，自动生成中文 `CSV / Markdown / HTML` 日报
- 输出结果优先服务“人工核对”和“快速验证”

## 适合拿它做什么

- 验证本地 Qlib 数据是否可用
- 每天收盘后自动生成候选股票清单
- 检查推荐价位和下一交易日价格行为是否一致
- 做个人研究项目，或者团队内部的研究型工具底座

## 当前已经具备的能力

- 远端 Qlib 数据包探测、下载、解压、校验
- AkShare 日频同步，不必再等别人更新数据包
- 本地 Qlib 初始化和特征读取检查
- `Alpha158` + 滚动训练任务生成
- 样本级 `30 元以下` 价格过滤
- 多 recorder 聚合、推荐导出、筛选报表
- 中文推荐日报和 HTML 可视化页面
- 前三候选股票中文解读页
- 复盘、TopK 回测、`mlruns` 备份

## 一条最常用的命令

每天收盘后，最推荐直接跑：

```bash
.venv/bin/python roll.py daily-run
```

这条命令会自动执行：

1. 同步最新日频数据
2. 训练当天模型
3. 生成推荐名单
4. 生成中文 Markdown / HTML 日报
5. 生成固定名称的 `latest_*` 文件，便于自动化和直接打开

## 主要输出文件

默认输出目录在 `~/.qlibAssistant/analysis`。

常用文件有：

- `latest_recommendations.csv`
  这是一份适合进一步筛选或做二次分析的推荐宽表。
- `latest_recommendation_report.md`
  这是一份中文 Markdown 日报，适合直接阅读。
- `latest_recommendation_report.html`
  这是一份浏览器可直接打开的中文可视化日报。
- `latest_recommendation_spotlight.md`
  这是一份只看前三只股票的重点解读页。
- `latest_recommendation_spotlight.html`
  这是前三只股票的 HTML 版重点解读页。

## 你最可能会用到的命令

### 1. 检查数据

```bash
python3 -m qlib_assistant_refactor probe
python3 roll.py data status
.venv/bin/python roll.py data qlib-check
```

### 2. 同步日频数据

```bash
.venv/bin/python roll.py data sync-akshare
```

如果你想指定日期范围：

```bash
.venv/bin/python roll.py data sync-akshare --start-date 2026-03-19 --end-date 2026-03-20
```

### 3. 跑训练

```bash
.venv/bin/python roll.py train plan --limit 1
.venv/bin/python roll.py train smoke
.venv/bin/python roll.py train start
```

### 4. 看推荐

```bash
.venv/bin/python roll.py model recommendations --date 2026-03-20 --limit 10 --max-price 30
.venv/bin/python roll.py model recommendation-report --date 2026-03-20 --limit 10 --max-price 30
.venv/bin/python roll.py model recommendation-html --date 2026-03-20 --limit 10 --max-price 30
```

### 5. 看前三只重点解读

```bash
.venv/bin/python roll.py model recommendation-spotlight --date 2026-03-20 --limit 3 --max-price 30
.venv/bin/python roll.py model save-recommendation-spotlight-html --date 2026-03-20 --limit 3 --max-price 30
```

## 推荐结果怎么看

这套系统不是“今天一定涨”的预测器，它更像一个短周期收益排序系统。

你在日报里最值得重点看这几项：

- `平均分`
  这是模型排序分数，越高一般代表相对更看好。
- `买入区间`
  这是规则化后的计划介入区间，更适合人工核对。
- `突破价`
  用于观察是否要等更强确认。
- `止损价 / 止盈位`
  用来快速判断这笔计划的风控结构。
- `验证状态`
  用于检查下一交易日是否真的给到计划中的价格行为。

## 当前默认策略口径

当前默认配置大致是：

- 股票池：`沪深300`
- 价格上限：`30 元`
- 数据集：`Alpha158`
- 模型：优先 `LightGBM`
- 训练窗口：最近 `2 年`
- 验证窗口：最近 `4 个月`

这个口径的目标不是追求最复杂，而是让训练行为更稳定、排序指标更适合候选股票筛选。

## 已知说明

- 行业信息目前会优先尝试在线查询；如果远端接口不稳定，会回退成“行业待补充”，不影响推荐价位和验证结果本身。
- 部分环境会出现 `urllib3 / LibreSSL` 的 warning，这通常不会阻塞主流程。
- 这是研究工作台，不是生产级交易执行系统。

## 建议的使用方式

如果你只是想每天看看这套系统推荐了什么，最简单的方式就是：

1. 收盘后运行 `daily-run`
2. 打开 `latest_recommendation_report.html`
3. 再打开 `latest_recommendation_spotlight.html`
4. 用你自己的行情软件核对价格和计划区间

如果你想继续二开，这个仓库更适合作为“研究底座”，而不是直接当成最终产品。
