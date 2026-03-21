# 架构说明

## 目标

这次重构保留了原始 `qlibAssistant` 的核心思路，但把整条流程拆成了更小、更容易测试的层次：

1. 数据可达性与本地数据集校验
2. Qlib 初始化与任务生成
3. 训练与实验结果持久化
4. 预测聚合、日报、复盘与回测
5. `mlruns` 的备份与恢复

## 核心模块

### `qlib_assistant_refactor/config.py`

负责应用配置模型和 YAML 加载。

### `qlib_assistant_refactor/qlib_env.py`

集中处理：

- `provider_uri` path handling
- 本地最新交易日读取
- MLflow 实验管理配置
- `qlib.init(...)`

### `qlib_assistant_refactor/data_service.py`

负责远端探测、压缩包下载、解压和本地数据校验。

### `qlib_assistant_refactor/data_cli.py`

基于 `DataService` 实现原始风格的数据子命令。

### `qlib_assistant_refactor/task_factory.py`

构建最小化的 Qlib 任务模板和滚动窗口分段。

### `qlib_assistant_refactor/train_cli.py`

负责：

- 滚动任务生成
- 训练计划预览
- smoke 训练
- 实验列表展示

### `qlib_assistant_refactor/model_cli.py`

负责：

- recorder 发现与过滤
- 已保存预测结果聚合
- 按日生成报表
- 复盘与回测输出
- `mlruns` backup and restore

### `qlib_assistant_refactor/roll_cli.py`

提供兼容 `qlibAssistant` 风格的顶层 CLI：

- `data`
- `train`
- `model`

### `qlib_assistant_refactor/cli.py`

提供更轻量、偏数据验证的包内 CLI，适合快速检查任务。

## 入口命令

- `python3 -m qlib_assistant_refactor ...`
- `python3 roll.py ...`
- `qlib-roll ...` after editable install

## 主要输出目录

- 本地 Qlib 数据：`~/.qlib/qlib_data/cn_data`
- MLflow 实验目录：`~/.qlibAssistant/mlruns`
- 分析输出目录：`~/.qlibAssistant/analysis`
- 归档压缩包目录：`~/model_pkl`
