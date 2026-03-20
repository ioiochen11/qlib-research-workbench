# qlib-assistant-refactor

[![CI](https://github.com/ioiochen11/a/actions/workflows/ci.yml/badge.svg)](https://github.com/ioiochen11/a/actions/workflows/ci.yml)

一个围绕原始 [`qlibAssistant`](https://github.com/touhoufan2024/qlibAssistant) 思路做的最小可测试重构版。

它现在已经不只是“验证数据能不能下”，而是具备了一条可运行闭环：

- 数据探测、下载、解压、校验
- Qlib 本地数据读取
- 最小训练与实验落盘
- 保存预测结果聚合
- 每日选股报表导出
- 复盘与 TopK 回测
- `mlruns` 备份与恢复

## 当前状态

这个仓库目前更像一个“可运行、可扩展的重构底座”，不是对原仓库 1:1 的完整替代品。

已经完成的部分：

- 数据链路从原仓库中独立抽出，并支持命令行验证
- Qlib 初始化和 MLflow 实验管理被统一封装
- 训练、分析、复盘、回测、备份都有可运行命令
- 项目已具备包入口、依赖文件、Makefile、文档和测试

## 快速开始

### 1. 安装基础依赖

```bash
python3 -m pip install -r requirements.txt
```

如果你要跑 Qlib 训练和分析：

```bash
python3 -m pip install -r requirements-qlib.txt
```

或者直接用项目安装方式：

```bash
python3 -m pip install -e .
python3 -m pip install -e .[qlib]
```

### 2. 探测远端数据

```bash
python3 -m qlib_assistant_refactor probe
```

### 3. 查看本地数据状态

```bash
python3 roll.py data status
```

### 4. 用 Qlib 验证本地数据

```bash
.venv/bin/python roll.py data qlib-check
```

### 5. 跑一次最小训练

```bash
.venv/bin/python roll.py train smoke
```

### 6. 产出结果报表

```bash
.venv/bin/python roll.py model report
.venv/bin/python roll.py model review
.venv/bin/python roll.py model backtest
```

## 常用命令

### 数据

```bash
python3 -m qlib_assistant_refactor probe
python3 -m qlib_assistant_refactor status
python3 -m qlib_assistant_refactor verify
python3 -m qlib_assistant_refactor qlib-check
python3 roll.py data update --proxy A
```

### 训练

```bash
.venv/bin/python roll.py train plan
.venv/bin/python roll.py train smoke
.venv/bin/python roll.py train start --limit 1
.venv/bin/python roll.py train list-experiments
```

### 结果

```bash
.venv/bin/python roll.py model ls --all
.venv/bin/python roll.py model top --limit 10
.venv/bin/python roll.py model report
.venv/bin/python roll.py model review
.venv/bin/python roll.py model backtest
```

### 备份

```bash
.venv/bin/python roll.py model list-backups
.venv/bin/python roll.py model backup
.venv/bin/python roll.py model restore
```

### Makefile 快捷入口

```bash
make test
make doctor
make probe
make train-smoke
make model-report
make model-review
make model-backtest
make clean-local
```

## 目录说明

- [`qlib_assistant_refactor/`](qlib_assistant_refactor): 主代码目录
- [`tests/`](tests): 单元测试
- [`docs/COMMANDS.md`](docs/COMMANDS.md): 命令参考
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md): 模块结构说明
- [`Makefile`](Makefile): 常用开发命令
- [`pyproject.toml`](pyproject.toml): 项目元数据和脚本入口

## 输出位置

- 本地 Qlib 数据：`~/.qlib/qlib_data/cn_data`
- MLflow 实验：`~/.qlibAssistant/mlruns`
- 分析结果：`~/.qlibAssistant/analysis`
- 备份归档：`~/model_pkl`

## 已验证能力

在当前工作区，这些能力已经实际跑通过：

- 下载并解压 Qlib A 股数据
- 读取 `CSI300` 当日特征
- 训练一个 `Linear + Alpha158` 的最小任务
- 导出 `top_predictions_*.csv`
- 导出 `selection_*/` 报表目录
- 导出 `review/` 和 `backtest/` 结果
- 打包 `mlruns_YYYY-MM-DD.tar.gz`

## 限制与说明

- 网络可达性是时变的，`gh-proxy` 和 GitHub 直链并不总是稳定。
- 当前实现优先保证“可运行、可验证、可测试”，不是对原仓库的全部功能逐字迁移。
- `pyqlib` 安装较重，建议在项目虚拟环境内使用。
- 某些环境会出现 `urllib3` 与 `LibreSSL` 的 warning，但不一定影响实际功能。

## 开发文档

- 命令参考：[docs/COMMANDS.md](docs/COMMANDS.md)
- 架构说明：[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- 开发说明：[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)
