# 开发说明

## 建议环境

建议使用本地虚拟环境：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install -r requirements-qlib.txt
```

## 常用开发命令

```bash
make test
make doctor
make probe
make train-smoke
make model-report
make model-review
make model-backtest
make model-backup
make clean-local
```

## 测试

当前项目使用 `unittest`：

```bash
python3 -m unittest discover -s tests -v
```

## CLI 入口

轻量数据 CLI：

```bash
python3 -m qlib_assistant_refactor probe
```

兼容 `roll.py` 的 CLI：

```bash
python3 roll.py data status
python3 roll.py train smoke
python3 roll.py model report
```

安装后的脚本入口：

```bash
qlib-research-workbench probe
qlib-roll data status
```

兼容性脚本也仍然可用：

```bash
python3 scripts/smoke_test.py
.venv/bin/python scripts/qlib_smoke.py
```

## 说明

- `roll.py` 被刻意保持得很薄，主要转发到 `qlib_assistant_refactor.roll_cli`。
- Qlib 初始化和 MLflow 运行时配置都集中在 `qlib_assistant_refactor.qlib_env`。
- `scripts/` 现在主要只保留对包内 CLI 的薄封装，避免逻辑重复。
- `make doctor` 是重新检查远端连通性、本地数据日期和解压结构的最快方式。
- `~/.qlibAssistant/analysis` 下的分析输出会增长得比较快，必要时记得清理旧文件。
