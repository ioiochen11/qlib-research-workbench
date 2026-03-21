# 贡献说明

## 开始之前

- 使用 Python `3.9+`
- 优先在本地虚拟环境中开发
- 修改尽量聚焦、尽量小步

## 本地环境

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

如果你需要运行和 Qlib 相关的命令：

```bash
.venv/bin/python -m pip install -r requirements-qlib.txt
```

## 建议工作流

1. 为你的改动创建一个分支
2. 用尽量小的改动解决问题
3. 在本地运行相关检查
4. 提交 Pull Request，并附上简短说明和测试记录

## 本地检查

运行基础测试：

```bash
make test
```

运行一次快速环境与数据健康检查：

```bash
make doctor
```

如果你的改动会影响训练或日报流程，下面这些 spot check 很有用：

```bash
make train-smoke
make model-report
```

## Pull Request 说明

建议至少写清楚：

- 改了什么
- 为什么改
- 你是怎么验证的
- 后续待做事项或已知限制

## 范围建议

- 不要把大体积生成文件、下载压缩包和本地实验输出提交进 git
- 优先扩展包内 CLI，而不是继续增加独立脚本
- 如果新增了命令或工作流，记得同步更新 `README.md` 或 `docs/`
