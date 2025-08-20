# Huey TaskRegistry 修复说明

## 问题描述
遇到错误：`HueyException: __main__.process_data not found in TaskRegistry`

## 问题原因
当使用 `python script.py` 直接运行脚本时，模块名为 `__main__`，但consumer通过 `huey_consumer.py module.huey` 启动时，模块名为 `module`，导致任务注册名不匹配。

## 解决方案
1. **使用正确的consumer启动方式**：使用 `uv run huey_consumer.py huey_prototype.huey`
2. **创建独立的producer脚本**：避免 `__main__` 模块名问题

## 使用方法

### 启动Consumer
```bash
uv run huey_prototype.py consumer
```

### 启动Producer
```bash
uv run producer.py <ID>
```

## 验证修复
运行完整测试：
```bash
uv run final_test.py
```

## 关键文件
- `huey_prototype.py`: 主要的Huey配置和任务定义
- `producer.py`: 独立的生产者脚本（避免模块名冲突）
- `final_test.py`: 完整流程测试脚本

## 修复要点
1. Consumer通过正确的模块路径启动
2. Producer导入任务而不是在 `__main__` 中直接定义
3. 确保任务注册名一致：`huey_prototype.process_data`
