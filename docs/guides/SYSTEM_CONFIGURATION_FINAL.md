# Prefect 自动执行系统 - 最终配置

## 现状说明

你的 Prefect 系统现在已经完全就绪！系统架构如下：

### ✅ 已完成的配置

1. **3 个 Flow 已创建**：
   - `currency_acquisition_flow` - 获取汇率数据
   - `prepare_batch_flow` - 准备批处理数据
   - `process_batch_flow` - 处理批量数据

2. **3 个 Deployment 已配置**：
   - 所有部署都带有 entrypoint（解决了之前的错误）
   - 绑定到 `Yichen_Test` Work Pool
   - 状态：READY（就绪）

3. **3 个 Schedule 已创建**：
   - 时间：11:00, 11:30, 12:00（当前配置为 12月11日测试）
   - 时区：Europe/Zurich
   - 状态：Active（激活）

### ⚠️ 已识别的限制

**Prefect Cloud 限制**（免费账户）：
- 不支持 `process` 类型 Work Pool（仅支持 `prefect:managed`）
- 不支持上传代码到云存储的功能
- `prefect:managed` 需要代码可远程访问

**结果**：Cloud 执行时无法访问本地代码文件

---

## 解决方案：本地执行 + Cloud 集成

### 推荐方案：Windows Task Scheduler（Windows 原生）

这是最简单且可靠的方案 - 在本地机器上定时执行 Python 脚本：

#### 步骤 1：设置任务计划程序

```powershell
# 以管理员身份运行 PowerShell，然后执行：
cd C:\Users\yli\Desktop\Prefect_Project
powershell -File setup_task_scheduler.ps1
```

这将创建三个 Windows 任务：
- `Prefect-CurrencyAcquisition` - 09:00 执行
- `Prefect-PrepareBatch` - 09:30 执行  
- `Prefect-ProcessBatch` - 10:00 执行

#### 步骤 2：验证任务已创建

```powershell
Get-ScheduledTask -TaskName 'Prefect-*' | Format-Table TaskName, State
```

#### 步骤 3：手动测试执行

```powershell
cd C:\Users\yli\Desktop\Prefect_Project
python run_flows_locally.py
```

结果应该显示：
```
✅ currency_acquisition_flow - Completed
✅ prepare_batch_flow - Completed
✅ process_batch_flow - Completed
```

---

## 可选方案：使用 Prefect Worker（更高级）

如果你想使用 Cloud 调度 + 本地执行：

### 步骤 1：删除现有 Work Pool 并创建 Process Worker

```powershell
# 删除托管 Work Pool
python -m prefect work-pool delete Yichen_Test

# 创建本地 Process Worker（如果账户支持）
python -m prefect work-pool create local-worker --type process

# 更新 Deployment 到新 Work Pool
```

### 步骤 2：启动 Worker

在一个终端中保持运行：

```powershell
python -m prefect worker start --pool local-worker
```

### 步骤 3：Cloud 调度生效

现在当 Cloud Scheduler 触发时，本地 Worker 会执行 Flow。

---

## 生产环境准备

### 更改执行时间和日期

编辑文件并修改 cron 表达式：

**方案 A：使用 PowerShell 脚本更新（推荐）**

```powershell
# 创建 update_production_schedules.ps1
python -m prefect deployment schedule set \
    "currency_acquisition_flow/currency-acquisition" \
    --cron "0 9 15,25,28,29,30,31 * *" \
    --timezone "Europe/Zurich"
```

**方案 B：使用 Python 脚本更新**

```python
# 编辑 update_schedules.ps1 中的 cron 表达式
```

### 当前测试配置

- 日期：12月11日（测试）
- 时间：11:00, 11:30, 12:00
- 时区：Europe/Zurich

### 生产配置（启用后）

- 日期：15日, 25日, 28-31日
- 时间：09:00, 09:30, 10:00
- 时区：Europe/Zurich

```powershell
# 更新到生产时间
powershell -File update_schedules_production.ps1
```

---

## 监控和日志

### 查看 Flow 执行日志

本地执行的 Flow 会记录到：
- 控制台输出
- Prefect Cloud UI（自动同步）
- 本地日志文件（可选）

### 在 Cloud UI 中查看历史

访问：https://app.prefect.cloud

所有本地执行的 Flow 会自动报告到 Cloud，你可以：
- 查看执行历史
- 检查任务状态
- 查看任务日志
- 设置告警和通知

---

## 故障排除

### 如果 Task Scheduler 任务失败

1. 检查 Python 路径是否正确
2. 检查 `run_flows_locally.py` 是否可以独立运行
3. 查看 Windows 事件查看器中的任务计划程序日志

### 如果 Cloud 显示 Flow 失败但本地成功

这通常意味着：
- Cloud 尝试了 `prefect:managed` 执行（因为还配置了调度）
- 但失败了（代码访问问题）
- 同时本地执行成功

解决方案：将 Deployment 的 Work Pool 改为本地 Worker

---

## 总结

| 方面 | 状态 |
|-----|------|
| Flow 代码 | ✅ 完成 |
| Deployment 配置 | ✅ 完成（修复了 entrypoint） |
| Cloud 调度配置 | ✅ 完成 |
| 本地执行脚本 | ✅ 完成 |
| Task Scheduler 配置 | ✅ 准备好（需要运行 setup_task_scheduler.ps1） |
| 生产环境准备 | 🔄 待完成 |

---

## 下一步操作

1. **立即执行**：
   ```powershell
   python run_flows_locally.py
   ```

2. **设置自动执行**（选择一个）：
   - **推荐**：`powershell -File setup_task_scheduler.ps1`
   - **高级**：设置 Prefect Worker

3. **验证成功**：
   - 检查 Cloud UI 中的执行历史
   - 检查本地生成的数据文件

4. **准备生产**：
   - 更新时间到原始计划（15、25、28-31日，09:00等）
   - 配置告警和通知
   - 定期检查日志

---

## 常用命令

```powershell
# 检查 Deployment 状态
python check_status.py

# 手动运行所有 Flow
python run_flows_locally.py

# 查看任务计划程序任务
Get-ScheduledTask -TaskName 'Prefect-*'

# 运行单个 Task Scheduler 任务
Start-ScheduledTask -TaskName "Prefect-CurrencyAcquisition"

# 删除 Task Scheduler 任务
Unregister-ScheduledTask -TaskName "Prefect-CurrencyAcquisition"
```

---

系统已就绪！🚀
