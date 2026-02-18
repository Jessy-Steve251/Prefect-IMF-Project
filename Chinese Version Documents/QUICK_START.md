# ⚡ 快速启动指南 (Quick Start)

> **预计时间**: 30 分钟  
> **难度**: ⭐ 简单  
> **前置条件**: Python 3.11+, Prefect Cloud 账户

---

## 📋 5 步快速启动

### 第 1 步：验证环境 (3 分钟)

```powershell
# 打开 PowerShell，进入项目目录
cd C:\Users\yli\Desktop\Prefect_Project

# 检查 Python
python --version
# 预期: Python 3.11.9+

# 检查虚拟环境
python -c "import sys; print(sys.executable)"
# 预期: 包含 'venv' 或虚拟环境名称的路径
```

✅ **检查清单**:
- [ ] Python 版本 ≥ 3.11
- [ ] 在虚拟环境中运行

---

### 第 2 步：安装依赖 (2 分钟)

```powershell
# 安装项目依赖
pip install -r requirements.txt

# 验证安装
python -c "import prefect, pandas, requests; print('✅ 依赖安装成功')"
```

✅ **检查清单**:
- [ ] prefect 已安装
- [ ] pandas 已安装
- [ ] requests 已安装

---

### 第 3 步：登录 Cloud (2 分钟)

```powershell
# 登录 Prefect Cloud
prefect cloud login

# 系统会提示输入 API Key
# 访问 https://app.prefect.cloud 获取 API Key
```

✅ **检查清单**:
- [ ] 已获得 Prefect Cloud 账户
- [ ] 已登录到 Cloud

---

### 第 4 步：部署 (2 分钟)

```powershell
# 部署所有 Flow 到 Cloud
prefect deploy

# 预期输出:
# Deployment 'currency-acquisition/Currency Acquisition' created
# Deployment 'prepare-batch/Prepare Batch' created
# Deployment 'process-batch/Process Batch' created
```

✅ **检查清单**:
- [ ] 所有 3 个 Deployment 已创建
- [ ] Cloud UI 中可看到 Deployment

---

### 第 5 步：配置 Schedule (15 分钟)

#### 方式 A：UI 配置（推荐）

访问 https://app.prefect.cloud

**第 1 个 Schedule（currency-acquisition）**:
1. 进入 **Deployments** → **currency-acquisition**
2. 点击 **Schedules** 标签
3. 点击 **Create Schedule**
4. 填写：
   - Cron: `0 9 15,25,28,29,30,31 * *`
   - Timezone: `Asia/Shanghai`
5. 勾选 **Enabled**
6. 点击 **Save**

**第 2 个 Schedule（prepare-batch）**:
重复上述步骤，使用：
- Cron: `30 9 15,25,28,29,30,31 * *`

**第 3 个 Schedule（process-batch）**:
重复上述步骤，使用：
- Cron: `0 10 15,25,28,29,30,31 * *`

#### 方式 B：快速表 (复制粘贴)

| Flow | Cron | 时间 |
|------|------|------|
| currency-acquisition | `0 9 15,25,28,29,30,31 * *` | 09:00 |
| prepare-batch | `30 9 15,25,28,29,30,31 * *` | 09:30 |
| process-batch | `0 10 15,25,28,29,30,31 * *` | 10:00 |

✅ **检查清单**:
- [ ] 创建了 3 个 Schedule
- [ ] 所有 Schedule 已启用
- [ ] 时区设置为 Asia/Shanghai

---

## 🔄 额外步骤：启动 Worker

> ⚠️ **重要**: 必须启动 Worker，Cloud 才能远程触发 Flow

```powershell
# 在新的 PowerShell 窗口中运行（持续运行）
prefect worker start --pool default

# 预期输出:
# Worker 'default' started polling for work
# Watching for flow runs from pool 'default'...
```

> 💡 **提示**: 不要关闭此窗口！Worker 需要持续运行。

---

## ✅ 验证部署成功

### 检查 1：Cloud UI

访问 https://app.prefect.cloud

**Deployments 页面**:
```
✅ currency-acquisition
   Status: Ready
   Last run: N/A (未运行)
   Next run: 2025-01-15 09:00

✅ prepare-batch
   Status: Ready
   Next run: 2025-01-15 09:30

✅ process-batch
   Status: Ready
   Next run: 2025-01-15 10:00
```

### 检查 2：命令行验证

```powershell
# 查看部署列表
prefect deployment ls
# 应该显示 3 个 Deployment

# 查看 Schedule 列表
prefect deployment schedule ls
# 应该显示 3 个 Schedule
```

### 检查 3：手动测试（可选）

```powershell
# 手动触发第一个 Flow
prefect deployment run currency-acquisition

# 预期输出:
# Submitted flow run 'abc123...'
# Check it out on the Cloud UI: https://app.prefect.cloud/...
```

观察 Cloud UI，检查运行日志：
```
✅ Flow started
✅ Fetching exchange rates...
✅ Retrieved 118 countries
✅ Flow completed successfully
```

---

## 🎯 现在你已完成！

当看到所有这些 ✅ 时，说明部署成功：

- [x] 3 个 Deployment 已创建
- [x] 3 个 Schedule 已配置
- [x] Worker 已启动
- [x] Cloud UI 显示下一个运行时间
- [x] 手动测试通过（可选）

**预期的自动执行时间表**:

| 日期 | 时间 | 动作 |
|------|------|------|
| 15 号 | 09:00 | 获取汇率 |
|  | 09:30 | 准备数据 |
|  | 10:00 | 处理数据 |
| 25 号 | 09:00 | 获取汇率 |
|  | 09:30 | 准备数据 |
|  | 10:00 | 处理数据 |
| 28-31 号 | 同上 | 同上 |

---

## ❓ 常见问题

### Q: Flow 未按时触发？

**检查清单**:
1. Worker 是否在运行？
   ```powershell
   prefect worker inspect default
   ```

2. Schedule 是否启用？
   - 进入 Cloud UI → Deployments → [Flow] → Schedules
   - 检查状态是否为 "Enabled"

3. 当前时间是否超过下一个触发时间？
   - Cloud UI 中显示的 "Next run" 时间
   - 需要等到该时间才会触发

### Q: 显示 "No worker is available"？

**解决**: 启动 Worker
```powershell
prefect worker start --pool default
```
确保此窗口持续运行且显示：
```
Worker 'default' started polling for work
```

### Q: 怎样手动运行 Flow？

```powershell
# 方式 1: 命令行
prefect deployment run currency-acquisition

# 方式 2: Cloud UI
# Deployments → [Flow] → Custom runs
# 点击 "Run"
```

### Q: 日志在哪里看？

**Local 日志**:
```
6_logs/ 目录
```

**Cloud 日志**:
```
Cloud UI → Deployments → [Flow] → Latest runs → [Run] → Logs
```

### Q: 多久会看到数据输出？

**流程时间**:
- 汇率获取: ~45 秒 → CSV 文件
- 数据准备: ~10 秒 → Manifest 文件
- 数据处理: ~10 秒 → 归档完成
- **总计**: ~65 秒

### Q: 数据去哪里了？

```
根据工作流阶段：

currency-acquisition:
  输出 → data/exchange_rates.csv

prepare-batch:
  输出 → 2_preprocessing/manifest_*.json

process-batch:
  输出 → 4_archive/* (最终存档)
        → 6_logs/* (执行日志)
        → 5_error/* (错误记录，如果有)
```

---

## 📚 需要更多帮助？

| 需求 | 参考文档 |
|------|---------|
| 详细部署步骤 | [DEPLOYMENT_SUMMARY.md](DEPLOYMENT_SUMMARY.md) |
| Cloud UI 设置 (详细) | [SCHEDULE_SETUP_GUIDE.md](SCHEDULE_SETUP_GUIDE.md) |
| 完整检查清单 | [PRODUCTION_DEPLOYMENT_CHECKLIST.md](PRODUCTION_DEPLOYMENT_CHECKLIST.md) |
| API 集成说明 | [EXCHANGE_RATE_FETCHER_NOTES.md](EXCHANGE_RATE_FETCHER_NOTES.md) |
| 项目完成总结 | [PROJECT_COMPLETION_SUMMARY.md](PROJECT_COMPLETION_SUMMARY.md) |
| 故障排除 | [PRODUCTION_DEPLOYMENT_CHECKLIST.md#-故障排除指南](PRODUCTION_DEPLOYMENT_CHECKLIST.md) |

---

## 🔧 有用的命令速查

```powershell
# ===== 部署相关 =====
prefect deploy                          # 部署 Flow
prefect deployment ls                   # 列出所有部署
prefect deployment run [NAME]           # 手动运行 Flow

# ===== Worker 相关 =====
prefect worker start --pool default     # 启动 Worker
prefect worker inspect default          # 检查 Worker 状态

# ===== Schedule 相关 =====
prefect deployment schedule ls          # 列出所有 Schedule

# ===== 日志相关 =====
prefect flow-run ls                     # 列出所有运行
prefect flow-run logs [RUN_ID]          # 查看运行日志

# ===== Cloud 相关 =====
prefect cloud login                     # 登录 Cloud
prefect cloud workspace ls              # 列出工作区
```

---

## ⚡ 速记卡

**最重要的 3 个 Cron 表达式**:

```
汇率获取        09:00   0 9 15,25,28,29,30,31 * *
数据准备        09:30   30 9 15,25,28,29,30,31 * *
数据处理        10:00   0 10 15,25,28,29,30,31 * *
```

**最重要的 3 个命令**:

```powershell
prefect deploy                          # 部署
prefect worker start --pool default     # 启动 Worker
prefect deployment run [NAME]           # 手动运行
```

**最重要的 3 个链接**:

```
Cloud UI: https://app.prefect.cloud
Cron 帮助: https://crontab.guru
本项目首页: README.md
```

---

## ✨ 现在你可以：

✅ 自动获取每月汇率数据
✅ 按计划处理和归档数据
✅ 在 Cloud UI 中监控运行状态
✅ 查看详细的执行日志
✅ 手动触发 Flow 进行测试

---

**部署日期**: 2025-01  
**预计首次运行**: 2025-01-15 09:00  
**时区**: Asia/Shanghai

祝部署顺利！🎉
