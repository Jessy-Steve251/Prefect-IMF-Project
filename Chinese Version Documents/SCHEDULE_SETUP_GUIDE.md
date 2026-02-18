# Prefect Cloud 定时任务配置指南

## 快速总结

你需要在 Prefect Cloud UI 中手动配置三个定时任务：

| Flow | 执行时间 | Cron 表达式 |
|------|--------|-----------|
| currency-acquisition | 每月 09:00 | `0 9 15,25,28,29,30,31 * *` |
| prepare-batch | 每月 09:30 | `30 9 15,25,28,29,30,31 * *` |
| process-batch | 每月 10:00 | `0 10 15,25,28,29,30,31 * *` |

---

## 📋 配置步骤

### 步骤 1：登录 Prefect Cloud

1. 打开浏览器访问：https://app.prefect.cloud
2. 使用你的账户登录

### 步骤 2：为 currency-acquisition 设置定时任务

1. 在左侧菜单选择 **"Deployments"** 或 **"Flows"**
2. 查找 **"currency_acquisition_flow"** flow
3. 点击该 flow，进入详情页
4. 找到 **"Schedules"** 或 **"Add Schedule"** 按钮
5. 点击 **"Create Schedule"** 或类似按钮
6. 配置如下：
   - **Cron 表达式**: `0 9 15,25,28,29,30,31 * *`
   - **时区**: 选择你的时区（如 Asia/Shanghai）
   - **说明**: "每月 15、25、28-31 号获取汇率数据"
   - **激活**: 勾选启用
7. 点击 **"Save"** 或 **"Create"**

### 步骤 3：为 prepare-batch 设置定时任务

重复步骤 2，但使用以下配置：
- **Flow**: prepare_batch_flow
- **Cron 表达式**: `30 9 15,25,28,29,30,31 * *`
- **说明**: "每月 15、25、28-31 号准备批处理数据"

### 步骤 4：为 process-batch 设置定时任务

重复步骤 2，但使用以下配置：
- **Flow**: process_batch_flow
- **Cron 表达式**: `0 10 15,25,28,29,30,31 * *`
- **说明**: "每月 15、25、28-31 号处理批数据"

---

## 🔍 Cron 表达式详解

```
0 9 15,25,28,29,30,31 * *
│ │ │                  │ │
│ │ │                  │ └─── 周几 (0-6，其中 0=周日，* 表示每天)
│ │ │                  └────── 月份 (1-12，* 表示每个月)
│ │ └──────────────────────── 日期 (1-31)
│ │                           这里指定: 15、25、28、29、30、31
│ └──────────────────────────── 小时 (0-23)
│                              这里是 9 = 09:00
└─────────────────────────────── 分钟 (0-59)
                                这里是 0 = :00
```

### 不同时间的 Cron 表达式

| 需求 | Cron 表达式 |
|------|-----------|
| 每月 15 号 09:00 | `0 9 15 * *` |
| 每月 15、25 号 09:00 | `0 9 15,25 * *` |
| 每月 15-31 号 09:00 | `0 9 15-31 * *` |
| 每月 15、25、28-31 号 09:00 | `0 9 15,25,28-31 * *` |
| 每月 28-31 号 09:00 | `0 9 28-31 * *` |

---

## ⏰ 执行时间安排

建议的执行时间序列：

```
09:00 - currency-acquisition 运行（获取汇率数据）
        ↓
09:30 - prepare-batch 运行（准备批处理数据，使用刚获取的汇率）
        ↓
10:00 - process-batch 运行（处理批数据）
        ↓
10:30 - 所有任务完成
```

这样保证了数据流的先后顺序。

---

## 🧪 测试定时任务

### 方式 1：等待计划时间（不推荐）
- 等到下一个触发时间手动检查

### 方式 2：在 Cloud UI 中手动触发（推荐）
1. 在 Prefect Cloud 中找到相应的 deployment
2. 点击 **"Run"** 或 **"Trigger"** 按钮
3. 检查运行日志和结果

### 方式 3：本地测试后部署
```bash
# 在本地运行 flow 验证逻辑
python -m flows.currency_acquisition_flow
python -m flows.prepare_batch_flow
python -m flows.process_batch_flow

# 然后再在 Cloud 中设置定时任务
```

---

## 📌 注意事项

### 1. 时区问题
- Cron 表达式中的时间是相对于选定时区的
- 确保选择正确的时区（如 Asia/Shanghai、America/New_York 等）
- 不同月份的不同天数会自动处理

### 2. 月末日期处理
- 对于 29、30、31 号，某些月份可能不存在这些日期
- Cron 会自动跳过不存在的日期
- 例如：2 月不存在 29、30、31 号，Cron 会跳过这些月份

### 3. 与 Agent 的关系
- Cron 定时是在 Prefect Cloud 端进行的
- Cloud 会在预定时间创建 flow run
- Run 会在有可用 agent 时执行
- 确保至少有一个 worker/agent 在运行

### 4. Timezone 处理
Prefect 支持的常见时区：
- `UTC` - 协调世界时
- `Asia/Shanghai` - 中国时间
- `America/New_York` - 纽约时间
- `Europe/London` - 伦敦时间
- [更多时区](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)

---

## 📊 检查定时任务状态

1. **在 Cloud UI 中查看**
   - Flows → 选择 flow → Schedules 标签
   - 可以看到所有定时任务及其状态

2. **查看过去的运行**
   - Flows → 选择 flow → Runs 标签
   - 可以看到历史运行记录和日志

3. **检查即将到来的运行**
   - 定时任务配置完成后，Cloud 会显示下一个预计运行时间

---

## 🚀 完整设置后的工作流

```
每月 15、25、28-31 号：

09:00:00 - Cloud 触发 currency-acquisition
           ↓
           从 IMF API 获取汇率数据
           ↓
           保存 exchange_rates_2025_XX.csv
           ✅ 完成 (约 30-60 秒)

09:30:00 - Cloud 触发 prepare-batch
           ↓
           读取刚生成的汇率数据
           ↓
           准备批处理文件
           ↓
           生成 MANIFEST.json
           ✅ 完成 (约 10 秒)

10:00:00 - Cloud 触发 process-batch
           ↓
           读取 MANIFEST.json
           ↓
           处理数据并归档
           ✅ 完成 (约 10 秒)

10:05:00 - 所有任务完成 ✨
```

---

## ❓ 常见问题

### Q1: 定时任务没有执行？
- 检查 Prefect Cloud 的计划是否已启用（enabled）
- 确认是否有 worker/agent 在运行
- 检查时区设置是否正确

### Q2: 如何修改现有定时任务？
- 在 Cloud UI 中找到该 schedule
- 点击编辑按钮
- 修改 cron 表达式或时区
- 保存

### Q3: 如何禁用定时任务？
- 在 Cloud UI 中找到该 schedule
- 点击禁用按钮或删除

### Q4: 能否设置多个不同的定时任务？
- 可以！每个 deployment 可以配置多个不同的 schedule
- 例如：既可以每月执行，也可以手动触发

---

## 📞 需要帮助？

参考 Prefect 官方文档：
- [Schedules and Deployments](https://docs.prefect.io/latest/concepts/deployments/)
- [Cron Syntax](https://en.wikipedia.org/wiki/Cron#Overview)
- [Prefect Cloud UI Guide](https://docs.prefect.io/latest/cloud/cloud-ui/)
