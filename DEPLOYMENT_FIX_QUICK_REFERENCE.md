# Prefect 部署错误修复 - 快速参考

## 问题
```
FileNotFoundError: [Errno 2] No such file or directory: '/opt/prefect/flows/currency_acquisition_flow.py'
```

## 原因
部署配置中 `path: .` 在远程执行时被解释为错误的路径。

## 修复步骤（已完成 ✅）

### 1. 更新配置文件
✅ **prefect.yaml** - 添加了正确的路径：
```yaml
path: c:\Users\yli\Desktop\Prefect_Project
```

### 2. 重新创建部署
✅ 运行脚本删除并重新创建所有3个部署：
- `currency-acquisition`
- `prepare-batch`
- `process-batch`

使用的脚本：`recreate_with_path.py`

### 3. 恢复调度
✅ 为所有部署重新创建了Cron调度

使用的脚本：`recreate_schedules.py`

### 4. 验证配置
✅ 所有检查都通过了

验证脚本：`final_verification.py`

## 最终状态

| 部署 | 路径 | Entrypoint | 工作池 | 调度 |
|------|------|-----------|--------|------|
| currency-acquisition | ✅ | ✅ | ✅ | ✅ |
| prepare-batch | ✅ | ✅ | ✅ | ✅ |
| process-batch | ✅ | ✅ | ✅ | ✅ |

## 测试修复

### 手动触发流程运行（推荐）
```bash
# 方法1：从Prefect Cloud UI
在部署页面点击"运行"

# 方法2：从CLI
prefect deployment run "currency-acquisition"
```

### 检查日志
1. 转到 Prefect Cloud > Flow Runs
2. 找到最新的运行
3. 检查日志确认没有 `FileNotFoundError`

### 自动验证
运行验证脚本：
```bash
python final_verification.py
```

## 相关文件

| 文件 | 用途 |
|------|------|
| [DEPLOYMENT_FIX_SUMMARY.md](DEPLOYMENT_FIX_SUMMARY.md) | 详细的修复说明 |
| [recreate_with_path.py](recreate_with_path.py) | 重新创建部署脚本 |
| [recreate_schedules.py](recreate_schedules.py) | 恢复调度脚本 |
| [final_verification.py](final_verification.py) | 验证脚本 |
| [prefect.yaml](prefect.yaml) | 部署配置 |

## 常见问题

### Q: 仍然看到 FileNotFoundError
**A:** 可能原因：
- 工作池运行在不同的系统/容器上
- Windows路径在Linux工作池上无法访问
- **解决方案**：需要配置拉取步骤或存储块

### Q: 调度没有触发
**A:** 检查：
- Prefect Cloud中的工作池状态
- 工作队列是否正常运行
- 时区设置是否正确

### Q: 需要回滚更改
**A:** 所有更改都在prefect.yaml中，可以：
1. 恢复 `path` 参数为 `.`
2. 删除并重新创建部署
3. 重新创建调度

---
✅ **修复状态：完成**  
📅 **修复日期：2025年12月17日**
