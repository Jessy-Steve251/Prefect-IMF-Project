# GitHub 部署方案 - 快速设置指南

## 第1步：准备 GitHub 仓库

### 选项 A：已有 GitHub 仓库
如果你已经有 GitHub 仓库，跳到第2步

### 选项 B：创建新的 GitHub 仓库
1. 访问 https://github.com/new
2. 创建仓库（可以是公开或私开）
3. 记下仓库 URL，例如：
   ```
   https://github.com/username/Prefect_Project.git
   ```

---

## 第2步：上传代码到 GitHub

在项目目录执行：

```bash
# 初始化 git（如果还没有）
git init

# 添加所有文件
git add .

# 提交代码
git commit -m "Initial commit: Prefect flows and utilities"

# 添加远程仓库（替换为你的 GitHub URL）
git remote add origin https://github.com/USERNAME/Prefect_Project.git

# 推送到 main 分支
git branch -M main
git push -u origin main
```

---

## 第3步：更新 prefect.yaml

编辑 `prefect.yaml`，替换所有的 `USERNAME` 为你的 GitHub 用户名：

```yaml
pull_steps:
  - type: git_clone
    repository: "https://github.com/YOUR_USERNAME/Prefect_Project.git"
    branch: "main"
```

### 完整示例：
```yaml
deployments:
  - name: currency-acquisition
    description: Acquire currency exchange rate data
    flow: currency_acquisition_flow
    entrypoint: flows/currency_acquisition_flow.py:currency_acquisition_flow
    path: .
    work_pool:
      name: Yichen_Test
    pull_steps:
      - type: git_clone
        repository: "https://github.com/kappa6/Prefect_Project.git"
        branch: "main"
    schedules:
      - cron: "10 12 17 * *"
        timezone: Europe/Zurich
        active: true
```

---

## 第4步：重新创建部署

执行以下命令更新 Prefect Cloud 中的部署：

```bash
python recreate_with_github.py
```

这个脚本会：
1. 删除旧部署
2. 使用新的 GitHub pull_steps 重新创建部署
3. 恢复所有调度

---

## 工作原理

当调度触发时，工作流如下：

```
Prefect Cloud 容器
  ↓
[执行 pull_steps]
  ↓
git clone https://github.com/USERNAME/Prefect_Project.git
  ↓
下载最新代码到容器
  ↓
执行流程
  ↓
[成功！]
```

---

## 需要 GitHub 个人访问令牌吗？

- ✅ 公开仓库：**不需要**，可以直接克隆
- 🔐 私开仓库：需要配置访问令牌（更复杂）

**建议：使用公开仓库**（快速简单）

---

## 故障排除

### 问题：pull_steps 未执行
**检查项：**
- GitHub URL 是否正确
- 仓库是否公开
- 分支名是否正确（main 或 master）

### 问题：权限错误
**如果使用私开仓库，需要配置认证**
```yaml
pull_steps:
  - type: git_clone
    repository: "https://github.com/USERNAME/Prefect_Project.git"
    branch: "main"
    credentials:
      token: "{{ prefect.blocks.github_credentials.my_token }}"
```

---

## 下一步

1. ✅ 创建/选择 GitHub 仓库
2. ✅ 上传代码
3. ✅ 更新 prefect.yaml
4. ✅ 运行 `python recreate_with_github.py`
5. ✅ 测试运行一个部署

---

**需要帮助？告诉我你的 GitHub 用户名，我可以帮你更新配置文件！**
