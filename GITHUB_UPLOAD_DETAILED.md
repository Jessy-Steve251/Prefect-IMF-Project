# 完整步骤：上传代码到 GitHub

## 📋 快速步骤 (推荐)

### 方案 A：使用批处理脚本 (最简单)

1. **打开文件浏览器**
   - 进入 `C:\Users\yli\Desktop\Prefect_Project`

2. **双击 `upload.bat` 文件**
   - 脚本会自动执行所有命令
   - 会弹出 GitHub 登录窗口
   - 输入用户名和密码

3. **完成！**

---

### 方案 B：手动逐步执行 (适合学习)

#### 前置要求
- ✅ 已安装 Git (如果没有，访问 https://git-scm.com/download/win)

#### 步骤

**1. 打开 PowerShell**
- 按 `Win + R`
- 输入 `powershell` 后按 Enter
- 或在文件夹中右键选择 "在终端中打开"

**2. 进入项目目录**
```powershell
cd C:\Users\yli\Desktop\Prefect_Project
```

**3. 初始化 Git 仓库**
```powershell
git init
```

**4. 配置用户信息**
```powershell
git config user.name "forg1ve1125"
git config user.email "no-reply@github.com"
```

**5. 添加所有文件**
```powershell
git add .
```

验证文件:
```powershell
git status
```

**6. 提交代码**
```powershell
git commit -m "Initial Prefect deployment"
```

**7. 创建 GitHub 仓库**
- 打开浏览器: https://github.com/new
- 填写:
  - Repository name: **Prefect_Project**
  - Public (选择公开)
  - 点击 "Create repository"

**8. 添加远程仓库**
```powershell
git remote add origin https://github.com/forg1ve1125/Prefect_Project.git
```

**9. 重命名分支**
```powershell
git branch -M main
```

**10. 推送代码**
```powershell
git push -u origin main
```

**输入凭证:**
- 用户名: `forg1ve1125`
- 密码: 你的 GitHub 密码或 Personal Access Token

---

## 🔑 GitHub 认证问题？

如果提示 "Authentication failed":

### 使用 Personal Access Token (推荐)

1. 访问: https://github.com/settings/tokens
2. 点击 "Generate new token" → "Generate new token (classic)"
3. 设置:
   - Token name: `Prefect_Deployment`
   - Expiration: 90 days (或更长)
   - Scopes: 勾选 `repo` (所有 repo 权限)
4. 点击 "Generate token"
5. **复制 token** (只会显示一次!)
6. 推送时用 token 代替密码

### 使用 SSH 密钥 (更安全)

参考: https://docs.github.com/en/authentication/connecting-to-github-with-ssh

---

## ✅ 验证上传成功

1. 打开浏览器访问:
   ```
   https://github.com/forg1ve1125/Prefect_Project
   ```

2. 查看:
   - ✅ 所有文件都已上传
   - ✅ 提交历史显示 "Initial Prefect deployment"
   - ✅ 分支是 "main"

---

## 🚀 后续步骤

### 1. 在 Prefect Cloud 中添加计划

访问: https://app.prefect.cloud

为每个部署添加计划:

| 部署名称 | 时间 | Cron 表达式 |
|---------|------|-----------|
| currency-acquisition | 每月 17 日 12:10 | `10 12 17 * *` |
| prepare-batch | 每月 17 日 12:30 | `30 12 17 * *` |
| process-batch | 每月 17 日 13:00 | `0 13 17 * *` |

### 2. 测试

手动触发一个流来测试:
1. 在 Prefect Cloud 中找到一个部署
2. 点击 "Run" 或 "Trigger run"
3. 查看日志确认代码从 GitHub 拉取成功

### 3. 定期更新

如果需要更新代码:
```powershell
git add .
git commit -m "描述你的改动"
git push
```

---

## 🆘 常见问题

**Q: 提示 "fatal: not a git repository"**

A: 确认你在项目目录:
```powershell
pwd  # 应该显示 C:\Users\yli\Desktop\Prefect_Project
```

**Q: 提示 "error: failed to push some refs"**

A: 可能是分支不一致，运行:
```powershell
git pull origin main --allow-unrelated-histories
git push -u origin main
```

**Q: 推送后仓库仍为空**

A: 可能是登录失败，检查:
```powershell
git config --list
git log  # 查看提交历史
git remote -v  # 查看远程仓库
```

**Q: 需要重新初始化**

A: 备份后删除 `.git` 文件夹，重新开始:
```powershell
Remove-Item -Recurse .git
git init
# ... 重复上述步骤
```

---

## 💡 提示

- 第一次推送会自动创建 GitHub 的工作流配置
- 推送后 Prefect 会自动从 GitHub 拉取代码
- 无需在 Prefect Cloud 中手动更新代码路径
- 所有功能都通过 GitHub 自动同步

---

**需要帮助？** 查看完整文档:
- Git 官方文档: https://git-scm.com/doc
- GitHub 指南: https://guides.github.com/
- Prefect 部署文档: https://docs.prefect.io/latest/concepts/deployments/
