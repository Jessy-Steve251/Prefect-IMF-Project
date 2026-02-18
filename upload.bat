@echo off
chcp 65001 >nul
REM UTF-8 编码支持

echo ================================
echo 检查 Git 安装状态
echo ================================
echo.

REM 检查 git 是否在 PATH 中
where git >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    echo ✅ Git 已安装在 PATH 中
    git --version
    echo.
    goto :start_upload
)

REM 检查常见的 Git 安装位置
echo ❌ Git 在 PATH 中未找到，检查常见安装位置...
echo.

set "GIT_PATH="
for %%i in (
    "C:\Program Files\Git\bin\git.exe"
    "C:\Program Files (x86)\Git\bin\git.exe"
    "C:\Users\%USERNAME%\AppData\Local\Programs\Git\bin\git.exe"
) do (
    if exist %%i (
        set "GIT_PATH=%%i"
        echo ✅ 找到 Git: %%i
        set "PATH=%PATH%;C:\Program Files\Git\bin"
        "%%i" --version
        echo.
        goto :start_upload
    )
)

REM Git 未找到
echo.
echo ❌ 未找到 Git 安装
echo.
echo 请按照以下步骤安装 Git：
echo.
echo 1. 访问: https://git-scm.com/download/win
echo 2. 下载最新版本的 Git for Windows
echo 3. 运行安装程序，选择以下选项：
echo    - Use Git from Windows Command Prompt
echo    - 其他选项保持默认
echo 4. 安装完成后，重启此批处理文件
echo.
pause
exit /b 1

:start_upload
echo ================================
echo 开始上传代码到 GitHub
echo ================================
echo.

REM 步骤 1: 初始化
echo 步骤 1: 初始化 Git 仓库...
git init
echo ✓ 完成
echo.

REM 步骤 2: 配置用户
echo 步骤 2: 配置用户信息...
git config user.name "forg1ve1125"
git config user.email "no-reply@github.com"
echo ✓ 完成
echo.

REM 步骤 3: 添加文件
echo 步骤 3: 添加所有文件...
git add .
echo ✓ 完成
echo.

REM 步骤 4: 提交
echo 步骤 4: 提交代码...
git commit -m "Initial Prefect deployment"
echo ✓ 完成
echo.

REM 步骤 5: 远程仓库
echo 步骤 5: 配置远程仓库...
git remote add origin https://github.com/forg1ve1125/Prefect_Project.git
echo ✓ 完成
echo.

REM 步骤 6: 重命名分支
echo 步骤 6: 重命名分支...
git branch -M main
echo ✓ 完成
echo.

REM 步骤 7: 推送
echo 步骤 7: 推送代码到 GitHub...
echo ⚠️  会弹出登录窗口，输入你的 GitHub 用户名和密码
echo.
git push -u origin main

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ================================
    echo ✅ 上传成功！
    echo ================================
    echo.
    echo 代码已推送到:
    echo https://github.com/forg1ve1125/Prefect_Project
    echo.
    echo 下一步:
    echo 1. 去 Prefect Cloud 添加计划 (schedules)
    echo 2. 每月 17 日自动运行
    echo.
) else (
    echo.
    echo ❌ 推送失败
    echo 请检查网络连接或 GitHub 凭证
    echo.
)

pause
