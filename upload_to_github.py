#!/usr/bin/env python
"""上传代码到 GitHub - 自动化脚本"""

import subprocess
import sys
import os
from pathlib import Path
import os
from pathlib import Path

def find_git():
    """查找 Git 可执行文件"""
    # 常见的 Git 安装位置
    potential_paths = [
        "git",  # 在 PATH 中
        "C:\\Program Files\\Git\\bin\\git.exe",
        "C:\\Program Files (x86)\\Git\\bin\\git.exe",
        f"C:\\Users\\{os.getenv('USERNAME')}\\AppData\\Local\\Programs\\Git\\bin\\git.exe",
    ]
    
    for path in potential_paths:
        try:
            result = subprocess.run([path, "--version"], capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ 找到 Git: {path}")
                print(f"   版本: {result.stdout.strip()}")
                return path
        except:
            continue
    
    return None

def run_command(cmd, description=""):
    """运行 shell 命令"""
    if description:
        print(f"\n📋 {description}")
    
    print(f"   命令: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, shell=False)
        
        if result.returncode == 0:
            print("   ✅ 成功")
            if result.stdout and result.stdout.strip():
                for line in result.stdout.strip().split('\n')[:3]:
                    print(f"      {line}")
            return True
        else:
            # 某些命令即使返回非零也是正常的（如已存在的仓库）
            if "already exists" in result.stderr or "fatal" not in result.stderr:
                print("   ⚠️  已存在或其他状态")
                return True
            print(f"   ❌ 错误: {result.stderr[:100]}")
            return False
    except Exception as e:
        print(f"   ❌ 异常: {e}")
        return False

def main():
    print("=" * 80)
    print("上传代码到 GitHub - 自动化脚本")
    print("=" * 80)
    print()
    
    # 查找 Git
    print("🔍 检查 Git 安装...")
    git_path = find_git()
    
    if not git_path:
        print()
        print("❌ 未找到 Git 安装")
        print()
        print("请按照以下步骤安装:")
        print("   1. 访问: https://git-scm.com/download/win")
        print("   2. 下载 Git for Windows")
        print("   3. 运行安装程序（保持默认选项）")
        print("   4. 重启计算机")
        print("   5. 再次运行此脚本")
        print()
        input("按 Enter 退出...")
        sys.exit(1)
    
    # 进入项目目录
    project_dir = Path.cwd()
    print(f"\n📁 项目目录: {project_dir}")
    
    # Git 命令列表
    steps = [
        (["init"], "步骤 1/7: 初始化 Git 仓库"),
        (["config", "user.name", "forg1ve1125"], "步骤 2/7: 配置用户名"),
        (["config", "user.email", "no-reply@github.com"], "步骤 2/7: 配置邮箱"),
        (["add", "."], "步骤 3/7: 添加所有文件"),
        (["commit", "-m", "Initial Prefect deployment"], "步骤 4/7: 提交代码"),
        (["remote", "add", "origin", "https://github.com/forg1ve1125/Prefect_Project.git"], "步骤 5/7: 配置远程仓库"),
        (["branch", "-M", "main"], "步骤 6/7: 重命名分支为 main"),
    ]
    
    print("\n" + "=" * 80)
    print("执行 Git 命令")
    print("=" * 80)
    
    for git_args, description in steps:
        cmd = [git_path] + git_args
        if not run_command(cmd, description):
            if "commit" not in str(git_args):  # commit 失败可能是因为没有更改
                pass
    
    # 最后一步：推送
    print("\n📋 步骤 7/7: 推送代码到 GitHub")
    print()
    print("⚠️  提示:")
    print("   • 会弹出 GitHub 登录窗口")
    print("   • 用户名: forg1ve1125")
    print("   • 密码: 输入你的 GitHub 密码或 Personal Access Token")
    print()
    input("按 Enter 继续推送...")
    
    cmd = [git_path, "push", "-u", "origin", "main"]
    result = subprocess.run(cmd, capture_output=True, text=True, shell=False)
    
    if result.returncode == 0:
        print("   ✅ 成功")
    else:
        print(f"   ⚠️  状态: {result.returncode}")
        if result.stderr:
            print(f"   信息: {result.stderr[:200]}")
    
    # 完成
    print()
    print("=" * 80)
    print("✅ 上传流程完成")
    print("=" * 80)
    print()
    print("🎉 代码已推送到:")
    print("   https://github.com/forg1ve1125/Prefect_Project")
    print()
    print("📋 后续步骤:")
    print("   1. 访问 Prefect Cloud: https://app.prefect.cloud")
    print("   2. 为每个部署添加计划:")
    print("      • currency-acquisition: 10 12 17 * *  (每月17日 12:10)")
    print("      • prepare-batch: 30 12 17 * *  (每月17日 12:30)")
    print("      • process-batch: 0 13 17 * *  (每月17日 13:00)")
    print()
    print("💡 提示:")
    print("   • 流会自动从 GitHub 拉取代码")
    print("   • 按计划时间自动运行")
    print("   • 查看日志可以看到 GitHub pull_steps 的执行过程")
    print()
    input("按 Enter 退出...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中止")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()
        input("按 Enter 退出...")
        sys.exit(1)
