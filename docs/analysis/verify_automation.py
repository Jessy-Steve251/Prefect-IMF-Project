"""
验证 Prefect 自动执行系统的脚本
检查：
1. Deployment 是否正确配置
2. Schedule 是否激活
3. 测试本地执行
4. 验证 Cloud 连接
"""
import asyncio
from prefect.client.orchestration import get_client
from datetime import datetime
import subprocess
import time


async def check_deployments():
    """检查 Deployment 配置"""
    print("=" * 70)
    print("1. 检查 Deployment 配置")
    print("=" * 70)
    print()
    
    client = get_client()
    deployments = await client.read_deployments()
    
    all_ready = True
    for dep in deployments:
        status = "✅" if dep.status.value == "READY" else "❌"
        print(f"{status} {dep.name}")
        print(f"   ID: {dep.id}")
        print(f"   Work Pool: {dep.work_pool_name}")
        print(f"   Entrypoint: {dep.entrypoint}")
        print(f"   Status: {dep.status.value}")
        
        if dep.status.value != "READY":
            all_ready = False
        print()
    
    return all_ready


async def check_schedules():
    """检查 Schedule 配置"""
    print("=" * 70)
    print("2. 检查 Schedule 配置")
    print("=" * 70)
    print()
    
    client = get_client()
    deployments = await client.read_deployments()
    
    all_active = True
    for dep in deployments:
        schedules = await client.read_deployment_schedules(dep.id)
        
        if not schedules:
            print(f"❌ {dep.name}: 没有 Schedule")
            all_active = False
            continue
        
        for schedule in schedules:
            status = "✅" if schedule.active else "❌"
            print(f"{status} {dep.name}")
            # 显示 Schedule 的主要属性
            print(f"   ID: {schedule.id}")
            print(f"   Active: {schedule.active}")
            print(f"   Created: {schedule.created}")
            
            if not schedule.active:
                all_active = False
            print()
    
    return all_active


async def check_cloud_connection():
    """检查 Cloud 连接"""
    print("=" * 70)
    print("3. 检查 Cloud 连接")
    print("=" * 70)
    print()
    
    try:
        client = get_client()
        # 获取当前账户信息
        workspace = await client.read_workspace()
        
        print(f"✅ 已连接到 Prefect Cloud")
        print(f"   Workspace: {workspace.name if hasattr(workspace, 'name') else 'Default'}")
        print(f"   API Server: {client.api_url}")
        print()
        
        return True
    except Exception as e:
        print(f"❌ Cloud 连接失败: {str(e)}")
        print()
        return False


def test_local_execution():
    """测试本地执行"""
    print("=" * 70)
    print("4. 测试本地执行")
    print("=" * 70)
    print()
    
    print("运行 run_flows_locally.py（这会执行所有 3 个 Flow）...")
    print()
    
    try:
        result = subprocess.run(
            ["python", "run_flows_locally.py"],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        # 检查是否成功
        if result.returncode == 0 and "Finished in state Completed()" in result.stdout:
            print("✅ 本地执行成功")
            print()
            return True
        else:
            print("❌ 本地执行失败")
            print(f"输出: {result.stdout[-500:]}")
            print()
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ 执行超时")
        print()
        return False
    except Exception as e:
        print(f"❌ 执行错误: {str(e)}")
        print()
        return False


def check_task_scheduler():
    """检查 Windows Task Scheduler 任务"""
    print("=" * 70)
    print("5. 检查 Windows Task Scheduler 任务")
    print("=" * 70)
    print()
    
    try:
        result = subprocess.run(
            ["powershell", "-Command", 
             "Get-ScheduledTask -TaskName 'Prefect-*' | Select-Object TaskName, State"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            output = result.stdout
            if "Prefect-" in output:
                print("✅ Task Scheduler 任务已创建")
                print()
                print(output)
                return True
            else:
                print("❌ 未找到 Prefect Task Scheduler 任务")
                print()
                print("需要先运行: powershell -File setup_task_scheduler.ps1")
                print()
                return False
        else:
            print("❌ 无法读取 Task Scheduler")
            print()
            return False
            
    except Exception as e:
        print(f"❌ 检查失败: {str(e)}")
        print()
        return False


def show_verification_guide():
    """显示验证指南"""
    print("=" * 70)
    print("验证明天 11:00 自动执行的方法")
    print("=" * 70)
    print()
    
    print("方法 1：Cloud UI 监控（推荐）")
    print("-" * 70)
    print("访问 https://app.prefect.cloud")
    print()
    print("在 Dashboard 中：")
    print("  1. 找到 'Deployments' 菜单")
    print("  2. 选择 'currency-acquisition' Deployment")
    print("  3. 查看 'Runs' 标签页")
    print("  4. 明天 11:00 UTC (Europe/Zurich 时区)")
    print("     应该看到自动触发的 Flow Run")
    print()
    print()
    
    print("方法 2：本地检查（立即验证）")
    print("-" * 70)
    print("运行以下命令检查配置：")
    print()
    print("  python check_status.py")
    print()
    print("或检查 Task Scheduler：")
    print()
    print("  Get-ScheduledTask -TaskName 'Prefect-*'")
    print()
    print()
    
    print("方法 3：强制执行（模拟明天 11:00）")
    print("-" * 70)
    print("立即手动运行所有 Flow，以确保它们能成功执行：")
    print()
    print("  python run_flows_locally.py")
    print()
    print("如果这个成功了，说明明天 11:00 也会成功！")
    print()
    print()
    
    print("方法 4：设置 Task Scheduler（如果还没设置）")
    print("-" * 70)
    print("如果你还没运行 setup_task_scheduler.ps1，现在运行：")
    print()
    print("  powershell -File setup_task_scheduler.ps1")
    print()
    print("这会在 Windows Task Scheduler 中创建自动任务")
    print()


async def main():
    """主程序"""
    print()
    print("=" * 70)
    print("Prefect 自动执行系统验证")
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()
    
    # 执行检查
    deployment_ok = await check_deployments()
    schedule_ok = await check_schedules()
    cloud_ok = await check_cloud_connection()
    local_ok = test_local_execution()
    scheduler_ok = check_task_scheduler()
    
    # 总结
    print("=" * 70)
    print("验证总结")
    print("=" * 70)
    print()
    
    checks = {
        "Deployment 配置": deployment_ok,
        "Schedule 配置": schedule_ok,
        "Cloud 连接": cloud_ok,
        "本地执行": local_ok,
        "Task Scheduler": scheduler_ok
    }
    
    for check_name, result in checks.items():
        status = "✅" if result else "⚠️ "
        print(f"{status} {check_name}")
    
    print()
    
    # 最终建议
    all_ok = all(checks.values())
    
    if all_ok:
        print("✅ 所有检查通过！系统已准备好自动执行")
        print()
        print("明天 11:00 时，Flow 会自动执行")
    else:
        print("⚠️  某些检查未通过，请查看上面的信息进行修复")
        print()
        if not scheduler_ok:
            print("需要运行: powershell -File setup_task_scheduler.ps1")
    
    print()
    show_verification_guide()


if __name__ == "__main__":
    asyncio.run(main())
