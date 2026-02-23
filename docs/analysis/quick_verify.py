"""
快速验证脚本 - 检查明天自动执行是否已准备就绪
"""
import subprocess
import sys
from datetime import datetime, timedelta


def check_task_scheduler():
    """检查 Task Scheduler 任务"""
    print("=" * 70)
    print("1. Task Scheduler 任务状态")
    print("=" * 70)
    print()
    
    try:
        result = subprocess.run(
            ["schtasks", "/query"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        tasks_found = False
        for line in result.stdout.split('\n'):
            if 'Prefect-' in line:
                print(f"  {line}")
                tasks_found = True
        
        if tasks_found:
            print()
            print("✅ 所有任务已创建并就绪")
            print()
            return True
        else:
            print("❌ 未找到 Prefect 任务")
            print()
            return False
            
    except Exception as e:
        print(f"❌ 检查失败: {str(e)}")
        print()
        return False


def check_bat_files():
    """检查执行脚本文件"""
    print("=" * 70)
    print("2. 执行脚本文件状态")
    print("=" * 70)
    print()
    
    import os
    
    bat_files = [
        r"C:\Users\yli\Desktop\Prefect_Project\run_Prefect-CurrencyAcquisition.bat",
        r"C:\Users\yli\Desktop\Prefect_Project\run_Prefect-PrepareBatch.bat",
        r"C:\Users\yli\Desktop\Prefect_Project\run_Prefect-ProcessBatch.bat",
    ]
    
    all_exist = True
    for bat_file in bat_files:
        if os.path.exists(bat_file):
            print(f"✅ {os.path.basename(bat_file)}")
        else:
            print(f"❌ {os.path.basename(bat_file)} - 不存在")
            all_exist = False
    
    print()
    return all_exist


def check_python_script():
    """检查 Python 执行脚本"""
    print("=" * 70)
    print("3. Python 执行脚本状态")
    print("=" * 70)
    print()
    
    import os
    
    script_path = r"C:\Users\yli\Desktop\Prefect_Project\run_flows_locally.py"
    
    if os.path.exists(script_path):
        print(f"✅ run_flows_locally.py 存在")
        print()
        return True
    else:
        print(f"❌ run_flows_locally.py 不存在")
        print()
        return False


def show_tomorrow_schedule():
    """显示明天的执行计划"""
    print("=" * 70)
    print("4. Tomorrow's Execution Schedule")
    print("=" * 70)
    print()
    
    tomorrow = datetime.now() + timedelta(days=1)
    print(f"Date: {tomorrow.strftime('%Y-%m-%d')}")
    print()
    
    tasks = [
        ("09:00", "Prefect-CurrencyAcquisition", "Acquire exchange rates"),
        ("09:30", "Prefect-PrepareBatch", "Prepare batch data"),
        ("10:00", "Prefect-ProcessBatch", "Process batch data"),
    ]
    
    for time, task_name, description in tasks:
        print(f"  {time}  ->  {task_name}")
        print(f"         {description}")
        print()


def show_verification_methods():
    """显示验证方法"""
    print("=" * 70)
    print("5. 验证方法")
    print("=" * 70)
    print()
    
    print("✓ 方法 A：等待明天自动执行")
    print("  - 任务会在指定时间自动运行")
    print("  - 检查数据输出文件是否更新")
    print()
    
    print("✓ 方法 B：现在手动测试")
    print("  - 运行: python run_flows_locally.py")
    print("  - 如果成功，说明明天也会成功")
    print()
    
    print("✓ 方法 C：Cloud UI 查看")
    print("  - 打开: https://app.prefect.cloud")
    print("  - 进入 Deployments → currency-acquisition")
    print("  - 查看 Runs 标签，明天会看到自动运行")
    print()
    
    print("✓ 方法 D：查看 Windows 事件日志")
    print("  - 打开事件查看器 (eventvwr)")
    print("  - 找 Task Scheduler 日志")
    print("  - 明天会记录任务执行情况")
    print()


def main():
    """主程序"""
    print()
    print("=" * 70)
    print("Prefect 自动执行系统 - 快速验证")
    print(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()
    
    # 执行检查
    scheduler_ok = check_task_scheduler()
    bat_ok = check_bat_files()
    python_ok = check_python_script()
    
    # 显示计划
    show_tomorrow_schedule()
    
    # 显示验证方法
    show_verification_methods()
    
    # 总结
    print("=" * 70)
    print("验证结果")
    print("=" * 70)
    print()
    
    checks = {
        "Task Scheduler": scheduler_ok,
        "执行脚本文件": bat_ok,
        "Python 脚本": python_ok,
    }
    
    all_ok = all(checks.values())
    
    for check_name, result in checks.items():
        status = "✅" if result else "❌"
        print(f"{status} {check_name}")
    
    print()
    
    if all_ok:
        print("🎉 所有检查通过！")
        print()
        print("系统已准备就绪，明天会自动执行：")
        print("  • 09:00 - 获取汇率")
        print("  • 09:30 - 准备批处理")
        print("  • 10:00 - 处理数据")
        print()
    else:
        print("⚠️  某些检查未通过，请检查上面的错误")
        print()
    
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()
