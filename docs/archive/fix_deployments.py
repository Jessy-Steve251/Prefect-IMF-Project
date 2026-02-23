"""
使用 API 重新创建 Deployment，带上 entrypoint，不需要交互
"""
import asyncio
from prefect.client.orchestration import get_client


async def recreate_deployments_with_entrypoint():
    """重新创建 Deployment，这次带上 entrypoint"""
    client = get_client()
    
    print("=" * 70)
    print("Recreating Deployments with Entrypoints")
    print("=" * 70)
    print()
    
    # 获取现有 Deployment
    deployments = await client.read_deployments()
    
    print(f"Found {len(deployments)} existing deployments")
    print()
    
    # 定义新的 Deployment 配置
    deployments_config = [
        {
            "name": "currency-acquisition",
            "flow_name": "currency_acquisition_flow",
            "entrypoint": "flows/currency_acquisition_flow.py:currency_acquisition_flow",
            "description": "Acquire currency exchange rate data"
        },
        {
            "name": "prepare-batch",
            "flow_name": "prepare_batch_flow",
            "entrypoint": "flows/prepare_batch_flow.py:prepare_batch_flow",
            "description": "Prepare batch data"
        },
        {
            "name": "process-batch",
            "flow_name": "process_batch_flow",
            "entrypoint": "flows/process_batch_flow.py:process_batch_flow",
            "description": "Process batch data"
        }
    ]
    
    # 获取所有 Flow
    flows = await client.read_flows()
    flow_map = {flow.name: flow.id for flow in flows}
    
    print("Creating new Deployments with entrypoints...\n")
    
    for dep_config in deployments_config:
        try:
            flow_name = dep_config["flow_name"]
            
            if flow_name not in flow_map:
                print(f"❌ Flow not found: {flow_name}")
                continue
            
            flow_id = flow_map[flow_name]
            
            # 创建 Deployment，带上 entrypoint
            # 注意：entrypoint 是必需的
            deployment_data = {
                "name": dep_config["name"],
                "flow_id": flow_id,
                "work_pool_name": "Yichen_Test",
                "description": dep_config["description"],
                "path": ".",  # Working directory path
                "entrypoint": dep_config["entrypoint"],
            }
            
            result = await client.create_deployment(**deployment_data)
            
            print(f"✅ {dep_config['name']}")
            print(f"   Flow: {flow_name}")
            print(f"   Entrypoint: {dep_config['entrypoint']}")
            print(f"   Work Pool: Yichen_Test")
            print()
            
        except Exception as e:
            error_msg = str(e)
            if "already exists" in error_msg:
                print(f"⚠️  {dep_config['name']} already exists (skipping)")
            else:
                print(f"❌ Error creating {dep_config['name']}: {error_msg}")
            print()


async def recreate_schedules():
    """为新 Deployment 创建 Schedule"""
    client = get_client()
    
    print("=" * 70)
    print("Creating Schedules")
    print("=" * 70)
    print()
    
    schedules_config = [
        {
            "deployment_name": "currency-acquisition",
            "cron": "0 11 11 * *",
            "timezone": "Europe/Zurich",
            "description": "Fetch exchange rates"
        },
        {
            "deployment_name": "prepare-batch",
            "cron": "30 11 11 * *",
            "timezone": "Europe/Zurich",
            "description": "Prepare batch data"
        },
        {
            "deployment_name": "process-batch",
            "cron": "0 12 11 * *",
            "timezone": "Europe/Zurich",
            "description": "Process batch data"
        }
    ]
    
    # 获取所有 Deployment
    deployments = await client.read_deployments()
    deployment_map = {dep.name: dep.id for dep in deployments}
    
    for schedule_config in schedules_config:
        dep_name = schedule_config["deployment_name"]
        
        if dep_name not in deployment_map:
            print(f"❌ Deployment not found: {dep_name}")
            continue
        
        try:
            dep_id = deployment_map[dep_name]
            
            # 创建 Schedule
            await client.create_deployment_schedules(
                deployment_id=dep_id,
                schedules=[{
                    "cron": schedule_config["cron"],
                    "timezone": schedule_config["timezone"],
                    "active": True,
                    "description": schedule_config["description"]
                }]
            )
            
            print(f"✅ Schedule created for {dep_name}")
            print(f"   Cron: {schedule_config['cron']}")
            print(f"   Timezone: {schedule_config['timezone']}")
            print()
            
        except Exception as e:
            print(f"❌ Error creating schedule for {dep_name}: {str(e)}")
            print()


async def main():
    """主程序"""
    await recreate_deployments_with_entrypoint()
    print()
    await recreate_schedules()
    
    print("=" * 70)
    print("✅ All Deployments and Schedules created!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
