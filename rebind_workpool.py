"""
修复 Deployment，重新绑定 Work Pool
"""
import asyncio
from prefect.client.orchestration import get_client


async def fix_deployments_work_pool():
    """删除旧 Deployment 并重新创建"""
    client = get_client()
    
    print("=" * 70)
    print("Recreating Deployments with Work Pool")
    print("=" * 70)
    print()
    
    # 先删除所有旧 Deployment
    print("Deleting old deployments...")
    deployments = await client.read_deployments()
    
    for dep in deployments:
        try:
            await client.delete_deployment(dep.id)
            print(f"  ✅ Deleted {dep.name}")
        except Exception as e:
            print(f"  ❌ {dep.name}: {str(e)}")
    
    print()
    print("Creating new deployments...")
    
    # 获取所有 Flow
    flows = await client.read_flows()
    flow_map = {flow.name: flow.id for flow in flows}
    
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
    
    # 创建新 Deployment
    for config in deployments_config:
        try:
            flow_id = flow_map.get(config["flow_name"])
            if not flow_id:
                print(f"  ❌ Flow not found: {config['flow_name']}")
                continue
            
            dep = await client.create_deployment(
                name=config["name"],
                flow_id=flow_id,
                work_pool_name="Yichen_Test",
                description=config["description"],
                entrypoint=config["entrypoint"],
                path="."
            )
            
            print(f"  ✅ {config['name']}")
            
        except Exception as e:
            print(f"  ❌ {config['name']}: {str(e)}")
    
    print()
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(fix_deployments_work_pool())
