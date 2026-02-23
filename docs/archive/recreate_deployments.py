"""
重新创建 Deployment，带上 entrypoint
"""
import asyncio
from prefect.client.orchestration import get_client


async def recreate_deployments():
    """重新创建所有 Deployment，这次带上 entrypoint"""
    client = get_client()
    
    print("=" * 70)
    print("Recreating Deployments with Entrypoints")
    print("=" * 70)
    print()
    
    # 首先获取所有现有的 Deployment，然后删除它们
    deployments = await client.read_deployments()
    deployment_map = {dep.name: dep for dep in deployments}
    
    print("Existing Deployments:")
    for dep_name in deployment_map:
        print(f"  - {dep_name}")
    print()
    
    # 定义新的 Deployment 配置，带上 entrypoint
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
    
    print("Creating new Deployments with entrypoints...\n")
    
    for dep_config in deployments_config:
        try:
            # Get the flow
            flows = await client.read_flows()
            flow_map = {flow.name: flow.id for flow in flows}
            
            flow_name = dep_config["flow_name"]
            
            if flow_name not in flow_map:
                print(f"❌ Flow not found: {flow_name}")
                continue
            
            flow_id = flow_map[flow_name]
            
            # Create deployment with entrypoint
            deployment_data = {
                "name": dep_config["name"],
                "flow_id": flow_id,
                "work_pool_name": "Yichen_Test",
                "description": dep_config["description"],
                "entrypoint": dep_config["entrypoint"],
            }
            
            result = await client.create_deployment(**deployment_data)
            
            print(f"✅ {dep_config['name']}")
            print(f"   Entrypoint: {dep_config['entrypoint']}")
            print()
            
        except Exception as e:
            print(f"❌ Error creating {dep_config['name']}: {str(e)}")
            print()


if __name__ == "__main__":
    asyncio.run(recreate_deployments())
    print("=" * 70)
    print("✅ Deployments recreated with entrypoints!")
    print("=" * 70)
