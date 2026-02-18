import asyncio
from prefect import Flow
from prefect.client.orchestration import get_client
from prefect.utilities.asyncutils import run_coro_as_sync

def create_deployments_sync():
    async def _create():
        client = get_client()
        
        # Get ALL Flow
        flows = await client.read_flows()
        
        flow_map = {flow.name: flow.id for flow in flows}
        
        print("Found Flows:")
        for flow_name in flow_map:
            print(f"  - {flow_name}")
        
        # Define Deployments to create
        deployments_config = [
            {
                "name": "currency-acquisition",
                "flow_name": "currency_acquisition_flow",
                "description": "Acquire currency exchange rate data"
            },
            {
                "name": "prepare-batch",
                "flow_name": "prepare_batch_flow",
                "description": "Prepare batch data"
            },
            {
                "name": "process-batch",
                "flow_name": "process_batch_flow",
                "description": "Process batch data"
            }
        ]
        
        for dep_config in deployments_config:
            flow_name = dep_config["flow_name"]
            if flow_name not in flow_map:
                print(f"❌ Flow not found: {flow_name}")
                continue
                
            flow_id = flow_map[flow_name]
            
            # Create Deployment - using dictionary format
            deployment_data = {
                "name": dep_config["name"],
                "flow_id": flow_id,
                "work_pool_name": "Yichen_Test",
                "description": dep_config["description"]
            }
            
            result = await client.create_deployment(**deployment_data)
            print(f"✅ {dep_config['name']} Deployment has been created")
    
    return asyncio.run(_create())

if __name__ == "__main__":
    create_deployments_sync()
    print("\n✅ All Deployments have been created!")
