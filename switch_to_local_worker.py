"""
Delete Yichen_Test work pool and recreate deployments with local work pool
"""

import asyncio
from prefect.client.orchestration import get_client


async def switch_to_local_worker():
    """Switch deployments from managed to local worker"""
    
    client = get_client()
    
    print("=" * 80)
    print("SWITCHING TO LOCAL WORKER")
    print("=" * 80)
    print()
    
    # Get current deployments
    print("Step 1: Backing up deployment configuration")
    print("-" * 80)
    
    deployments = await client.read_deployments()
    backed_up = {}
    
    for dep in deployments:
        print(f"  {dep.name}")
        backed_up[dep.name] = {
            "id": dep.id,
            "flow_id": dep.flow_id,
            "schedules": dep.schedules,
            "description": dep.description,
            "entrypoint": dep.entrypoint,
        }
    
    print()
    print("Step 2: Creating local work pool")
    print("-" * 80)
    
    try:
        from prefect.client.schemas.objects import WorkPool
        from prefect.workers.process import ProcessWorkerConfiguration
        
        # Create local work pool
        work_pool = await client.create_work_pool(
            work_pool={
                "name": "local",
                "type": "process",
                "description": "Local process worker for Windows"
            }
        )
        print(f"✅ Created work pool: local")
    except Exception as e:
        print(f"ℹ️  Work pool info: {e}")
        print(f"   You may need to delete Yichen_Test first or use existing pool")
    
    print()
    print()
    print("=" * 80)
    print("NEXT STEPS")
    print("=" * 80)
    print("""
1. Delete the old work pool (if you want):
   prefect work-pool delete "Yichen_Test"

2. Create local work pool:
   prefect work-pool create "local" --type process

3. Update deployments to use "local" work pool:
   - Via Prefect Cloud UI: Edit each deployment, change work pool to "local"
   - Or run the Python script to update programmatically

4. Start the local worker:
   prefect worker start --pool "local"
   
5. Schedule the flow runs (already configured with cron)

The local worker will run flows on your Windows machine
and have full access to your project files!
""")


if __name__ == "__main__":
    asyncio.run(switch_to_local_worker())
