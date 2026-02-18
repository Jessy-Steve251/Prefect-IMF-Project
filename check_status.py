"""
检查 Deployment 和 Schedule 状态
"""
import asyncio
from prefect.client.orchestration import get_client


async def check_status():
    """检查所有 Deployment 和它们的 Schedule"""
    client = get_client()
    
    print("=" * 70)
    print("Checking Deployments and Schedules Status")
    print("=" * 70)
    print()
    
    # Get all deployments
    deployments = await client.read_deployments()
    
    if not deployments:
        print("❌ No deployments found")
        return
    
    print(f"Found {len(deployments)} Deployments:\n")
    
    for dep in deployments:
        print(f"Deployment: {dep.name}")
        print(f"  ID: {dep.id}")
        print(f"  Status: {dep.status}")
        print(f"  Work Pool: {dep.work_pool_name}")
        
        try:
            # Check schedules for this deployment
            schedules = await client.read_deployment_schedules(dep.id)
            
            if schedules:
                print(f"  Schedules: {len(schedules)} found ✅")
                for sched in schedules:
                    # Print available attributes
                    print(f"    - ID: {sched.id}")
                    print(f"      Active: {sched.active}")
                    print(f"      Created: {sched.created}")
                    if hasattr(sched, 'timezone'):
                        print(f"      Timezone: {sched.timezone}")
                    print()
            else:
                print(f"  Schedules: NONE (This is the problem!)")
                
        except Exception as e:
            print(f"  Error reading schedules: {str(e)}")
        
        print()


if __name__ == "__main__":
    asyncio.run(check_status())
