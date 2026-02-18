"""
Recreate schedules for deployments
This script reads the prefect.yaml and recreates the schedules
"""

import asyncio
from pathlib import Path
from prefect.client.orchestration import get_client
import yaml


async def recreate_schedules():
    """Recreate schedules using the correct API"""
    
    client = get_client()
    
    # Load configuration
    with open("prefect.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    deployments_config = config.get('deployments', [])
    
    # Get current deployments
    deployments = {dep.name: dep for dep in await client.read_deployments()}
    
    print("=" * 80)
    print("RECREATING DEPLOYMENT SCHEDULES")
    print("=" * 80)
    print()
    
    for dep_config in deployments_config:
        dep_name = dep_config.get('name')
        schedules_list = dep_config.get('schedules', [])
        
        print(f"{dep_name}:")
        
        if dep_name not in deployments:
            print(f"  ❌ Deployment not found")
            continue
        
        dep = deployments[dep_name]
        
        if not schedules_list:
            print(f"  ℹ️  No schedules configured")
            continue
        
        for schedule_config in schedules_list:
            cron = schedule_config.get('cron')
            timezone = schedule_config.get('timezone', 'UTC')
            active = schedule_config.get('active', True)
            
            print(f"  Adding schedule: cron={cron}, tz={timezone}")
            
            try:
                # Use the correct API - schedules is list of (schedule_dict, active) tuples
                result = await client.create_deployment_schedules(
                    deployment_id=dep.id,
                    schedules=[({
                        "cron": cron,
                        "timezone": timezone
                    }, active)]
                )
                
                print(f"    ✅ Created")
                
            except Exception as e:
                print(f"    ❌ Error: {str(e)}")
                import traceback
                traceback.print_exc()
        
        print()
    
    # Verify
    print("=" * 80)
    print("VERIFICATION")
    print("=" * 80)
    print()
    
    updated_deployments = await client.read_deployments()
    
    for dep in updated_deployments:
        print(f"{dep.name}:")
        if dep.schedules:
            print(f"  ✅ Schedules present:")
            for schedule in dep.schedules:
                print(f"     - {schedule}")
        else:
            print(f"  ℹ️  No schedules yet")


if __name__ == "__main__":
    asyncio.run(recreate_schedules())
