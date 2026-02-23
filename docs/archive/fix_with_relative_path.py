"""
Recreate deployments with relative path instead of absolute Windows path
"""

import asyncio
from pathlib import Path
from prefect.client.orchestration import get_client
import yaml


async def fix_deployment_paths():
    """Recreate deployments with relative path"""
    
    client = get_client()
    
    print("=" * 80)
    print("FIXING DEPLOYMENT PATHS - Using Relative Path")
    print("=" * 80)
    print()
    
    # Load configuration
    with open("prefect.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    deployments_config = config.get('deployments', [])
    
    # Get current deployments
    print("Step 1: Backing up current deployment configuration")
    print("-" * 80)
    
    current_deployments = await client.read_deployments()
    backed_up_configs = {}
    
    for current_dep in current_deployments:
        print(f"  {current_dep.name}")
        backed_up_configs[current_dep.name] = {
            "id": current_dep.id,
            "flow_id": current_dep.flow_id,
            "work_pool_name": current_dep.work_pool_name,
            "schedules": current_dep.schedules,
            "parameters": current_dep.parameters,
            "description": current_dep.description,
            "tags": current_dep.tags,
        }
    
    print()
    
    # Delete old deployments
    print("Step 2: Deleting old deployments")
    print("-" * 80)
    
    for dep in current_deployments:
        try:
            await client.delete_deployment(dep.id)
            print(f"  ✅ Deleted: {dep.name}")
        except Exception as e:
            print(f"  ❌ Error deleting {dep.name}: {e}")
    
    print()
    
    # Create new deployments with relative path
    print("Step 3: Creating new deployments with relative path (.)")
    print("-" * 80)
    
    for dep_config in deployments_config:
        dep_name = dep_config.get('name')
        flow_name = dep_config.get('flow')
        entrypoint = dep_config.get('entrypoint')
        
        print(f"  {dep_name}")
        
        # Get backed up config
        backed_up = backed_up_configs.get(dep_name, {})
        flow_id = backed_up.get('flow_id')
        work_pool_name = backed_up.get('work_pool_name')
        
        if not flow_id:
            print(f"    ❌ Flow ID not found, skipping")
            continue
        
        try:
            # Prepare deployment data with relative path
            deployment_data = {
                "name": dep_name,
                "flow_id": flow_id,
                "work_pool_name": work_pool_name,
                "description": dep_config.get('description', ''),
                "entrypoint": entrypoint,
                "path": ".",  # RELATIVE PATH
            }
            
            # Add optional fields
            if backed_up.get('parameters'):
                deployment_data['parameters'] = backed_up['parameters']
            if backed_up.get('tags'):
                deployment_data['tags'] = backed_up['tags']
            
            # Create deployment
            result = await client.create_deployment(**deployment_data)
            
            print(f"    ✅ Created with path: .")
            
        except Exception as e:
            print(f"    ❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    print()
    
    # Restore schedules
    print("Step 4: Restoring schedules")
    print("-" * 80)
    
    new_deployments = await client.read_deployments()
    new_dep_map = {dep.name: dep for dep in new_deployments}
    
    for dep_config in deployments_config:
        dep_name = dep_config.get('name')
        schedules_list = dep_config.get('schedules', [])
        
        if dep_name not in new_dep_map or not schedules_list:
            continue
        
        dep = new_dep_map[dep_name]
        
        for schedule_config in schedules_list:
            cron = schedule_config.get('cron')
            timezone = schedule_config.get('timezone', 'UTC')
            
            try:
                await client.create_deployment_schedules(
                    deployment_id=dep.id,
                    schedules=[({
                        "cron": cron,
                        "timezone": timezone
                    }, True)]
                )
                print(f"  ✅ {dep_name}: {cron}")
            except Exception as e:
                print(f"  ❌ {dep_name}: {e}")
    
    print()
    
    # Verify
    print("=" * 80)
    print("VERIFICATION")
    print("=" * 80)
    print()
    
    final_deployments = await client.read_deployments()
    
    for dep in final_deployments:
        print(f"{dep.name}:")
        print(f"  Path: {dep.path}")
        print(f"  Entrypoint: {dep.entrypoint}")
        if dep.schedules:
            for schedule in dep.schedules:
                print(f"  Schedule: {schedule.schedule.cron} ({schedule.schedule.timezone})")
        print()
    
    print("=" * 80)
    print("✅ FIX COMPLETE - Using relative path (.)")
    print("=" * 80)
    print()
    print("Note: Path is now '.' which will be interpreted relative to worker's cwd")
    print("Make sure your work pool is configured to use the correct directory")


if __name__ == "__main__":
    asyncio.run(fix_deployment_paths())
