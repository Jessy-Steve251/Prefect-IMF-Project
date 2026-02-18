"""
Recreate deployments with GitHub pull_steps
"""

import asyncio
import yaml
from pathlib import Path
from prefect.client.orchestration import get_client


async def recreate_with_github():
    """Recreate deployments with GitHub pull_steps"""
    
    client = get_client()
    
    print("=" * 80)
    print("RECREATING DEPLOYMENTS WITH GITHUB PULL_STEPS")
    print("=" * 80)
    print()
    
    # Load configuration
    with open("prefect.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    deployments_config = config.get('deployments', [])
    
    # Step 1: Backup current deployments
    print("Step 1: Backing up current deployment configuration")
    print("-" * 80)
    
    current_deployments = await client.read_deployments()
    backed_up = {}
    
    for dep in current_deployments:
        print(f"  {dep.name}")
        backed_up[dep.name] = {
            "id": dep.id,
            "flow_id": dep.flow_id,
            "schedules": dep.schedules,
        }
    
    print()
    
    # Step 2: Delete old deployments
    print("Step 2: Deleting old deployments")
    print("-" * 80)
    
    for dep in current_deployments:
        try:
            await client.delete_deployment(dep.id)
            print(f"  ✅ Deleted: {dep.name}")
        except Exception as e:
            print(f"  ❌ Error: {e}")
    
    print()
    
    # Step 3: Create new deployments with GitHub pull_steps
    print("Step 3: Creating new deployments with GitHub pull_steps")
    print("-" * 80)
    
    for dep_config in deployments_config:
        dep_name = dep_config.get('name')
        print(f"  {dep_name}")
        
        backed_up_dep = backed_up.get(dep_name, {})
        flow_id = backed_up_dep.get('flow_id')
        
        if not flow_id:
            print(f"    ❌ Flow ID not found")
            continue
        
        try:
            pull_steps = dep_config.get('pull_steps', [])
            
            deployment_data = {
                "name": dep_name,
                "flow_id": flow_id,
                "work_pool_name": dep_config.get('work_pool', {}).get('name'),
                "description": dep_config.get('description', ''),
                "entrypoint": dep_config.get('entrypoint'),
                "path": dep_config.get('path', '.'),
            }
            
            if pull_steps:
                deployment_data['pull_steps'] = pull_steps
            
            result = await client.create_deployment(**deployment_data)
            print(f"    ✅ Created")
            
        except Exception as e:
            print(f"    ❌ Error: {e}")
    
    print()
    
    # Step 4: Restore schedules
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
        print(f"  Pull Steps: {'✅ Configured' if dep.pull_steps else '❌ Not configured'}")
        if dep.pull_steps:
            for step in dep.pull_steps:
                if isinstance(step, dict):
                    repo = step.get('parameters', {}).get('repository', 'N/A')
                    print(f"    Repository: {repo}")
                else:
                    print(f"    {step}")
        print(f"  Schedules: {len(dep.schedules)}")
        for schedule in dep.schedules:
            print(f"    - {schedule.schedule.cron}")
        print()
    
    print("=" * 80)
    print("✅ DEPLOYMENT RECREATION COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(recreate_with_github())
