"""
Recreate deployments with correct path configuration
This will delete old deployments and create new ones with the path parameter set
"""

import asyncio
from pathlib import Path
from prefect.client.orchestration import get_client
import yaml


async def recreate_deployments_with_path():
    """Delete old deployments and recreate with correct path"""
    
    client = get_client()
    
    print("=" * 80)
    print("RECREATING DEPLOYMENTS WITH CORRECT PATH")
    print("=" * 80)
    print()
    
    # Load configuration
    with open("prefect.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    deployments_config = config.get('deployments', [])
    
    # Get current deployments (to save their IDs and configs)
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
    
    # Create new deployments with path
    print("Step 3: Creating new deployments with correct path")
    print("-" * 80)
    
    project_path = Path.cwd()
    
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
            # Prepare deployment data
            deployment_data = {
                "name": dep_name,
                "flow_id": flow_id,
                "work_pool_name": work_pool_name,
                "description": dep_config.get('description', ''),
                "entrypoint": entrypoint,
                "path": str(project_path),  # THE KEY PARAMETER
            }
            
            # Add optional fields
            if backed_up.get('parameters'):
                deployment_data['parameters'] = backed_up['parameters']
            if backed_up.get('tags'):
                deployment_data['tags'] = backed_up['tags']
            
            # Create deployment
            result = await client.create_deployment(**deployment_data)
            
            print(f"    ✅ Created with path: {project_path}")
            
            # Add schedules if they exist
            if dep_config.get('schedules') and backed_up.get('schedules'):
                schedules = backed_up['schedules']
                print(f"    ℹ️  Note: Schedules need to be re-created separately")
                for schedule in schedules:
                    print(f"       - {schedule.schedule.cron}")
            
        except Exception as e:
            print(f"    ❌ Error creating: {e}")
            import traceback
            traceback.print_exc()
    
    print()
    
    # Verify
    print("Step 4: Verification")
    print("-" * 80)
    
    new_deployments = await client.read_deployments()
    
    for dep in new_deployments:
        print(f"{dep.name}:")
        print(f"  Path: {dep.path}")
        print(f"  Entrypoint: {dep.entrypoint}")
        if str(project_path) in dep.path or dep.path == str(project_path):
            print(f"  ✅ Path looks correct")
        else:
            print(f"  ⚠️  Path might need review")
        print()
    
    print("=" * 80)
    print("DEPLOYMENT RECREATION COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(recreate_deployments_with_path())
