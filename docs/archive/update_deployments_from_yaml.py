"""
Update existing deployments with correct path configuration
This reads the updated prefect.yaml and updates the corresponding deployments
"""

import asyncio
import yaml
from pathlib import Path
from prefect.client.orchestration import get_client


async def update_deployments_from_config():
    """Update deployments using prefect.yaml configuration"""
    
    # Read prefect.yaml
    yaml_path = Path("prefect.yaml")
    if not yaml_path.exists():
        print(f"❌ {yaml_path} not found")
        return
    
    with open(yaml_path, 'r') as f:
        config = yaml.safe_load(f)
    
    deployments_config = config.get('deployments', [])
    
    client = get_client()
    existing_deployments = await client.read_deployments()
    
    print("=" * 80)
    print("UPDATING DEPLOYMENTS FROM prefect.yaml")
    print("=" * 80)
    print()
    
    for dep_config in deployments_config:
        dep_name = dep_config.get('name')
        print(f"Deployment: {dep_name}")
        
        # Find existing deployment
        existing_dep = None
        for dep in existing_deployments:
            if dep.name == dep_name:
                existing_dep = dep
                break
        
        if not existing_dep:
            print(f"  ❌ Deployment not found in cloud")
            print()
            continue
        
        # Prepare update payload
        update_payload = {}
        
        # Update path if specified
        if 'path' in dep_config:
            new_path = dep_config['path']
            print(f"  Current path: {existing_dep.path}")
            print(f"  New path: {new_path}")
            update_payload['path'] = new_path
        
        # Update entrypoint if specified
        if 'entrypoint' in dep_config:
            new_entrypoint = dep_config['entrypoint']
            print(f"  Current entrypoint: {existing_dep.entrypoint}")
            print(f"  New entrypoint: {new_entrypoint}")
            update_payload['entrypoint'] = new_entrypoint
        
        # Try updating
        if update_payload:
            try:
                # Prefect API requires using specific field names
                # Check what parameters are available
                result = await client.update_deployment(
                    deployment_id=existing_dep.id,
                    **update_payload
                )
                print(f"  ✅ Updated successfully")
            except TypeError as e:
                # Parameter name might be different
                print(f"  ℹ️  Update method note: {str(e)}")
                
                # Try alternative approach using deployment update method
                try:
                    # Fetch the deployment object again  
                    dep_data = {
                        "name": existing_dep.name,
                        "flow_id": existing_dep.flow_id,
                        "work_pool_name": existing_dep.work_pool_name,
                    }
                    
                    # Add optional fields if they changed
                    if 'path' in update_payload:
                        dep_data['path'] = update_payload['path']
                    if 'entrypoint' in update_payload:
                        dep_data['entrypoint'] = update_payload['entrypoint']
                    
                    result = await client.update_deployment(
                        existing_dep.id,
                        **dep_data
                    )
                    print(f"  ✅ Updated successfully (alternative method)")
                except Exception as e2:
                    print(f"  ❌ Could not update: {str(e2)}")
            except Exception as e:
                print(f"  ❌ Error: {str(e)}")
        else:
            print(f"  ℹ️  No updates specified in config")
        
        print()
    
    # Verify updates
    print("=" * 80)
    print("VERIFICATION")
    print("=" * 80)
    print()
    
    updated_deployments = await client.read_deployments()
    
    for dep in updated_deployments:
        print(f"{dep.name}:")
        print(f"  Path: {dep.path}")
        print(f"  Entrypoint: {dep.entrypoint}")
        print()


if __name__ == "__main__":
    asyncio.run(update_deployments_from_config())
