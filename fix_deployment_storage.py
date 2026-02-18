"""
Fix Prefect Deployments: Configure storage location for flow code

This script:
1. Updates deployments to set the correct path to project root
2. Ensures Prefect can find the flow code files when executing

The issue: Deployments had path="." which in container context became /opt/prefect/
The solution: Set path to the actual project directory where flows exist
"""

import asyncio
import os
from pathlib import Path
from prefect.client.orchestration import get_client


async def fix_deployment_paths():
    """Fix path configuration for deployments"""
    client = get_client()
    
    print("=" * 80)
    print("FIXING DEPLOYMENT PATH CONFIGURATION")
    print("=" * 80)
    print()
    
    try:
        project_path = Path.cwd()
        print(f"Project path: {project_path}")
        print()
        
        # Step 1: Verify flow files exist
        print("Step 1: Verifying Flow Files")
        print("-" * 80)
        
        flow_files = [
            "flows/currency_acquisition_flow.py",
            "flows/prepare_batch_flow.py", 
            "flows/process_batch_flow.py"
        ]
        
        all_exist = True
        for flow_file in flow_files:
            full_path = project_path / flow_file
            if full_path.exists():
                print(f"✅ {flow_file}")
            else:
                print(f"❌ {flow_file} - NOT FOUND")
                all_exist = False
        
        if not all_exist:
            print()
            print("⚠️  Some flow files are missing!")
            return
        
        print()
        
        # Step 2: Get all deployments and update their paths
        print("Step 2: Updating Deployment Paths")
        print("-" * 80)
        
        deployments = await client.read_deployments()
        print(f"Found {len(deployments)} deployment(s)\n")
        
        for deployment in deployments:
            print(f"Deployment: {deployment.name}")
            print(f"  Current path: {deployment.path}")
            print(f"  Entrypoint: {deployment.entrypoint}")
            print(f"  Storage block: {deployment.storage_document_id or 'None'}")
            
            # Update deployment path to project root
            try:
                # Use update_deployment to set the path
                update_payload = {
                    "path": str(project_path)
                }
                
                await client.update_deployment(
                    deployment_id=deployment.id,
                    **update_payload
                )
                print(f"  ✅ Updated path to: {project_path}")
                
            except Exception as e:
                print(f"  ❌ Error updating: {str(e)}")
            
            print()
        
        # Step 3: Verify updates
        print("Step 3: Verifying Updates")
        print("-" * 80)
        
        deployments = await client.read_deployments()
        
        for deployment in deployments:
            print(f"{deployment.name}:")
            print(f"  Path: {deployment.path}")
            if deployment.path == str(project_path):
                print(f"  ✅ Path correctly set")
            else:
                print(f"  ⚠️  Path not updated as expected")
            print()
        
        print("=" * 80)
        print("DEPLOYMENT PATH FIX COMPLETE")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Trigger a new flow run to test the fix")
        print("  2. Monitor the flow run logs in Prefect Cloud")
        print("  3. Verify that flows can now find their files")
        print()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(fix_deployment_paths())
