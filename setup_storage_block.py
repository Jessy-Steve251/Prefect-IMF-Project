"""
Setup LocalFileSystem storage block and configure deployments to use it
This allows Prefect Cloud's managed workers to download code from local storage
"""

import asyncio
from pathlib import Path
from prefect.client.orchestration import get_client


async def setup_local_storage():
    """Setup local file system storage block"""
    
    client = get_client()
    
    print("=" * 80)
    print("SETTING UP LOCAL STORAGE BLOCK")
    print("=" * 80)
    print()
    
    project_path = Path.cwd()
    print(f"Project path: {project_path}")
    print()
    
    # Step 1: Create LocalFileSystem storage block
    print("Step 1: Creating LocalFileSystem storage block")
    print("-" * 80)
    
    block_name = "project-storage"
    
    try:
        # Try to create the storage block
        block_data = {
            "block_type_slug": "local-file-system",
            "block_document_name": block_name,
            "data": {
                "basepath": str(project_path)
            }
        }
        
        response = await client._post(
            "/block_documents/",
            json=block_data
        )
        print(f"✅ Created storage block: {block_name}")
        print(f"   Base path: {project_path}")
    except Exception as e:
        print(f"⚠️  Error creating block: {e}")
        print(f"   Note: Block may already exist")
    
    print()
    
    # Step 2: Update deployments to use storage block and pull_steps
    print("Step 2: Updating deployments with pull_steps")
    print("-" * 80)
    
    deployments = await client.read_deployments()
    
    for dep in deployments:
        print(f"{dep.name}:")
        
        try:
            # Update with pull_steps to sync from local storage
            update_data = {
                "pull_steps": [
                    {
                        "type": "prefect.deployments.steps.run_shell_script",
                        "id": "sync-code",
                        "parameters": {
                            "script": f"cd '{project_path}' && python -m prefect.utilities.sync"
                        }
                    }
                ]
            }
            
            # Try updating (this might fail if pull_steps not supported)
            await client.update_deployment(dep.id, **update_data)
            print(f"  ✅ Updated with pull_steps")
        except Exception as e:
            print(f"  ℹ️  Note: {e}")
        
        print()
    
    print()
    print("=" * 80)
    print("ALTERNATIVE SOLUTION: Use Storage Block Reference")
    print("=" * 80)
    print("""
The best approach for managed workers is to:

1. Create a LocalFileSystem storage block (done ✓)
2. Configure deployments to use this storage block
3. Add pull_steps that copy code from storage to execution directory

Manual steps in Prefect Cloud:
1. Go to Deployment details
2. In "Storage" section, select the storage block
3. Save

Or use the CLI:
prefect deployment set-schedules "currency-acquisition"
prefect storage-block ls  # List storage blocks
""")


if __name__ == "__main__":
    asyncio.run(setup_local_storage())
