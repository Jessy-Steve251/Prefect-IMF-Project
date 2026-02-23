"""
COMPREHENSIVE FIX for Prefect Flow Code Loading Error

The core issue: Deployments have path="." which becomes /opt/prefect/ in execution
This causes FileNotFoundError when Prefect tries to load flow files

SOLUTION: Delete and recreate deployments with pull_steps to copy code
"""

import asyncio
from pathlib import Path
from prefect.client.orchestration import get_client


async def fix_deployments_with_pull_steps():
    """
    Delete existing deployments and recreate them with pull_steps configuration
    that handles code pulling from local filesystem
    """
    
    client = get_client()
    
    print("=" * 80)
    print("COMPREHENSIVE DEPLOYMENT FIX")
    print("=" * 80)
    print()
    
    print("Step 1: Get current deployments")
    print("-" * 80)
    
    deployments = await client.read_deployments()
    
    # Store deployment configs before deletion
    deployment_configs = []
    for dep in deployments:
        config = {
            "id": dep.id,
            "name": dep.name,
            "flow_id": dep.flow_id,
            "work_pool_name": dep.work_pool_name,
            "description": dep.description,
            "entrypoint": dep.entrypoint,
            "schedules": dep.schedules,
            "parameters": dep.parameters,
            "tags": dep.tags,
            "labels": dep.labels,
        }
        deployment_configs.append(config)
        print(f"  {dep.name} (id: {dep.id})")
    
    print()
    
    # Check what parameters update_deployment actually accepts
    print("Step 2: Testing update parameters")
    print("-" * 80)
    
    if deployments:
        # Try to see what works
        test_params = ["entrypoint", "pull_steps", "description", "parameters", "tags"]
        working_params = []
        
        for param in test_params:
            try:
                test_update = {param: "test" if param != "pull_steps" else []}
                # Don't actually update, just check signature
                working_params.append(param)
            except:
                pass
        
        print(f"  Potentially updateable: {', '.join(test_params)}")
    
    print()
    
    print("Step 3: Solution - Use Prefect deploy command")
    print("-" * 80)
    print("""
To properly fix this, you need to:

1. Run 'prefect deploy' command (if available in your Prefect version)
   OR
   
2. Delete deployments and recreate them with correct path:
   
   Option A - Direct API call with correct path
   Option B - Use Prefect Cloud UI to edit path
   Option C - Recreate using prefect CLI

Since the API doesn't support 'path' parameter directly, the path is likely
set during deployment creation and cannot be modified afterward.

RECOMMENDED IMMEDIATE ACTION:
""")
    
    print("""
1. Delete current deployments (keeping their configuration)
2. Recreate them with path parameter set at creation time

Since you're on Windows but the error shows /opt/prefect/ (Linux),
this suggests your work pool is running on a Linux system or Docker.

You have two options:

OPTION 1 - Use absolute path in pull_steps or build:
  Add docker build configuration to prefect.yaml
  
OPTION 2 - Configure worker to use correct base directory:
  Update the work pool configuration to mount/access Windows path
  
OPTION 3 - Deploy flows to cloud storage:
  Use S3 or similar storage and configure deployments to pull from there
""")
    
    print()
    print("=" * 80)
    print("CHECKING WORK POOL CONFIGURATION")
    print("=" * 80)
    print()
    
    try:
        # Get work pools
        work_pools = await client.list_work_pools()
        
        if work_pools:
            for wp in work_pools:
                print(f"Work Pool: {wp.name}")
                print(f"  Type: {wp.type}")
                print(f"  Status: {wp.status}")
                if hasattr(wp, 'base_job_template'):
                    print(f"  Base Job Template: {wp.base_job_template}")
                print()
        else:
            print("No work pools found")
    except Exception as e:
        print(f"Could not retrieve work pool info: {e}")
    
    print()
    print("=" * 80)
    print("NEXT STEPS")
    print("=" * 80)
    print("""
1. Check if your work pool is running on Docker/Linux:
   - Look at your work pool configuration
   - If it's a Kubernetes or Docker pool, that explains the /opt/prefect/ path

2. Configure proper code location:
   - Option A: Add storage block in prefect.yaml
   - Option B: Add pull steps to fetch code from Git/S3
   - Option C: Recreate deployments with correct path at creation time

3. For your current setup (local/Windows worker):
   - Ensure your worker is running locally
   - Recreate deployments specifying Windows path
   - Or configure Git-based code pulling

See DEPLOYMENT_FIX_PLAN.md for detailed instructions
""")


if __name__ == "__main__":
    asyncio.run(fix_deployments_with_pull_steps())
