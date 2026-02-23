"""
Diagnose work pool configuration
Check if the work pool is properly configured to access the code
"""

import asyncio
from prefect.client.orchestration import get_client


async def diagnose_work_pool():
    """Check work pool configuration"""
    
    client = get_client()
    
    print("=" * 80)
    print("WORK POOL CONFIGURATION DIAGNOSIS")
    print("=" * 80)
    print()
    
    # Get deployments
    deployments = await client.read_deployments()
    
    if not deployments:
        print("❌ No deployments found")
        return
    
    dep = deployments[0]
    print(f"Deployment: {dep.name}")
    print(f"Work Pool: {dep.work_pool_name}")
    print()
    
    # Try to get work pool details
    try:
        # Get the work pool by name
        work_pools = await client.read_work_pools()
        print(f"Total work pools: {len(work_pools)}")
        print()
        
        for wp in work_pools:
            print(f"Work Pool Name: {wp.name}")
            print(f"  Type: {wp.type}")
            print(f"  Status: {wp.status}")
            print(f"  Base Job Template: {wp.base_job_template}")
            print()
    except Exception as e:
        print(f"Could not retrieve work pool details: {e}")
    
    print()
    print("=" * 80)
    print("ANALYSIS")
    print("=" * 80)
    print("""
The error shows:
  FileNotFoundError: /opt/prefect/flows/currency_acquisition_flow.py

This means:
1. The work pool is running in a Docker container or remote Linux system
2. The container's working directory is /opt/prefect/
3. The code is NOT mounted or copied into the container

SOLUTION OPTIONS:

1. ✅ OPTION A - Use pull_steps (GIT)
   Configure deployments to pull code from a Git repository
   
   Example in prefect.yaml:
   pull_steps:
     - type: git_clone
       repository: "https://github.com/your-repo/project.git"
       branch: "main"

2. ✅ OPTION B - Configure Storage Block + Push Code
   Create a LocalFileSystem storage block and configure push_steps
   to upload code before running

3. ✅ OPTION C - Reconfigure Work Pool
   Update the work pool to:
   - Mount the Windows directory as a volume (if Docker)
   - Or set the working directory to where code is located

4. ✅ OPTION D - Use Docker Image with Code Included
   Create a Docker image that includes your project code
   and use it as the infrastructure for deployments

RECOMMENDED: Use Git-based pull_steps (OPTION A)
- Most reliable
- Works with cloud deployments
- Version controlled
- Easy to maintain
""")


if __name__ == "__main__":
    asyncio.run(diagnose_work_pool())
