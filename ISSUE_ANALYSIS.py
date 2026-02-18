"""
Analysis: Prefect Deployment Flow Code Loading Issue
======================================================

PROBLEM:
--------
FileNotFoundError: [Errno 2] No such file or directory: '/opt/prefect/flows/currency_acquisition_flow.py'

When the flow runs in the worker (container), it looks for files in '/opt/prefect/' but they're not there.

ROOT CAUSE:
-----------
The prefect.yaml is missing the 'build' section that tells Prefect how to package and prepare the code.
When Prefect executes the flow, it tries to load from a path that doesn't exist in the execution environment.

SOLUTIONS:
----------

1. LOCAL EXECUTION (RECOMMENDED - Simplest)
   - Set the deployment to use 'path' pointing to your local project directory
   - Worker runs flows using local code directly
   - No need for storage blocks or complex setup

2. DOCKER/CLOUD EXECUTION (More Complex)
   - Add 'build' section to prefect.yaml with Docker steps
   - Create storage block and push code to it
   - Requires Docker and container registry

BEST FIX FOR YOUR CASE:
-----------------------
Since you're running locally and getting '/opt/prefect/' path error, the system is:
1. Configured for remote/container execution
2. But code isn't being deployed to that location

IMMEDIATE ACTION NEEDED:
1. Update prefect.yaml with correct 'build' configuration
2. OR Update prefect.yaml to explicitly set absolute path
3. OR Recreate deployments with prefect deploy command (which uses prefect.yaml)
"""

import asyncio
from prefect.client.orchestration import get_client
import os
from pathlib import Path


async def check_current_state():
    """Check current deployment configuration"""
    client = get_client()
    
    print(__doc__)
    
    print("\n" + "=" * 80)
    print("CURRENT DEPLOYMENT STATE")
    print("=" * 80)
    
    deployments = await client.read_deployments()
    
    for dep in deployments:
        print(f"\n{dep.name}:")
        print(f"  Entrypoint: {dep.entrypoint}")
        print(f"  Path: {dep.path}")
        print(f"  Work Pool: {dep.work_pool_name}")
        print(f"  Pull Steps: {dep.pull_steps or 'None'}")
        print(f"  Storage Block ID: {dep.storage_document_id or 'None'}")


if __name__ == "__main__":
    asyncio.run(check_current_state())
