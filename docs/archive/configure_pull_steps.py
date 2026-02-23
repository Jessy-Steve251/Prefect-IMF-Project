"""
Configure deployments with pull_steps to download code from local directory
This works with Prefect Cloud's managed workers
"""

import asyncio
import yaml
from pathlib import Path
from prefect.client.orchestration import get_client


async def configure_pull_steps():
    """Add pull_steps to deployments to fetch code"""
    
    client = get_client()
    
    print("=" * 80)
    print("CONFIGURING PULL_STEPS FOR DEPLOYMENTS")
    print("=" * 80)
    print()
    
    project_path = Path.cwd()
    print(f"Project path: {project_path}")
    print()
    
    # Get deployments
    deployments = {dep.name: dep for dep in await client.read_deployments()}
    
    # Define pull_steps - copy files from project directory
    pull_steps = [
        {
            "type": "prefect.deployments.steps.run_shell_script",
            "id": "clone-project",
            "parameters": {
                "script": f"mkdir -p /opt/prefect && cp -r '{project_path}'/* /opt/prefect/ || true"
            }
        }
    ]
    
    print("Updating deployments with pull_steps...")
    print("-" * 80)
    print(f"Pull step will copy code from: {project_path}")
    print()
    
    for dep_name, dep in deployments.items():
        print(f"{dep_name}:")
        
        try:
            # Update deployment with pull_steps
            result = await client.update_deployment(
                dep.id,
                pull_steps=pull_steps
            )
            print(f"  ✅ Updated")
        except Exception as e:
            print(f"  ❌ Error: {e}")
        
        print()
    
    # Verify
    print("=" * 80)
    print("VERIFICATION")
    print("=" * 80)
    print()
    
    updated_deployments = await client.read_deployments()
    
    for dep in updated_deployments:
        print(f"{dep.name}:")
        if dep.pull_steps:
            print(f"  ✅ Pull steps configured")
            for step in dep.pull_steps:
                print(f"     Type: {step.get('type')}")
        else:
            print(f"  ⚠️  No pull steps")
        print()


if __name__ == "__main__":
    asyncio.run(configure_pull_steps())
