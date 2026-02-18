"""
Create local work pool via Python API with minimal configuration
"""

import asyncio
from prefect.client.orchestration import get_client


async def create_local_pool():
    """Create local work pool"""
    
    client = get_client()
    
    print("Creating local work pool...")
    print()
    
    try:
        # Create minimal work pool configuration
        work_pool_data = {
            "name": "local",
            "type": "process",
            "is_paused": False,
            "concurrency_limit": None,
            "concurrency_options": None,
            "base_job_template": {
                "job_configuration": {},
                "variables": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
        }
        
        result = await client._post(
            "/work_pools/",
            json=work_pool_data
        )
        
        print("✅ Created work pool: local")
        print(result)
        
    except Exception as e:
        error_str = str(e)
        if "already exists" in error_str.lower():
            print("✅ Work pool 'local' already exists")
        else:
            print(f"❌ Error: {e}")
            print()
            print("This is expected on free Prefect Cloud plans.")
            print("Free plans don't support self-hosted workers.")
            print()
            print("ALTERNATIVE: Use GitHub-based deployment")
            print("See SOLUTION_GITHUB_DEPLOYMENT.md")


if __name__ == "__main__":
    asyncio.run(create_local_pool())
