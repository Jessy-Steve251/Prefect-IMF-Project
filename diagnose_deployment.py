"""
Diagnose deployment configuration
"""
import asyncio
from prefect.client.orchestration import get_client


async def diagnose():
    client = get_client()
    
    deps = await client.read_deployments()
    if deps:
        dep = deps[0]
        print("Deployment attributes:")
        for attr in dir(dep):
            if not attr.startswith('_'):
                try:
                    val = getattr(dep, attr)
                    if not callable(val):
                        print(f"  {attr}: {val}")
                except:
                    pass


asyncio.run(diagnose())
