#!/usr/bin/env python
"""Recreate the Yichen_Test work pool"""

from prefect.client.cloud_client import get_cloud_client
from prefect.client.schemas.objects import WorkPool
import asyncio

async def recreate_work_pool():
    async with get_cloud_client() as client:
        # Check if work pool exists
        try:
            wp = await client.read_work_pool("Yichen_Test")
            print("✅ Work pool 'Yichen_Test' already exists")
            return
        except Exception as e:
            print(f"ℹ️  Work pool not found, creating new one: {e}")
        
        # Create new work pool
        try:
            wp_dict = {
                "name": "Yichen_Test",
                "type": "prefect:managed",
                "is_paused": False
            }
            
            response = await client.post(
                "/work_pools",
                json=wp_dict
            )
            print(f"✅ Work pool created successfully!")
            print(f"   Name: {response.get('name')}")
            print(f"   Type: {response.get('type')}")
            print(f"   Status: {'Paused' if response.get('is_paused') else 'Active'}")
            
        except Exception as e:
            print(f"❌ Failed to create work pool: {e}")
            raise

if __name__ == "__main__":
    asyncio.run(recreate_work_pool())
