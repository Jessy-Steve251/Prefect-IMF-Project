import asyncio
from prefect.client.orchestration import get_client

async def check_methods():
    client = get_client()
    methods = [m for m in dir(client) if 'schedule' in m.lower()]
    print("Schedule-related methods:")
    for m in methods:
        print(f"  - {m}")

asyncio.run(check_methods())
