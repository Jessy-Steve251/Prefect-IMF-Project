"""
Update currency-acquisition schedule to 12:10
"""

import asyncio
from prefect.client.orchestration import get_client


async def update_currency_schedule():
    """Update currency-acquisition schedule to 12:10 (10 12 17 * *)"""
    
    client = get_client()
    
    print("=" * 80)
    print("UPDATING CURRENCY-ACQUISITION SCHEDULE")
    print("=" * 80)
    print()
    
    # Get deployments
    deployments = {dep.name: dep for dep in await client.read_deployments()}
    
    dep = deployments.get("currency-acquisition")
    if not dep:
        print("❌ currency-acquisition deployment not found")
        return
    
    print(f"Deployment: {dep.name}")
    print(f"New time: 12:10 PM")
    print(f"New cron: 10 12 17 * *")
    print()
    
    # Delete old schedule
    if dep.schedules:
        print(f"Deleting old schedule(s)...")
        for schedule in dep.schedules:
            try:
                await client.delete_deployment_schedule(
                    deployment_id=dep.id,
                    schedule_id=schedule.id
                )
                print(f"  ✅ Deleted: {schedule.schedule.cron}")
            except Exception as e:
                print(f"  ❌ Error: {e}")
    
    # Create new schedule
    print(f"Creating new schedule...")
    try:
        await client.create_deployment_schedules(
            deployment_id=dep.id,
            schedules=[({
                "cron": "10 12 17 * *",
                "timezone": "Europe/Zurich"
            }, True)]
        )
        print(f"  ✅ Created: 10 12 17 * * (12:10 PM, Europe/Zurich)")
    except Exception as e:
        print(f"  ❌ Error: {e}")
    
    print()
    
    # Verify
    print("=" * 80)
    print("VERIFICATION")
    print("=" * 80)
    print()
    
    updated_dep = await client.read_deployment(dep.id)
    print(f"{updated_dep.name}:")
    if updated_dep.schedules:
        for schedule in updated_dep.schedules:
            print(f"  ✅ {schedule.schedule.cron} ({schedule.schedule.timezone})")
    else:
        print(f"  ⚠️  No schedules")


if __name__ == "__main__":
    asyncio.run(update_currency_schedule())
