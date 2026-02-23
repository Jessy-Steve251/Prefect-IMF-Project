"""
Update all deployment schedules to 17th of every month at 12:00 Europe/Zurich
"""

import asyncio
from prefect.client.orchestration import get_client


async def update_schedules():
    """Delete old schedules and create new ones"""
    
    client = get_client()
    
    print("=" * 80)
    print("UPDATING DEPLOYMENT SCHEDULES")
    print("=" * 80)
    print()
    
    # Get all deployments
    deployments = await client.read_deployments()
    
    print(f"Found {len(deployments)} deployment(s)")
    print()
    
    new_schedule_config = {
        "cron": "0 12 17 * *",
        "timezone": "Europe/Zurich"
    }
    
    print(f"New schedule: Every 17th of month at 12:00 (Europe/Zurich)")
    print(f"Cron expression: {new_schedule_config['cron']}")
    print()
    
    for dep in deployments:
        print(f"Updating: {dep.name}")
        
        # Delete old schedules
        if dep.schedules:
            print(f"  Deleting {len(dep.schedules)} old schedule(s)...")
            for schedule in dep.schedules:
                try:
                    await client.delete_deployment_schedule(
                        deployment_id=dep.id,
                        schedule_id=schedule.id
                    )
                    print(f"    ✅ Deleted: {schedule.schedule.cron}")
                except Exception as e:
                    print(f"    ❌ Error: {e}")
        
        # Create new schedule
        print(f"  Creating new schedule...")
        try:
            result = await client.create_deployment_schedules(
                deployment_id=dep.id,
                schedules=[({
                    "cron": new_schedule_config["cron"],
                    "timezone": new_schedule_config["timezone"]
                }, True)]
            )
            print(f"    ✅ Created: {new_schedule_config['cron']} (Europe/Zurich)")
        except Exception as e:
            print(f"    ❌ Error creating schedule: {e}")
        
        print()
    
    # Verify
    print("=" * 80)
    print("VERIFICATION")
    print("=" * 80)
    print()
    
    updated_deployments = await client.read_deployments()
    
    for dep in updated_deployments:
        print(f"{dep.name}:")
        if dep.schedules:
            for schedule in dep.schedules:
                tz = schedule.schedule.timezone
                cron = schedule.schedule.cron
                active = schedule.active
                print(f"  ✅ {cron} (Timezone: {tz}, Active: {active})")
        else:
            print(f"  ⚠️  No schedules")
        print()
    
    print("=" * 80)
    print("SCHEDULE UPDATE COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(update_schedules())
