"""
Update schedules for prepare-batch (12:30) and process-batch (13:00)
"""

import asyncio
from prefect.client.orchestration import get_client


async def update_specific_schedules():
    """Update only prepare-batch and process-batch schedules"""
    
    client = get_client()
    
    print("=" * 80)
    print("UPDATING SCHEDULES FOR 2 DEPLOYMENTS")
    print("=" * 80)
    print()
    
    # Get all deployments
    deployments = {dep.name: dep for dep in await client.read_deployments()}
    
    schedule_updates = {
        "prepare-batch": {
            "cron": "30 12 17 * *",
            "timezone": "Europe/Zurich",
            "time": "12:30 PM"
        },
        "process-batch": {
            "cron": "0 13 17 * *",
            "timezone": "Europe/Zurich",
            "time": "13:00 (1:00 PM)"
        }
    }
    
    for dep_name, schedule_config in schedule_updates.items():
        print(f"Updating: {dep_name}")
        print(f"  New time: {schedule_config['time']}")
        print(f"  Cron: {schedule_config['cron']}")
        
        if dep_name not in deployments:
            print(f"  ❌ Deployment not found")
            print()
            continue
        
        dep = deployments[dep_name]
        
        # Delete old schedules
        if dep.schedules:
            print(f"  Deleting old schedule(s)...")
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
                    "cron": schedule_config["cron"],
                    "timezone": schedule_config["timezone"]
                }, True)]
            )
            print(f"    ✅ Created: {schedule_config['cron']}")
        except Exception as e:
            print(f"    ❌ Error: {e}")
        
        print()
    
    # Verify
    print("=" * 80)
    print("VERIFICATION")
    print("=" * 80)
    print()
    
    updated_deployments = {dep.name: dep for dep in await client.read_deployments()}
    
    for dep_name in ["currency-acquisition", "prepare-batch", "process-batch"]:
        if dep_name in updated_deployments:
            dep = updated_deployments[dep_name]
            print(f"{dep_name}:")
            if dep.schedules:
                for schedule in dep.schedules:
                    cron = schedule.schedule.cron
                    tz = schedule.schedule.timezone
                    print(f"  ✅ {cron} (Timezone: {tz})")
            else:
                print(f"  ⚠️  No schedules")
            print()
    
    print("=" * 80)
    print("UPDATE COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(update_specific_schedules())
