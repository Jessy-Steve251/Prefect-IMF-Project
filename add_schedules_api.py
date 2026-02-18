#!/usr/bin/env python
"""Add schedules to deployments via API"""

from prefect import get_client
import asyncio
from datetime import datetime
import sys

async def add_schedules():
    """Add schedules to all deployments"""
    
    async with get_client() as client:
        deployments_info = [
            ("currency_acquisition_flow/currency-acquisition", "10 12 17 * *", "12:10 on 17th"),
            ("prepare_batch_flow/prepare-batch", "30 12 17 * *", "12:30 on 17th"),
            ("process_batch_flow/process-batch", "0 13 17 * *", "13:00 on 17th"),
        ]
        
        print("=" * 80)
        print("ADDING SCHEDULES TO DEPLOYMENTS")
        print("=" * 80)
        
        for deploy_name, cron, description in deployments_info:
            print(f"\n📅 {deploy_name}")
            print(f"   Schedule: {description}")
            print(f"   Cron: {cron}")
            
            try:
                # Get deployment
                deployments = await client.read_deployments(
                    deployment_filter={"name": {"like_": deploy_name}}
                )
                
                if not deployments:
                    print(f"   ⚠️  Deployment not found")
                    continue
                
                deployment = deployments[0]
                deployment_id = deployment.id
                
                print(f"   ✅ Found (ID: {str(deployment_id)[:8]}...)")
                
                # Create schedule
                from prefect.client.schemas.objects import DeploymentSchedule
                from prefect.client.schemas.schedules import CronSchedule
                
                schedule = DeploymentSchedule(
                    schedule=CronSchedule(cron=cron, timezone="UTC"),
                    active=True
                )
                
                result = await client.create_deployment_schedule(
                    deployment_id=deployment_id,
                    schedule=schedule
                )
                
                print(f"   ✅ Schedule added!")
                
            except Exception as e:
                error_msg = str(e)
                if "already" in error_msg.lower():
                    print(f"   ⚠️  Schedule already exists")
                else:
                    print(f"   ❌ Error: {error_msg[:100]}")
        
        print("\n" + "=" * 80)
        print("✅ SCHEDULE SETUP COMPLETE")
        print("=" * 80)

if __name__ == "__main__":
    try:
        asyncio.run(add_schedules())
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\n⚠️  Manually add schedules in Prefect Cloud UI:")
        print("   1. Go to Deployments page")
        print("   2. Click each deployment")
        print("   3. Add schedule with cron expressions")
        sys.exit(1)
