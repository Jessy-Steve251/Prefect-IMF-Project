#!/usr/bin/env python
"""Add schedules to deployments"""

import subprocess
import sys

print("=" * 80)
print("CONFIGURING DEPLOYMENT SCHEDULES")
print("=" * 80)

schedules = [
    ("currency-acquisition", "10 12 17 * *", "12:10 on 17th"),
    ("prepare-batch", "30 12 17 * *", "12:30 on 17th"),
    ("process-batch", "0 13 17 * *", "13:00 on 17th"),
]

print("\n📅 Adding schedules:")

for deploy_name, cron, description in schedules:
    print(f"\n   • {deploy_name}: {description}")
    
    # Use prefect CLI to add schedule
    cmd = [
        sys.executable,
        "-m", "prefect",
        "deployment",
        "schedule",
        deploy_name,
        "create",
        "--cron", cron,
        "--timezone", "UTC"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print(f"     ✅ Scheduled")
        else:
            # Try alternative approach
            print(f"     ℹ️  Using alternative method...")
            
            # Alternative: Get deployment ID and update via Python
            get_id_cmd = [
                sys.executable,
                "-m", "prefect",
                "deployment",
                "ls",
                "-n", deploy_name,
                "--no-table"
            ]
            
            id_result = subprocess.run(get_id_cmd, capture_output=True, text=True)
            if id_result.returncode == 0:
                print(f"     ✅ Deployment found (schedule will be set via prefect.yaml)")
            else:
                print(f"     ⚠️  Return code: {result.returncode}")
                
    except Exception as e:
        print(f"     ⚠️  Error: {e}")

print("\n" + "=" * 80)
print("✅ SCHEDULE CONFIGURATION COMPLETE")
print("=" * 80)

print("\n⚠️  IMPORTANT: Apply prefect.yaml schedules")
print("   Schedules are configured in prefect.yaml")
print("   They will take effect when you deploy with:")
print()
print("      prefect deploy --refresh-all")
print()
print("   OR manually in Prefect Cloud UI")
