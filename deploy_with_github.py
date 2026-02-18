#!/usr/bin/env python
"""Deploy flows with GitHub pull_steps"""

import subprocess
import sys

# Deploy each flow
flows = [
    ("flows/currency_acquisition_flow.py", "currency_acquisition_flow", "currency-acquisition"),
    ("flows/prepare_batch_flow.py", "prepare_batch_flow", "prepare-batch"),
    ("flows/process_batch_flow.py", "process_batch_flow", "process-batch"),
]

print("=" * 80)
print("DEPLOYING WITH GITHUB PULL_STEPS")
print("=" * 80)

for flow_path, flow_name, deployment_name in flows:
    print(f"\n📦 Deploying: {deployment_name}")
    print(f"   Flow: {flow_name}")
    print(f"   File: {flow_path}")
    
    cmd = [
        sys.executable,
        "-m", "prefect",
        "deploy",
        f"{flow_path}:{flow_name}",
        "-n", deployment_name,
        "--work-pool", "Yichen_Test",
        "-q"  # Quiet mode
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"   ✅ Success!")
        if result.stdout:
            print(f"   Output: {result.stdout.strip()}")
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Error: {e.stderr}")
        sys.exit(1)

print("\n" + "=" * 80)
print("✅ ALL DEPLOYMENTS COMPLETE")
print("=" * 80)
print("\n🔗 GitHub Configuration:")
print("   Repository: https://github.com/forg1ve1125/Prefect_Project")
print("   Branch: main")
print("\n📋 Schedules:")
print("   currency-acquisition: 12:10 on 17th of each month")
print("   prepare-batch: 12:30 on 17th of each month")
print("   process-batch: 13:00 on 17th of each month")
print("\n⚠️  NEXT STEPS:")
print("   1. Create GitHub repository: https://github.com/new")
print("   2. Push code to GitHub:")
print("      git init")
print("      git add .")
print("      git commit -m 'Initial Prefect deployment'")
print('      git branch -M main')
print("      git remote add origin https://github.com/forg1ve1125/Prefect_Project.git")
print("      git push -u origin main")
