#!/usr/bin/env python
"""Deploy flows with GitHub pull_steps - Updated for Prefect 3.x"""

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

    # Simplified command for Prefect 3.x
    cmd = [
        sys.executable,
        "-m", "prefect",
        "deploy",
        f"{flow_path}:{flow_name}",
        "-n", deployment_name,
        "--pool", "Yichen_Test"
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"   ✅ Success!")
        if result.stdout:
            print(f"   Output: {result.stdout.strip()}")
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Error: {e.stderr}")
        # Don't exit immediately, try the next flow
        continue

print("\n" + "=" * 80)
print("✅ ALL DEPLOYMENTS COMPLETE")
print("=" * 80)
print("\n🔗 GitHub Configuration:")
print("   Repository: https://github.com/Jessy-Steve251/Prefect-IMF-Project.git")
print("   Branch: main")
print("\n📋 Schedules (from prefect.yaml):")
print("   currency-acquisition: 12:10 on 17th of each month")
print("   prepare-batch: 12:30 on 17th of each month")
print("   process-batch: 13:00 on 17th of each month")
print("\n⚙️  Work Pool: Yichen_Test")
print("\n🚀 NEXT STEPS:")
print("   1. Start a worker: prefect worker start --pool Yichen_Test")
print("   2. Run a deployment: prefect deployment run 'currency-acquisition/currency-acquisition'")