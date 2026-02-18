#!/usr/bin/env python
"""Deploy flows from prefect.yaml"""

import subprocess
import sys
import os

os.chdir('c:\\Users\\yli\\Desktop\\Prefect_Project')

print("=" * 80)
print("DEPLOYING ALL FLOWS FROM prefect.yaml")
print("=" * 80)

# Use prefect deploy with yaml
cmd = [
    sys.executable,
    "-m", "prefect",
    "deploy",
    "--prefect-file", "prefect.yaml",
    "--ignore-warnings"
]

print("\n📦 Deploying flows...")
print(f"   Command: {' '.join(cmd)}\n")

try:
    result = subprocess.run(cmd, capture_output=False, text=True)
    
    if result.returncode == 0:
        print("\n" + "=" * 80)
        print("✅ DEPLOYMENT SUCCESSFUL")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("⚠️  Deployment completed with status code:", result.returncode)
        print("=" * 80)
        
except Exception as e:
    print(f"\n❌ Error: {e}")
    sys.exit(1)

print("\n📋 NEXT STEPS:")
print("   1. Create GitHub repository: https://github.com/new")
print("      Name: Prefect_Project")
print("   2. Initialize and push code:")
print()
print("      git init")
print("      git add .")
print("      git commit -m 'Initial Prefect deployment'")
print("      git branch -M main")
print("      git remote add origin https://github.com/forg1ve1125/Prefect_Project.git")
print("      git push -u origin main")
print()
print("   3. Check Prefect Cloud to verify deployments are ready")
print("      URL: https://app.prefect.cloud")
print()
print("   4. Flows will run on schedule:")
print("      • currency-acquisition: 12:10 on 17th of each month")
print("      • prepare-batch: 12:30 on 17th of each month")
print("      • process-batch: 13:00 on 17th of each month")
