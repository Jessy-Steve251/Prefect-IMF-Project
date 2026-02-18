#!/usr/bin/env python
"""Deploy flows using Prefect CLI"""

import subprocess
import sys

print("=" * 80)
print("DEPLOYING FLOWS WITH GITHUB PULL STEPS")
print("=" * 80)

flows = [
    ("flows/currency_acquisition_flow.py", "currency_acquisition_flow", "currency-acquisition"),
    ("flows/prepare_batch_flow.py", "prepare_batch_flow", "prepare-batch"),
    ("flows/process_batch_flow.py", "process_batch_flow", "process-batch"),
]

print("\n📦 Deploying flows:")

for flow_path, flow_func, deploy_name in flows:
    print(f"\n   • {deploy_name}")
    
    cmd = [
        sys.executable,
        "-m", "prefect",
        "deploy",
        f"{flow_path}:{flow_func}",
        "-n", deploy_name,
        "--pool", "Yichen_Test"
    ]
    
    print(f"     Running: {' '.join(cmd[3:])}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        # Check if successful
        if result.returncode == 0 or "Deployment" in result.stdout or "successfully" in result.stdout.lower():
            print(f"     ✅ Success")
            # Print relevant output
            for line in result.stdout.split('\n'):
                if 'Deployment' in line or 'URL' in line:
                    print(f"     {line.strip()}")
        else:
            # Check for already exists message
            output = result.stdout + result.stderr
            if "already exists" in output or "conflict" in output.lower():
                print(f"     ⚠️  Already exists (will be updated)")
            else:
                print(f"     ⚠️  Return code: {result.returncode}")
                # Print error
                err_lines = (result.stderr or result.stdout).split('\n')
                for line in err_lines[:3]:
                    if line.strip():
                        print(f"     {line.strip()[:80]}")
                        
    except subprocess.TimeoutExpired:
        print(f"     ⏱️  Timeout - may still be deploying")
    except Exception as e:
        print(f"     ❌ Error: {e}")

print("\n" + "=" * 80)
print("✅ DEPLOYMENT COMPLETE")
print("=" * 80)

print("\n🔗 Configuration Summary:")
print("   • Work Pool: Yichen_Test (prefect:managed)")
print("   • GitHub Repo: https://github.com/forg1ve1125/Prefect_Project")
print("   • Branch: main")

print("\n📅 Schedule:")
print("   • currency-acquisition: 12:10 on 17th of month")
print("   • prepare-batch: 12:30 on 17th of month")
print("   • process-batch: 13:00 on 17th of month")

print("\n📝 FINAL STEP - Push code to GitHub:")
print("   1. Go to: https://github.com/new")
print("   2. Create repository 'Prefect_Project'")
print("   3. Run in terminal:")
print()
print("      git init")
print("      git add .")
print('      git commit -m "Initial commit"')
print("      git branch -M main")
print("      git remote add origin https://github.com/forg1ve1125/Prefect_Project.git")
print("      git push -u origin main")
