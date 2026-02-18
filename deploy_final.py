#!/usr/bin/env python
"""Deploy flows using Prefect Python API"""

import asyncio
from pathlib import Path
from prefect.client.cloud_client import get_cloud_client
import subprocess
import sys

async def deploy_from_yaml():
    """Deploy using prefect CLI programmatically"""
    
    print("=" * 80)
    print("DEPLOYING FLOWS WITH GITHUB PULL STEPS")
    print("=" * 80)
    
    # Check prefect.yaml exists
    yaml_file = Path("prefect.yaml")
    if not yaml_file.exists():
        print("❌ prefect.yaml not found!")
        return False
    
    print(f"\n✅ Found prefect.yaml")
    
    # Try deploying each flow individually
    flows = [
        ("flows/currency_acquisition_flow.py", "currency_acquisition_flow", "currency-acquisition"),
        ("flows/prepare_batch_flow.py", "prepare_batch_flow", "prepare-batch"),
        ("flows/process_batch_flow.py", "process_batch_flow", "process-batch"),
    ]
    
    async with get_cloud_client() as client:
        try:
            # Get work pool
            wp = await client.read_work_pool("Yichen_Test")
            print(f"\n✅ Work pool found: {wp.name} ({wp.type})")
        except Exception as e:
            print(f"\n❌ Work pool not found: {e}")
            return False
    
    print("\n📦 Deploying flows:")
    for flow_path, flow_func, deploy_name in flows:
        print(f"\n   • {deploy_name}")
        
        # Build deployment command
        cmd = [
            sys.executable,
            "-m", "prefect",
            "deploy",
            f"{flow_path}:{flow_func}",
            "-n", deploy_name,
            "--pool", "Yichen_Test"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                print(f"      ✅ Deployed")
                # Show deployment URL from output
                if "Deployment" in result.stdout:
                    for line in result.stdout.split('\n'):
                        if "Deployment" in line or "URL" in line or "app.prefect" in line:
                            print(f"      {line.strip()}")
            else:
                print(f"      ⚠️  Status: {result.returncode}")
                if "already exists" in result.stderr or "already exists" in result.stdout:
                    print(f"      ℹ️  Deployment already exists (updating)")
                else:
                    error_line = result.stderr.split('\n')[0] if result.stderr else result.stdout.split('\n')[0]
                    print(f"      Error: {error_line[:100]}")
        except subprocess.TimeoutExpired:
            print(f"      ⏱️  Timeout (may still be deploying)")
        except Exception as e:
            print(f"      ❌ Error: {e}")
    
    print("\n" + "=" * 80)
    print("✅ DEPLOYMENT PROCESS COMPLETE")
    print("=" * 80)
    
    print("\n📋 GitHub Repository Setup:")
    print("   Repository: https://github.com/forg1ve1125/Prefect_Project")
    print("   Branch: main")
    
    print("\n📝 Next: Push your code to GitHub")
    print("   1. Create repository at: https://github.com/new")
    print("   2. Then run:")
    print()
    print("      git init")
    print("      git add .")
    print('      git commit -m "Initial Prefect deployment"')
    print("      git branch -M main")
    print("      git remote add origin https://github.com/forg1ve1125/Prefect_Project.git")
    print("      git push -u origin main")
    
    return True

if __name__ == "__main__":
    result = asyncio.run(deploy_from_yaml())
    sys.exit(0 if result else 1)
