"""
Final verification script for Prefect deployment fix
Checks all deployments, their paths, entrypoints, and schedules
"""

import asyncio
from prefect.client.orchestration import get_client
from pathlib import Path


async def final_verification():
    """Verify all deployments are correctly configured"""
    
    client = get_client()
    
    print("=" * 80)
    print("FINAL PREFECT DEPLOYMENT VERIFICATION")
    print("=" * 80)
    print()
    
    # Get project path
    project_path = Path.cwd()
    
    # Get all deployments
    deployments = await client.read_deployments()
    
    print(f"Project Path: {project_path}")
    print(f"Total Deployments: {len(deployments)}")
    print()
    
    # Check each deployment
    all_valid = True
    
    for i, dep in enumerate(deployments, 1):
        print(f"Deployment {i}: {dep.name}")
        print("-" * 80)
        
        # Check path
        print(f"  Path: {dep.path}")
        if str(project_path) in dep.path or dep.path == str(project_path):
            print(f"    ✅ Path is correct")
        else:
            print(f"    ⚠️  Path may need review")
            all_valid = False
        
        # Check entrypoint
        print(f"  Entrypoint: {dep.entrypoint}")
        if dep.entrypoint and "flows/" in dep.entrypoint:
            # Check if the file exists
            flow_file = project_path / dep.entrypoint.split(":")[0]
            if flow_file.exists():
                print(f"    ✅ Flow file exists: {flow_file}")
            else:
                print(f"    ❌ Flow file not found: {flow_file}")
                all_valid = False
        else:
            print(f"    ❌ Invalid entrypoint format")
            all_valid = False
        
        # Check work pool
        print(f"  Work Pool: {dep.work_pool_name}")
        if dep.work_pool_name:
            print(f"    ✅ Work pool assigned")
        else:
            print(f"    ❌ No work pool assigned")
            all_valid = False
        
        # Check schedules
        print(f"  Schedules: {len(dep.schedules)}")
        if dep.schedules:
            for schedule in dep.schedules:
                print(f"    ✅ {schedule.schedule.cron} (TZ: {schedule.schedule.timezone}, Active: {schedule.active})")
        else:
            print(f"    ⚠️  No schedules configured")
        
        print()
    
    # Final summary
    print("=" * 80)
    print("VERIFICATION SUMMARY")
    print("=" * 80)
    print()
    
    if all_valid:
        print("✅ ALL CHECKS PASSED")
        print()
        print("Next steps:")
        print("  1. Test by triggering a flow run in Prefect Cloud")
        print("  2. Monitor the logs to ensure flow files are loaded correctly")
        print("  3. Verify that the error is resolved")
        print()
        print("To trigger a test run from the command line:")
        print('  prefect deployment run "currency-acquisition"')
    else:
        print("⚠️  SOME CHECKS FAILED - REVIEW NEEDED")
        print()
        print("Issues found:")
        for i, dep in enumerate(deployments, 1):
            issues = []
            
            if dep.path != str(project_path):
                issues.append(f"  - Path mismatch: {dep.path} vs {project_path}")
            
            if not dep.entrypoint:
                issues.append("  - Missing entrypoint")
            elif "flows/" in dep.entrypoint:
                flow_file = project_path / dep.entrypoint.split(":")[0]
                if not flow_file.exists():
                    issues.append(f"  - Flow file not found: {flow_file}")
            
            if not dep.work_pool_name:
                issues.append("  - No work pool assigned")
            
            if issues:
                print(f"\n{dep.name}:")
                for issue in issues:
                    print(issue)
    
    print()
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(final_verification())
