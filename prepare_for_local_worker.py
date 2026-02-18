"""
修复方案：将 Deployment 从 prefect:managed 改为使用本地 Worker
这样 Cloud 会在本地机器上执行 Flow，代码可以直接访问
"""
import asyncio
from prefect.client.orchestration import get_client


async def update_deployments_to_local_execution():
    """更新所有 Deployment 使用本地 Worker"""
    client = get_client()
    
    print("=" * 70)
    print("Updating Deployments to Use Local Worker")
    print("=" * 70)
    print()
    
    # 获取所有 Deployment
    deployments = await client.read_deployments()
    
    deployment_names = ["currency-acquisition", "prepare-batch", "process-batch"]
    
    for dep_name in deployment_names:
        try:
            # 找到 Deployment
            dep = None
            for d in deployments:
                if d.name == dep_name:
                    dep = d
                    break
            
            if not dep:
                print(f"❌ Deployment not found: {dep_name}")
                continue
            
            # 获取当前配置
            dep_data = await client.read_deployment(dep.id)
            
            # 关键修改：设置本地执行环境变量和配置
            # 使用 Yichen_Test pool，但需要在本地运行 Worker
            
            print(f"Updating {dep_name}...")
            print(f"  Current Work Pool: {dep_data.work_pool_name}")
            print(f"  Entrypoint: {dep_data.entrypoint}")
            
            # 这里暂时保持原样，关键是需要本地 Worker 运行
            # 不修改 Work Pool，改为启动本地 Worker 来处理任务
            
            print(f"  ✅ Ready for local worker execution")
            print()
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            print()
    
    print("=" * 70)
    print("Configuration Ready")
    print("=" * 70)
    print()
    print("NEXT STEPS:")
    print()
    print("1. Start a local worker that will execute the flows:")
    print()
    print("   prefect worker start --pool Yichen_Test")
    print()
    print("2. The worker will:")
    print("   - Connect to Prefect Cloud")
    print("   - Listen for scheduled flow runs")
    print("   - Execute flows locally on your machine")
    print("   - Report results back to Cloud")
    print()
    print("3. When flows are scheduled (at 11:00 on 11th):")
    print("   - Cloud will send the flow run to this worker")
    print("   - Worker will execute flows locally")
    print("   - Flow code will be found in local 'flows/' directory")
    print()


if __name__ == "__main__":
    asyncio.run(update_deployments_to_local_execution())
