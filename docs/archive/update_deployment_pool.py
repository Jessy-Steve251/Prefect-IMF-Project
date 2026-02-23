"""
修复 Deployment 配置以支持本地代码执行
关键是：
1. 将 Work Pool 改为 'process' 类型（本地执行）
2. 或者配置代码上传到 Cloud
"""
import asyncio
from prefect.client.orchestration import get_client
from prefect.deployments import Deployment
from prefect.utilities.asyncutils import asynccontextmanager


async def fix_deployments_for_local_execution():
    """修复 Deployment 使其可以在本地执行"""
    client = get_client()
    
    print("=" * 70)
    print("Fixing Deployments for Local Execution")
    print("=" * 70)
    print()
    
    # 首先获取所有 Deployment
    deployments = await client.read_deployments()
    
    print(f"Found {len(deployments)} deployments\n")
    
    # 定义需要修复的部署
    deployments_config = [
        {
            "name": "currency-acquisition",
            "work_pool": "local-worker",  # 改用本地 Worker
        },
        {
            "name": "prepare-batch",
            "work_pool": "local-worker",
        },
        {
            "name": "process-batch",
            "work_pool": "local-worker",
        }
    ]
    
    # 更新每个 Deployment 的 Work Pool
    for dep_config in deployments_config:
        try:
            # 找到对应的 Deployment
            dep = None
            for d in deployments:
                if d.name == dep_config["name"]:
                    dep = d
                    break
            
            if not dep:
                print(f"❌ Deployment not found: {dep_config['name']}")
                continue
            
            # 更新 Work Pool
            updated_dep = await client.update_deployment(
                deployment_id=dep.id,
                work_pool_name=dep_config["work_pool"]
            )
            
            print(f"✅ {dep_config['name']}")
            print(f"   Work Pool: {dep_config['work_pool']}")
            print(f"   Entrypoint: {dep.entrypoint}")
            print()
            
        except Exception as e:
            print(f"❌ Error updating {dep_config['name']}: {str(e)}")
            print()
    
    print("=" * 70)
    print("Deployment Update Complete!")
    print("=" * 70)
    print()
    print("IMPORTANT: Now start the local worker in a separate terminal:")
    print()
    print("  prefect worker start --pool local-worker")
    print()


if __name__ == "__main__":
    asyncio.run(fix_deployments_for_local_execution())
