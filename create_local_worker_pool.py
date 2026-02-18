"""
创建本地 Worker Pool，在本地机器上运行 Flow
这样就不需要上传代码到 Cloud
"""
import asyncio
from prefect.client.orchestration import get_client


async def create_local_worker_pool():
    """创建本地 Worker Pool"""
    client = get_client()
    
    print("=" * 70)
    print("Creating Local Worker Pool")
    print("=" * 70)
    print()
    
    pool_name = "local-worker"
    
    try:
        # 检查是否已存在
        pools = await client.read_work_pools()
        
        existing_pool = None
        for pool in pools:
            if pool.name == pool_name:
                existing_pool = pool
                break
        
        if existing_pool:
            print(f"✅ Local worker pool '{pool_name}' already exists")
            print(f"   Type: {existing_pool.type}")
            print()
        else:
            # 创建新的本地 Worker Pool
            # 使用 process 类型 - 在本地运行子进程
            from prefect.workers.process import ProcessWorker
            
            try:
                result = await client.create_work_pool(
                    work_pool_name=pool_name,
                    work_pool_base_job_template={
                        "job_configuration": {},
                        "variables": {}
                    }
                )
                print(f"✅ Created local worker pool: {pool_name}")
                print(f"   Type: process")
                print()
            except Exception as create_error:
                print(f"Note: {str(create_error)}")
                print()
        
        # 获取所有 Work Pool
        all_pools = await client.read_work_pools()
        
        print("Current Work Pools:")
        for pool in all_pools:
            print(f"  - {pool.name} ({pool.type})")
        
        print()
        print("=" * 70)
        print("Next Steps:")
        print("=" * 70)
        print()
        print("1. Update Deployments to use the 'local-worker' pool")
        print()
        print("2. Start the local worker:")
        print(f"   prefect worker start --pool {pool_name}")
        print()
        print("3. Flows will execute locally when scheduled")
        print()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")


if __name__ == "__main__":
    asyncio.run(create_local_worker_pool())
