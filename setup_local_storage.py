"""
为 Deployment 配置本地文件存储
这样 Cloud 会从本地下载代码
"""
import asyncio
import json
from prefect.client.orchestration import get_client
from prefect.filesystems import LocalFileSystem


async def setup_local_storage():
    """配置本地文件系统存储块"""
    client = get_client()
    
    print("=" * 70)
    print("Setting up Local File System Storage")
    print("=" * 70)
    print()
    
    try:
        # 创建本地文件存储块
        block_type = "local-file-system"
        block_name = "project-code"
        
        # 检查是否已存在
        existing_blocks = await client.read_blocks_of_type(block_type)
        print(f"Checking for existing {block_name} storage block...")
        
        block_id = None
        for block in existing_blocks:
            if block.name == block_name:
                block_id = block.id
                print(f"✅ Found existing storage block: {block_name}")
                break
        
        if not block_id:
            print(f"Creating new {block_name} storage block...")
            # 使用 LocalFileSystem 创建存储块
            # 需要使用块的 API 端点
            
            # 获取项目根目录
            import os
            project_path = os.path.abspath(os.getcwd())
            print(f"Project path: {project_path}")
            
            # 创建存储块 - 使用 REST API 调用
            block_data = {
                "block_type_slug": "local-file-system",
                "block_document_name": block_name,
                "data": {
                    "basepath": project_path
                }
            }
            
            try:
                response = await client._post(
                    "/block_documents/",
                    json=block_data
                )
                print(f"✅ Created storage block: {block_name}")
            except Exception as e:
                # 可能已存在，忽略
                print(f"Note: {str(e)}")
        
        print()
        print("=" * 70)
        print("Storage Block Setup Complete")
        print("=" * 70)
        print()
        print("Now you need to update Deployments to use this storage block.")
        print()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")


if __name__ == "__main__":
    asyncio.run(setup_local_storage())
