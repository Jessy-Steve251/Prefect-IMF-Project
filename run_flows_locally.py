#!/usr/bin/env python
"""
本地 Flow 执行脚本
直接在本地运行 Flow，而不依赖 Cloud 托管执行
可以通过 Task Scheduler 或 cron 调度执行
"""
import asyncio
from flows.currency_acquisition_flow import currency_acquisition_flow
from flows.prepare_batch_flow import prepare_batch_flow
from flows.process_batch_flow import process_batch_flow
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_currency_acquisition():
    """运行汇率获取 Flow"""
    logger.info("=" * 70)
    logger.info("Starting: currency_acquisition_flow")
    logger.info("=" * 70)
    
    try:
        result = currency_acquisition_flow()
        logger.info(f"✅ currency_acquisition_flow completed successfully")
        logger.info(f"Result: {result}")
        return True
    except Exception as e:
        logger.error(f"❌ currency_acquisition_flow failed: {str(e)}")
        return False


async def run_prepare_batch():
    """运行批次准备 Flow"""
    logger.info("=" * 70)
    logger.info("Starting: prepare_batch_flow")
    logger.info("=" * 70)
    
    try:
        result = prepare_batch_flow()
        logger.info(f"✅ prepare_batch_flow completed successfully")
        logger.info(f"Result: {result}")
        return True
    except Exception as e:
        logger.error(f"❌ prepare_batch_flow failed: {str(e)}")
        return False


async def run_process_batch():
    """运行批次处理 Flow"""
    logger.info("=" * 70)
    logger.info("Starting: process_batch_flow")
    logger.info("=" * 70)
    
    try:
        result = process_batch_flow()
        logger.info(f"✅ process_batch_flow completed successfully")
        logger.info(f"Result: {result}")
        return True
    except Exception as e:
        logger.error(f"❌ process_batch_flow failed: {str(e)}")
        return False


async def main():
    """主程序 - 按顺序执行所有 Flow"""
    logger.info("")
    logger.info("╔" + "=" * 68 + "╗")
    logger.info("║" + " " * 68 + "║")
    logger.info("║" + "  Prefect Local Flow Execution".center(68) + "║")
    logger.info("║" + f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(68) + "║")
    logger.info("║" + " " * 68 + "║")
    logger.info("╚" + "=" * 68 + "╝")
    logger.info("")
    
    results = {}
    
    # 执行流程 1: 汇率获取
    results['currency_acquisition'] = await run_currency_acquisition()
    logger.info("")
    
    # 执行流程 2: 批次准备
    results['prepare_batch'] = await run_prepare_batch()
    logger.info("")
    
    # 执行流程 3: 批次处理
    results['process_batch'] = await run_process_batch()
    logger.info("")
    
    # 总结
    logger.info("=" * 70)
    logger.info("Execution Summary")
    logger.info("=" * 70)
    for flow_name, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        logger.info(f"{flow_name:25} {status}")
    logger.info("=" * 70)
    logger.info("")


if __name__ == "__main__":
    asyncio.run(main())
