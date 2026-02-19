from prefect import flow, get_run_logger
from utils.core_processor import run_core_processing
import os
from pathlib import Path


@flow(name="process_batch_flow", retries=2, retry_delay_seconds=10)
def process_batch_flow(manifest_file: str = ""):
    """
    This flow processes a complete batch using the MANIFEST.json.
    It is triggered automatically when a new manifest file is created.
    
    Args:
        manifest_file: Path to the manifest JSON file. If not provided, 
                      automatically finds the latest manifest in the hotfolder.
    """
    logger = get_run_logger()
    
    # If no manifest file provided, find the latest one
    if not manifest_file:
        hotfolder = os.path.join(os.getcwd(), "data_pipeline", "3_processing_hotfolder")
        os.makedirs(hotfolder, exist_ok=True)
        
        # Find all manifest files
        manifest_files = list(Path(hotfolder).glob("*_MANIFEST.json"))
        
        if not manifest_files:
            logger.error("No manifest files found in hotfolder")
            raise FileNotFoundError(f"No manifest files found in {hotfolder}")
        
        # Get the latest manifest file
        manifest_file = str(max(manifest_files, key=os.path.getctime))
        logger.info(f"Found latest manifest: {manifest_file}")
    
    logger.info(f"Processing batch from manifest: {manifest_file}")
    
    run_core_processing(manifest_file)
    
    logger.info("Batch processing completed successfully.")


if __name__ == "__main__":
    # for debugging only
    process_batch_flow()
