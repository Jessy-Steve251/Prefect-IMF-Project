from prefect import flow, get_run_logger
from utils.batch_prepare import create_batch_manifest


@flow(name="prepare_batch_flow")
def prepare_batch_flow():
    """
    Step 1: Manually triggered flow.
    Generates the MANIFEST file after preparing all prerequisite
    intermediate files.
    """
    logger = get_run_logger()
    logger.info("Starting batch preparation...")

    manifest_path = create_batch_manifest()

    logger.info(f"Batch preparation completed. Manifest created at: {manifest_path}")

    return manifest_path


if __name__ == "__main__":
    prepare_batch_flow()
