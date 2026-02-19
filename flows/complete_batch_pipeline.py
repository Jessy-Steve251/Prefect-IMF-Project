from prefect import flow
from flows.prepare_batch_flow import prepare_batch_flow
from flows.process_batch_flow import process_batch_flow

@flow(name="complete_batch_pipeline")
def complete_batch_pipeline():
    """Run the complete batch pipeline in one flow"""
    
    # Step 1: Prepare batch - creates manifest
    manifest_path = prepare_batch_flow()
    
    # Step 2: Process batch using the manifest
    if manifest_path:
        process_batch_flow(manifest_file=manifest_path)
    
    return "Pipeline complete"

if __name__ == "__main__":
    complete_batch_pipeline()