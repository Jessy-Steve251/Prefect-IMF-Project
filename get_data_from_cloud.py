import asyncio
import pandas as pd
import json
import os
from prefect import get_client
import sys

async def download_artifact_data():
    """
    Download the 'exchange-rates-data' artifact from Prefect Cloud
    and save it as a CSV file locally.
    """
    print("Connecting to Prefect...")
    
    async with get_client() as client:
        try:
            # 1. Fetch the artifact by its key
            artifact_key = "exchange-rates-data"
            
            print(f"Searching for artifact with key: '{artifact_key}'...")
            
            # Using the correct method to read the artifact
            try:
                # Attempt 1: Direct client method
                if hasattr(client, 'read_artifact'):
                    artifact = await client.read_artifact(key=artifact_key)
                
                # Attempt 2: Via read_artifacts with filter
                else:
                    from prefect.client.schemas.filters import ArtifactFilter, ArtifactFilterKey
                    
                    artifacts = await client.read_artifacts(
                        artifact_filter=ArtifactFilter(
                            key=ArtifactFilterKey(like_=artifact_key)
                        ),
                        limit=1,
                        sort="CREATED_DESC"
                    )
                    artifact = artifacts[0] if artifacts else None

            except Exception as read_err:
                print(f"Debug Info - Read Error: {read_err}")
                raise read_err
            
            if not artifact:
                print("❌ Artifact not found. Please check if the Flow ran successfully.")
                return

            # 2. Parse the data
            print("Artifact found! Downloading data...")
            data = artifact.data
            
            # If data is a string, try to parse it as JSON
            if isinstance(data, str):
                data = json.loads(data)

            # 3. Convert to DataFrame
            df = pd.DataFrame(data)
            
            # 4. Save to CSV
            output_dir = "data"
            os.makedirs(output_dir, exist_ok=True)
            output_filename = os.path.join(output_dir, "downloaded_exchange_rates.csv")
            
            df.to_csv(output_filename, index=False)
            
            print(f"\n✅ Success! Data exported to: {output_filename}")
            print(f"Total rows: {len(df)}")
            print("\nPreview:")
            print(df.head())

        except Exception as e:
            print(f"\n❌ Error occurred: {e}")
            print("Make sure your Prefect server is running and you're logged in.")

if __name__ == "__main__":
    asyncio.run(download_artifact_data())