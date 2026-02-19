# create_github_secret.py
from prefect.blocks.system import Secret

# Replace with your actual GitHub token
YOUR_TOKEN = "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"

# Create and save the secret block
secret_block = Secret(value=YOUR_TOKEN)
secret_block.save(name="github-token", overwrite=True)

print("✅ GitHub token saved as secret block 'github-token'")
print("You can now use this in your prefect.yaml with: {{ prefect.blocks.secret.github-token }}")