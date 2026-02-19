# create_github_block.py
from prefect_github import GitHubRepository

# Create GitHub block - note: use repository_url, not repository
github_block = GitHubRepository(
    repository_url='https://github.com/Jessy-Steve251/Prefect-IMF-Project.git',
    reference='main'
)

# Save the block
github_block.save('imf-github-repo', overwrite=True)

print('✅ GitHub block created successfully!')
print('Repository: Jessy-Steve251/Prefect-IMF-Project')
print('Reference: main')