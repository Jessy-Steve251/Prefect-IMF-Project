from prefect_github import GitHubRepository 
 
def create_github_block(): 
    repo_name = "Jessy-Steve251/Prefect-IMF-Project" 
    branch = "main" 
    github_block = GitHubRepository( 
        repository_url=f"https://github.com/{repo_name}.git", 
        reference=branch 
    ) 
    block_name = "imf-github-repo" 
    github_block.save(block_name, overwrite=True) 
    print(f"? GitHub block '{block_name}' created successfully!") 
    print(f"Repository: {repo_name}") 
    return github_block 
 
def test_github_block(): 
    try: 
        github_block = GitHubRepository.load("imf-github-repo") 
        print("? Successfully loaded GitHub block") 
        print(f"Repository URL: {github_block.repository_url}") 
        print(f"Reference: {github_block.reference}") 
        return True 
    except Exception as e: 
        print(f"? Failed to load GitHub block: {e}") 
        return False 
 
if __name__ == "__main__": 
    create_github_block() 
    test_github_block() 
