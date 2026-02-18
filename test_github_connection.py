from prefect import flow, task 
from prefect_github import GitHubRepository 
 
@task 
def test_connection(): 
    github_block = GitHubRepository.load("imf-github-repo") 
    return f"? Connected to: {github_block.repository_url}" 
 
@flow(name="test-github-setup") 
def test_flow(): 
    print("?? Testing GitHub connection...") 
    conn_result = test_connection() 
    print(conn_result) 
    print("\n? GitHub integration test complete!") 
 
if __name__ == "__main__": 
    test_flow() 
