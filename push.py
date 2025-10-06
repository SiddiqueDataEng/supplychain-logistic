import os
import subprocess
from datetime import datetime, timedelta

# Path to the local folder (medallion folder)
local_folder_path = r"F:\Data Engineer for Supply Chain with Azure\medallion"  # Source folder
github_repo_url = "https://github.com/SiddiqueDataEng/supplychain-logistic.git"  # GitHub repository URL

# Ensure the folder exists
if not os.path.exists(local_folder_path):
    print(f"Error: The folder {local_folder_path} doesn't exist.")
    exit(1)

# Change directory to the local folder
os.chdir(local_folder_path)

# Initialize the git repository if not already initialized
subprocess.run(["git", "init"], check=True)

# Add remote if it doesn't exist
subprocess.run(["git", "remote", "add", "origin", github_repo_url], check=True)

# Ensure we add the files to the staging area
subprocess.run(["git", "add", "."], check=True)

# Check if there are files to commit
status = subprocess.run(["git", "status"], capture_output=True, text=True)
if "nothing to commit" in status.stdout:
    print("No files staged for commit. Please check your files.")
    exit(1)

# Backdate the commit (e.g., 10 days ago)
date_string = (datetime.now() - timedelta(days=10)).strftime("%a %b %d %H:%M:%S %Y")
print(f"Backdating commit to: {date_string}")

# Properly use the environment variable for backdated commit
env = os.environ.copy()
env["GIT_COMMITTER_DATE"] = date_string

# Commit the files with the backdate
subprocess.run(
    ["git", "commit", "--date", date_string, "-m", "Initial commit: Pushed all files with backdate"],
    env=env,
    check=True
)

# Push to GitHub repository
push_result = subprocess.run(
    ["git", "push", "-u", "origin", "main"], capture_output=True, text=True, check=True
)

# Print the result of the push attempt
if push_result.returncode == 0:
    print("Successfully pushed to GitHub!")
else:
    print("Error during push:", push_result.stderr)
