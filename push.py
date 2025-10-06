import os
import subprocess
import sys

# Path to the local folder (medallion folder)
local_folder_path = r"F:\Data Engineer for Supply Chain with Azure\medallion"  # Source folder
github_repo_url = "https://github.com/SiddiqueDataEng/supplychain-logistic.git"  # GitHub repository URL

def run(cmd, check=True, capture=False, text=True):
    return subprocess.run(cmd, check=check, capture_output=capture, text=text)

def get_output(cmd):
    result = run(cmd, check=False, capture=True)
    return (result.stdout or "").strip(), (result.stderr or "").strip(), result.returncode

def ensure_repo_initialized():
    stdout, _, code = get_output(["git", "rev-parse", "--is-inside-work-tree"])
    if code != 0 or stdout != "true":
        # Initialize with main as default branch when supported
        init_cmd = ["git", "init", "-b", "main"]
        result = run(init_cmd, check=False)
        if result.returncode != 0:
            # Fallback for older Git that doesn't support -b
            run(["git", "init"], check=True)
            run(["git", "checkout", "-B", "main"], check=True)

def ensure_user_config():
    name, _, _ = get_output(["git", "config", "user.name"])
    email, _, _ = get_output(["git", "config", "user.email"])
    if not name:
        # Set a placeholder; user can change later
        run(["git", "config", "user.name", "Automated Uploader"], check=True)
    if not email:
        run(["git", "config", "user.email", "uploader@example.com"], check=True)

def ensure_remote_origin(url):
    stdout, _, code = get_output(["git", "remote", "get-url", "origin"])
    if code != 0:
        # Add origin
        run(["git", "remote", "add", "origin", url], check=True)
    elif stdout != url:
        # Update origin URL if different
        run(["git", "remote", "set-url", "origin", url], check=True)

def ensure_main_branch():
    # Ensure we're on main
    branch, _, _ = get_output(["git", "symbolic-ref", "--short", "HEAD"])
    if branch != "main":
        run(["git", "checkout", "-B", "main"], check=True)

def ensure_gitignore_exclusions(exclusions):
    gitignore_path = os.path.join(local_folder_path, ".gitignore")
    existing = set()
    if os.path.exists(gitignore_path):
        try:
            with open(gitignore_path, "r", encoding="utf-8") as f:
                existing = set(line.strip() for line in f if line.strip())
        except Exception:
            existing = set()
    new_lines = []
    for item in exclusions:
        if item not in existing:
            new_lines.append(item)
    if new_lines:
        with open(gitignore_path, "a", encoding="utf-8") as f:
            if os.path.getsize(gitignore_path) > 0:
                f.write("\n")
            f.write("\n".join(new_lines) + "\n")

def stage_and_commit_if_needed():
    ensure_gitignore_exclusions(["supplychain-logistics/"])
    run(["git", "add", ".gitignore"], check=False)
    run(["git", "add", "."], check=True)
    status_out, _, _ = get_output(["git", "status", "--porcelain"])
    if not status_out:
        print("Nothing to commit. Proceeding to push current state.")
        return False
    run(["git", "commit", "-m", "Update: push project files"], check=True)
    return True

def push_main():
    # Ensure upstream and push
    # Try setting upstream if not set
    _, _, code = get_output(["git", "rev-parse", "--verify", "@{u}"])
    set_upstream = code != 0
    push_cmd = ["git", "push"] + (["-u", "origin", "main"] if set_upstream else [])
    result = run(push_cmd, check=False, capture=True)
    if result.returncode != 0:
        print("Error during push:\n" + (result.stderr or result.stdout))
        print("Hint: Ensure you are authenticated with GitHub (e.g., using Git Credential Manager or a PAT).")
        sys.exit(1)
    print("Successfully pushed to GitHub!")

def main():
    # Ensure the folder exists
    if not os.path.exists(local_folder_path):
        print(f"Error: The folder {local_folder_path} doesn't exist.")
        sys.exit(1)

    os.chdir(local_folder_path)
    ensure_repo_initialized()
    ensure_user_config()
    ensure_remote_origin(github_repo_url)
    ensure_main_branch()
    staged = stage_and_commit_if_needed()
    if staged:
        print("Committed local changes.")
    push_main()

if __name__ == "__main__":
    main()

def ensure_gitignore_exclusions(exclusions):
    gitignore_path = os.path.join(local_folder_path, ".gitignore")
    existing = set()
    if os.path.exists(gitignore_path):
        try:
            with open(gitignore_path, "r", encoding="utf-8") as f:
                existing = set(line.strip() for line in f if line.strip())
        except Exception:
            existing = set()
    new_lines = []
    for item in exclusions:
        if item not in existing:
            new_lines.append(item)
    if new_lines:
        with open(gitignore_path, "a", encoding="utf-8") as f:
            if os.path.getsize(gitignore_path) > 0:
                f.write("\n")
            f.write("\n".join(new_lines) + "\n")
