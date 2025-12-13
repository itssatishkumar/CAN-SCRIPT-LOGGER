import os
import sys
import requests
import subprocess
from PySide6.QtWidgets import QApplication, QMessageBox, QProgressDialog
from PySide6.QtCore import Qt


# ----------------------------
# Helper functions
# ----------------------------

def get_text_file_content(url):
    """Fetch and return plain text content from a URL."""
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text.strip()
    except Exception as e:
        print(f"Failed to fetch from {url}: {e}")
        return None


def download_file(url, target_path, parent=None):
    """Download a file from a URL with a progress dialog."""
    try:
        r = requests.get(url, stream=True, timeout=30)
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))

        progress = QProgressDialog(
            f"Downloading {os.path.basename(target_path)}...",
            "Cancel",
            0,
            total if total > 0 else 0,
            parent
        )
        progress.setWindowModality(Qt.ApplicationModal)
        progress.setWindowTitle("Updater")
        progress.setMinimumDuration(300)
        progress.show()

        downloaded = 0
        chunk_size = 8192

        # Ensure parent directory exists
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        with open(target_path, "wb") as f:
            for chunk in r.iter_content(chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        progress.setValue(downloaded)
                    QApplication.processEvents()
                    if progress.wasCanceled():
                        progress.close()
                        return False

        progress.close()
        return True
    except Exception as e:
        print(f"Download failed for {url}: {e}")
        return False


def is_running_as_exe():
    """Return True if the app is running as a frozen EXE."""
    _, ext = os.path.splitext(sys.argv[0])
    return ext.lower() == ".exe"


def list_repo_files_recursive(repo_user, repo_name, path=""):
    """
    Return a flat list of file metadata for all files in the repo, recursively,
    using the GitHub Contents API.
    """
    api_url = f"https://api.github.com/repos/{repo_user}/{repo_name}/contents/{path}"
    r = requests.get(api_url, timeout=15)
    r.raise_for_status()
    items = r.json()

    all_files = []
    if isinstance(items, dict) and items.get("type") == "file":
        # Single file response
        return [items]

    for item in items:
        if item.get("type") == "file":
            all_files.append(item)
        elif item.get("type") == "dir":
            subpath = item.get("path", "")
            all_files.extend(list_repo_files_recursive(repo_user, repo_name, subpath))

    return all_files


# ----------------------------
# Core updater logic
# ----------------------------

def check_for_update(
    local_version,
    app,
    repo_user="itssatishkumar",
    repo_name="CAN-SCRIPT-LOGGER",
    updater_exe_name="updater.exe"
):
    """
    Check GitHub for new version and update if needed.

    FIX:
    - Recursively downloads all .py, .txt, and .dbc files from the repo
      (including new folders like MCU_DBC/) when version changes.
    - Preserves folder structure locally using file_info["path"].
    """

    parent = app.activeWindow() if app else None

    # --- Step 1: Check online version ---
    version_url = f"https://raw.githubusercontent.com/{repo_user}/{repo_name}/main/version.txt"
    online_version = get_text_file_content(version_url)
    if online_version is None:
        print("Could not retrieve online version — skipping update.")
        return

    if online_version == local_version:
        print("No update available.")
        return  # ✅ Up to date, nothing to do

    # --- Step 2: Ask for confirmation ---
    reply = QMessageBox.question(
        parent,
        "Update Available",
        f"A new version ({online_version}) is available.\n"
        f"Do you want to download and install the update?",
        QMessageBox.Yes | QMessageBox.No
    )
    if reply != QMessageBox.Yes:
        print("User declined update.")
        return

    # --- Step 3: Determine update target folder ---
    target_folder = os.path.dirname(os.path.abspath(sys.argv[0]))

    # --- Step 4: EXE mode ---
    if is_running_as_exe():
        appversion_url = f"https://raw.githubusercontent.com/{repo_user}/{repo_name}/main/appversion.txt"
        new_exe_url = get_text_file_content(appversion_url)
        if not new_exe_url:
            QMessageBox.warning(parent, "Update Failed", "Could not retrieve EXE download URL.")
            return

        new_exe_path = os.path.join(target_folder, "CAN_Logger_New.exe")
        updater_exe_path = os.path.join(target_folder, updater_exe_name)

        success = download_file(new_exe_url, new_exe_path, parent=parent)
        if not success:
            QMessageBox.warning(parent, "Update Failed", "Failed to download new EXE file.")
            return

        try:
            subprocess.Popen([updater_exe_path, sys.argv[0], new_exe_path], shell=True)
        except Exception as e:
            QMessageBox.warning(parent, "Update Failed", f"Failed to launch updater helper:\n{e}")
            return

        sys.exit(0)

    # --- Step 5: Script mode (RECURSIVE FIX) ---
    try:
        all_files = list_repo_files_recursive(repo_user, repo_name, path="")
    except Exception as e:
        QMessageBox.warning(parent, "Update Failed", f"Failed to fetch file list:\n{e}")
        return

    # Include .py, .txt, and .dbc files from anywhere in the repo
    valid_files = [
        f for f in all_files
        if f.get("type") == "file" and f.get("name", "").endswith((".py", ".txt", ".dbc"))
    ]

    if not valid_files:
        QMessageBox.warning(parent, "Update Failed", "No .py, .txt, or .dbc files found in repository.")
        return

    total_files = len(valid_files)
    overall_progress = QProgressDialog("Updating files...", "Cancel", 0, total_files, parent)
    overall_progress.setWindowTitle("Updater")
    overall_progress.setWindowModality(Qt.ApplicationModal)
    overall_progress.show()

    for i, file_info in enumerate(valid_files, 1):
        rel_path = file_info["path"]            # IMPORTANT: keeps folders (e.g., MCU_DBC/x.dbc)
        file_url = file_info["download_url"]
        local_path = os.path.join(target_folder, rel_path)

        overall_progress.setLabelText(f"Updating {rel_path} ({i}/{total_files})")
        overall_progress.setValue(i - 1)
        QApplication.processEvents()

        if overall_progress.wasCanceled():
            break

        success = download_file(file_url, local_path, parent=parent)
        if not success:
            QMessageBox.warning(parent, "Update Failed", f"Failed to update {rel_path}")
            return

    overall_progress.close()

    # --- Step 6: Update version.txt locally ---
    try:
        version_file_path = os.path.join(target_folder, "version.txt")
        with open(version_file_path, "w", encoding="utf-8") as vf:
            vf.write(online_version)
    except Exception as e:
        print(f"Failed to update local version file: {e}")

    QMessageBox.information(
        parent,
        "Update Complete",
        "All updates installed successfully.\nPlease restart the application."
    )
    sys.exit(0)


# ----------------------------
# Example usage
# ----------------------------

if __name__ == "__main__":
    app = QApplication(sys.argv)
    check_for_update(local_version="1.0.1", app=app)
