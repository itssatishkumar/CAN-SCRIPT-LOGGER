import os
import sys
import requests
from PySide6.QtWidgets import QApplication, QMessageBox, QProgressDialog
from PySide6.QtCore import Qt
import subprocess


def get_text_file_content(url):
    """Fetches and returns text content from a URL."""
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text.strip()
    except Exception as e:
        print(f"Failed to fetch from {url}: {e}")
        return None


def download_file(url, target_path, parent=None):
    """Download a file from a URL showing a progress dialog."""
    try:
        r = requests.get(url, stream=True, timeout=30)
        r.raise_for_status()
        total = int(r.headers.get('content-length', 0))

        progress = QProgressDialog(f"Downloading {os.path.basename(target_path)}...", "Cancel", 0, total if total > 0 else 0, parent)
        progress.setWindowModality(Qt.ApplicationModal)
        progress.setWindowTitle("Updater")
        progress.setMinimumDuration(300)
        progress.show()

        downloaded = 0
        chunk_size = 8192
        with open(target_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        progress.setValue(downloaded)
                    else:
                        # Indeterminate progress (no total length)
                        progress.setValue(0)
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
    """Detect if running as a frozen executable (.exe)."""
    _, ext = os.path.splitext(sys.argv[0])
    return ext.lower() == ".exe"


def check_for_update(local_version,
                     app,
                     version_url="https://raw.githubusercontent.com/itssatishkumar/PCANView-Logger-DebugTool-/main/version.txt",
                     download_url_txt="https://raw.githubusercontent.com/itssatishkumar/PCANView-Logger-DebugTool-/refs/heads/main/appversion.txt",
                     updater_exe_name="updater.exe"):
    """
    Check for update, download and install.
    For exe: downloads new exe and runs updater helper.
    For script: downloads updated .py files.
    """

    # Step 1: Fetch online version string
    online_version = get_text_file_content(version_url)
    if online_version is None:
        print("Update check skipped: unable to fetch version info.")
        return

    # If versions match, no update needed
    if online_version == local_version:
        print("No update available.")
        return

    parent = app.activeWindow() if app else None

    # Ask user to confirm update
    reply = QMessageBox.question(parent, "Update Available",
                                 f"A new version {online_version} is available.\n"
                                 f"Do you want to download and install the update?",
                                 QMessageBox.Yes | QMessageBox.No)
    if reply != QMessageBox.Yes:
        print("User declined update.")
        return

    target_folder = os.path.dirname(os.path.abspath(sys.argv[0]))

    if is_running_as_exe():
        # --- EXE update flow ---

        # Get new EXE download URL from remote text file
        new_exe_url = get_text_file_content(download_url_txt)
        if not new_exe_url:
            QMessageBox.warning(parent, "Update Failed", "Could not retrieve EXE download URL.")
            return

        new_exe_path = os.path.join(target_folder, "MyApp-new.exe")
        updater_exe_path = os.path.join(target_folder, updater_exe_name)

        # Download the new EXE
        success = download_file(new_exe_url, new_exe_path, parent=parent)
        if not success:
            QMessageBox.warning(parent, "Update Failed", "Failed to download new EXE file.")
            return

        # Launch updater helper to replace old exe with new one and restart app
        try:
            # shell=True helps subprocess find .exe on Windows
            subprocess.Popen([updater_exe_path, sys.argv[0], new_exe_path], shell=True)
        except Exception as e:
            QMessageBox.warning(parent, "Update Failed", f"Failed to launch updater helper:\n{e}")
            return

        # Exit current app for updater to replace EXE
        sys.exit(0)

    else:
        # --- Python script update flow ---

        # List of files to update from remote URLs
        files_to_update = [
            ("https://raw.githubusercontent.com/itssatishkumar/PCANView-Logger-DebugTool-/main/pcan_logger.py", "pcan_logger.py"),
            ("https://raw.githubusercontent.com/itssatishkumar/PCANView-Logger-DebugTool-/main/parse_tool.py", "parse_tool.py"),
            # Add other files here if needed
        ]

        for file_url, local_name in files_to_update:
            local_path = os.path.join(target_folder, local_name)
            success = download_file(file_url, local_path, parent=parent)
            if not success:
                QMessageBox.warning(parent, "Update Failed", f"Failed to download {local_name}")
                return

        # Update local version file
        version_file_path = os.path.join(target_folder, "version.txt")
        try:
            with open(version_file_path, "w") as vf:
                vf.write(online_version)
        except Exception as e:
            print(f"Failed to update local version file: {e}")

        QMessageBox.information(parent, "Update Complete",
                                "Update installed successfully.\nPlease restart the application.")
        sys.exit(0)
