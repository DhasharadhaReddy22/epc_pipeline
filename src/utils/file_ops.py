import os
import shutil
import zipfile
import logging
from pathlib import Path
from typing import Optional, List

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s | %(name)s | %(asctime)s] - [%(filename)s | %(funcName)s | L%(lineno)d] : %(message)s"
)
logger = logging.getLogger(__name__)

def extract_zip(zip_path: str | Path, dest_dir: str | Path = "/home/dhasharadhareddyb/projects/epc_pipeline/api_datasets/bulk_download/unzipped") -> bool:
    """
    Extracts a zip file to a folder with the same name (without .zip).
    Skips extraction if the folder already exists and is not empty.
    
    Args:
        zip_path (str): Path to the zip file.
        dest_dir (str): Directory where the extracted folder will be created.
    Returns:
        bool: True if extraction successful or skipped (already extracted), False otherwise.
    """
    try:
        zip_path = Path(zip_path)
        dest_dir = Path(dest_dir)
        folder_name = zip_path.stem # gives filename without suffix
        extract_folder = dest_dir / folder_name

        # Skip if folder already exists and is not empty
        if extract_folder.exists() and any(extract_folder.iterdir()):
            logger.info(f"Skipping extraction, folder already exists: {extract_folder}")
            return True

        # Create the folder (and parents) if it doesn't exist. 
        # exist_ok=True prevents exception if folder exists (safe).
        extract_folder.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)

        logger.info(f"Extracted: {zip_path.name} to {extract_folder}")
        return True

    except zipfile.BadZipFile:
        logger.error(f"Invalid zip file: {zip_path}")
    except Exception as e:
        logger.exception(f"Failed to extract {zip_path}: {e}")
    return False

def move_file(source: str | Path, destination: str | Path) -> bool:
    """
    Moves a single file to a specified destination directory or file path.
    Automatically removes the source directory if it becomes empty.
    Does not overwrite existing files.

    Args:
        source (str): Path to the source file.
        destination (str): Destination file path or directory.

    Returns:
        bool: True if move successful, False otherwise.
    """
    try:
        src = Path(source)
        dest = Path(destination)

        if not src.exists() or not src.is_file():
            logger.error(f"Source file not found or not a regular file: {src}")
            return False

        # Determine if destination should be treated as a directory
        if dest.exists() and dest.is_dir():
            final_dest = dest / src.name
        elif str(destination).endswith(os.sep) or dest.suffix == "": 
            # os.sep is the path separator used by your operating system "/" or "\"
            # Treat as directory path even if it doesn't exist yet
            dest.mkdir(parents=True, exist_ok=True)
            final_dest = dest / src.name
        else: # if path given allong with filename
            dest.parent.mkdir(parents=True, exist_ok=True)
            final_dest = dest

        # Prevent overwriting
        if final_dest.exists():
            logger.warning(f"Destination file already exists, skipping move: {final_dest}")
            return False

        shutil.move(str(src), str(final_dest))
        logger.info(f"Moved file: {src} → {final_dest}")

        # Remove source directory if empty
        parent_dir = src.parent
        if not any(parent_dir.iterdir()):
            parent_dir.rmdir()
            logger.info(f"Removed empty directory: {parent_dir}")

        return True

    except Exception as e:
        logger.exception(f"Failed to move file from {source} to {destination}: {e}")
        return False

def move_directory(source: str | Path, destination: str | Path) -> bool:
    """
    Moves all files from the source directory to the destination directory.
    Does not overwrite existing files.
    
    Args:
        source (str): Source directory path.
        destination (str): Destination directory path.

    Returns:
        bool: True if any files moved successfully, False otherwise.
    """
    try:
        source = Path(source)
        destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)

        if not source.exists():
            logger.error(f"Source directory not found: {source}")
            return False

        if not source.is_dir():
            logger.error(f"Source path is not a directory, use move_file() instead")
            return False

        moved_any = False
        for item in source.iterdir():
            if not item.is_file():
                continue  # Skip subdirectories
            target = destination / item.name
            if target.exists():
                logger.info(f"Skipping move, file already exists: {target}")
                continue

            shutil.move(str(item), str(target))
            logger.info(f"Moved: {item.name} → {destination}")
            moved_any = True

        return moved_any

    except Exception as e:
        logger.exception(f"Failed to move files from {source} to {destination}: {e}")
        return False

def delete_file(location: str | Path) -> bool:
    """
    Deletes a single file safely.
    Will not delete directories to prevent accidental data loss.

    Args:
        location (str): Path to the file to delete.

    Returns:
        bool: True if deletion successful or file already absent, False otherwise.
    """
    try:
        path = Path(location)
        if not path.exists():
            logger.warning(f"File not found, skipping delete: {path}")
            return True  # Nothing to delete; considered successful.

        if path.is_dir():
            logger.warning(f"Path is a directory, use delete_directory() instead")
            return False  # Explicitly reject directory deletions.

        path.unlink()
        logger.info(f"Deleted file: {path}")
        
        # Check if directory is now empty
        if not any(path.parent.iterdir()):
            path.parent.rmdir()
            logger.info(f"Removed empty directory: {path.parent}")

        return True

    except PermissionError as e:
        logger.error(f"Permission denied when deleting file {location}: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error deleting file {location}: {e}")
    return False

def delete_directory(location: str | Path) -> bool:
    """
    Deletes an entire directory and all its contents safely.
    Should be called explicitly when directory deletion is intended.

    Args:
        location (str): Path to the directory to delete.

    Returns:
        bool: True if deletion successful or directory already absent, False otherwise.
    """
    try:
        path = Path(location)

        if not path.exists():
            logger.warning(f"Directory not found, skipping delete: {path}")
            return True  # Nothing to delete; considered successful.

        if not path.is_dir():
            logger.warning(f"Path is a file, use delete_file() instead")
            return False  # Prevent misuse; avoid deleting files accidentally.

        shutil.rmtree(path)
        logger.info(f"Deleted directory and all contents: {path}")
        return True

    except PermissionError as e:
        logger.error(f"Permission denied when deleting directory {location}: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error deleting directory {location}: {e}")
    return False

if __name__=="__main__":
    import time

    result = extract_zip("/home/projects/epc_pipeline/api_datasets/bulk_download/zips/display-E07000216-Waverley.zip", "/home/projects/epc_pipeline/api_datasets/bulk_download/")
    print("Extracted: ", result)
    time.sleep(3)

    result = move_directory("/home/projects/epc_pipeline/api_datasets/bulk_download/display-E07000216-Waverley", "/home/projects/epc_pipeline/api_datasets/bulk_download/unzipped/display-E07000216-Waverley")
    print("Moved: ", result)
    time.sleep(3)

    result = move_file("/home/projects/epc_pipeline/api_datasets/bulk_download/move_dummy/display-E07000103-Watford.zip", "/home/projects/epc_pipeline/api_datasets/bulk_download/dummy/display-E07000103-Watford.zip")
    print("Moved file: ", result)
    time.sleep(5)

    result = delete_file("/home/projects/epc_pipeline/api_datasets/bulk_download/dummy/display-E07000103-Watford.zip")
    print("Deleted file: ", result)
    time.sleep(5)

    result = delete_directory("/home/projects/epc_pipeline/api_datasets/bulk_download/unzipped/display-E07000216-Waverley")
    print("Deleted folder: ", result)
    time.sleep(3)