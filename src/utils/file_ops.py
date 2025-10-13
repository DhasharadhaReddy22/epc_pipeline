import os
import zipfile
import shutil
from datetime import datetime

def extract_zip(zip_path: str, dest_dir: str):
    """Extracts a zip file to a folder with same name."""
    folder_name = os.path.splitext(os.path.basename(zip_path))[0]
    extract_path = os.path.join(dest_dir, folder_name)
    os.makedirs(extract_path, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)
    return extract_path

def move_to_s3_folders(local_folder: str, s3_client, bucket_name: str, source_type: str):
    """Uploads all CSV files to S3 under structured prefixes."""
    for file in os.listdir(local_folder):
        if file.endswith(".csv"):
            prefix = file.split(".")[0]  # certificates, recommendations, columns
            date_tag = datetime.now().strftime("%Y%m%d")
            s3_path = f"raw/{source_type}/{prefix}/{file.replace('.csv', f'_{date_tag}.csv')}"
            s3_client.upload_file(os.path.join(local_folder, file), bucket_name, s3_path)