import os
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Dict

from src.utils.api_client import EPCAPIClient
from src.utils.bucket_client import S3BucketClient
from src.utils.file_ops import extract_zip, move_file, delete_directory
from src.utils.logger import get_logger

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOG_DIR = BASE_DIR / "logs"
DATA_DIR = BASE_DIR / "api_datasets"
DATASET_DIR = DATA_DIR / "bulk_download"
ZIP_DIR = DATASET_DIR / "zips"
PROCESSED_DIR = DATASET_DIR / "processed"

logger = get_logger("src.ingestion.epc_ingestion", log_dir=LOG_DIR, log_file="epc_ingestion.log")

load_dotenv(BASE_DIR / ".env")

epc_file_client = EPCAPIClient(
    email=os.getenv("API_EMAIL"),
    api_key=os.getenv("API_KEY"),
    headers={"accept": "*/*"}
)

epc_s3 = S3BucketClient(
    bucket_name=os.getenv("AWS_BUCKET_NAME"),
    aws_access_key_id=os.getenv("AWS_S3_KEY"),
    aws_secret_access_key=os.getenv("AWS_S3_PASSWORD"),
    region_name=os.getenv("AWS_REGION_NAME")
)

def get_prev_month_string() -> str:
    """
        Returns the Year and Month of previous month in '%Y-%m' format
    """
    today = datetime.today()
    first_day_this_month = datetime(today.year, today.month-1, 1)
    prev_month_last_day = first_day_this_month - timedelta(days=1)
    return prev_month_last_day.strftime("%Y-%m")

def list_latest_files(month_tag:str = None) -> list[str]:
    """
        Lists the latest uploaded files in s3 bucket
    """
    list = epc_s3.list_objects(prefix="raw/")
    latest_month_tag = month_tag if month_tag else get_prev_month_string()
    latest_files = [f"s3://{epc_s3.bucket_name}/{file}" for file in list if latest_month_tag in file]
    logger.info(f"Latest files for {latest_month_tag}: {latest_files}")
    return latest_files

def fetch_files(month_tag:str = None) -> None:
    """
        Downloads zip files iff all file present, else aborts
    """
    month_tag = month_tag if month_tag else get_prev_month_string()

    expected_files = [
        f"display-{month_tag}.zip",
        f"domestic-{month_tag}.zip",
        f"non-domestic-{month_tag}.zip"
    ]
    logger.info(f"Expected files this month: {expected_files}")

    available_files = epc_file_client.get_file_list()
    if available_files is None:
        raise Exception("Failed to connect to EPC API")

    try:
        present_files_dict = {file: available_files[file] for file in expected_files if file in available_files}
        if not present_files_dict:
            raise FileNotFoundError(f"No expected files found for {month_tag} in EPC API.")
        logger.info(f"Available files this month: {present_files_dict}")

        present_files = list(present_files_dict.keys())
        if len(present_files) != len(expected_files):
            missing_files = set(expected_files) - set(present_files)
            error_msg = f"Missing expected files for {month_tag}: {', '.join(missing_files)}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        for file in expected_files:
            epc_file_client.get_file(file, ZIP_DIR)
            logger.info(f"Downloaded file: {file}")

    except Exception as e:
        logger.exception(f"Error fetching files for {month_tag}: {e}")
        raise

def process_files(month_tag:str = None) -> Dict[Path, Path]:
    """
        Extracts, renames, and organizes downloaded zip files
    """
    month_tag = month_tag if month_tag else get_prev_month_string()
    files_to_process = [
        f"display-{month_tag}.zip",
        f"domestic-{month_tag}.zip",
        f"non-domestic-{month_tag}.zip"
    ]

    files_processed = {}

    try:
        for zip_file in files_to_process:
            zip_file_path = ZIP_DIR / zip_file
            if not zip_file_path.exists():
                logger.error(f"File not found for processing: {zip_file_path}")
                raise FileNotFoundError(f"File not found: {zip_file_path}")
            
            # Extracting the zip file
            extract_zip(zip_file_path, PROCESSED_DIR)
            logger.info(f"Extracted file: {zip_file_path}")

            unzipped_dir = PROCESSED_DIR / zip_file_path.stem
            if not unzipped_dir.exists():
                logger.error(f"Extraction failed, folder not found: {unzipped_dir}")
                raise FileNotFoundError(f"Extraction failed, folder not found: {unzipped_dir}")
            
            if "display" in zip_file:
                dataset_type = "display"
            elif "non-domestic" in zip_file:
                dataset_type = "non_domestic"
            elif "domestic" in zip_file:
                dataset_type = "domestic"
            else:
                logger.warning(f"Unrecognized dataset type for: {zip_file}")
                continue

            cert_dest_dir = PROCESSED_DIR / "certificates" / dataset_type
            rec_dest_dir = PROCESSED_DIR / "recommendations" / dataset_type
            cert_dest_dir.mkdir(parents=True, exist_ok=True)
            rec_dest_dir.mkdir(parents=True, exist_ok=True)

            # moving the files
            cert_src = unzipped_dir / "certificates.csv"
            if cert_src.exists():
                cert_dest = cert_dest_dir / f"{dataset_type}_certificates_{month_tag}.csv"
                move_file(cert_src, cert_dest)
                logger.info(f"Moved and renamed certificates: {cert_src} → {cert_dest}")
                files_processed[str(cert_src)] = str(cert_dest)
            else:
                logger.warning(f"certificates.csv not found in {unzipped_dir}")

            rec_src = unzipped_dir / "recommendations.csv"
            if rec_src.exists():
                rec_dest = rec_dest_dir / f"{dataset_type}_recommendations_{month_tag}.csv"
                move_file(rec_src, rec_dest)
                logger.info(f"Moved and renamed recommendations: {rec_src} → {rec_dest}")
                files_processed[str(rec_src)] = str(rec_dest)
            else:
                logger.warning(f"recommendations.csv not found in {unzipped_dir}")

        return files_processed

    except Exception as e:
        logger.exception(f"Error processing files for {month_tag}: {e}")
        raise

def s3_upload(processed_files: dict) -> list:
    """
        Upload processed files to S3
    """
    uploaded_files = []
    s3_paths = []
    try:
        for _, filepath in processed_files.items():
            src = Path(filepath)
            if not src.exists():
                logger.error(f"File not found for S3 upload: {src}")
                raise FileNotFoundError(f"File not found: {src}")
            if "cert" in filepath:
                file_type = "certificates"
            elif "recom" in filepath:
                file_type = "recommendations"
            else:
                logger.warning(f"Unrecognized file type for: {filepath}")
                continue

            if "display" in filepath:
                dataset_type = "display"
            elif "non_domestic" in filepath:
                dataset_type = "non_domestic"
            elif "domestic" in filepath:
                dataset_type = "domestic"
            else:
                logger.warning(f"Unrecognized dataset type for: {filepath}")
                continue

            s3_path = f"raw/{file_type}/{dataset_type}/{src.name}"
            s3_paths.append(f"s3://epc-snowflake-project/{s3_path}")
            result = epc_s3.upload_file(src, s3_path)
            if result:
                uploaded_files.append(f"s3://epc-snowflake-project/{s3_path}")

    except Exception as e:
        logger.exception(f"Error uploading files to S3: {e}")
        raise
    
    if not uploaded_files:
        logger.error("No files were uploaded to S3.")
        uploaded_files = s3_paths  # to avoid returning empty list silently
    return uploaded_files

def cleanup():
    """
        Cleans up local directories after processing
    """
    try:
        delete_directory(ZIP_DIR)
        delete_directory(PROCESSED_DIR)
        logger.info("Cleaned up local directories")
    except Exception as e:
        logger.exception(f"Error during cleanup: {e}")

if __name__=="__main__":

    # print(get_prev_month_string())
    # fetch_files()
    # processed_files = process_files()
    # s3_upload(processed_files)
    # cleanup()
    list_latest_files()