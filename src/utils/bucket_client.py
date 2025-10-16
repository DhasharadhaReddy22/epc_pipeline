import os
import logging
from pathlib import Path
from typing import List, Any
from dotenv import load_dotenv

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s | %(asctime)s] - [%(filename)s | %(funcName)s | L%(lineno)d] : %(message)s"
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")


class S3BucketClient:
    """
        Basic S3 Client for reading, writing, and appending JSONL files.
        Args:
            bucket_name (str): Name of the S3 bucket.
            aws_access_key_id (str): AWS access key ID.
            aws_secret_access_key (str): AWS secret access key.
            region_name (str): AWS region name.
    """
    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str,
    ):
        self.bucket_name = bucket_name
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def list_objects(self, prefix: str = "") -> List[str]:
        """
            List all objects in the bucket under a prefix. 
            Default prefix is root, and will list all objects in bucket.
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
            # logger.info(f"Found {len(keys)} objects under '{prefix}'")
            return keys
        
        except ClientError as e:
            logger.error(f"Failed to list objects: {e}")
            raise

    def upload_file(self, local_path: str, s3_path: str, overwrite: bool = False) -> bool:
        """Upload a local file to an S3 bucket."""

        try:
            if overwrite and (s3_path in self.list_objects(s3_path)):
                self.client.upload_file(local_path, self.bucket_name, s3_path)
                logger.warning(f"File already exists at {s3_path}, overwritting.")
                return True
            
            if (s3_path in self.list_objects(s3_path)) and (not overwrite):
                logger.warning(f"File already exists at {s3_path}. Skipping upload to avoid overwrite.")
                return False
                
            self.client.upload_file(local_path, self.bucket_name, s3_path)
            logger.info(f"Uploaded {local_path} → s3://{self.bucket_name}/{s3_path}")
            return True
        
        except ClientError as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            return False

    def download_file(self, s3_path: str, local_path: str) -> bool:
        """Download an S3 object to a local path."""

        try:
            src = Path(s3_path)
            dest = Path(local_path)
            # Determine if destination should be treated as a directory
            if dest.exists() and dest.is_dir():
                final_dest = dest / src.name
            elif str(local_path).endswith(os.sep) or dest.suffix == "": 
                # os.sep is the path separator used by your operating system "/" or "\"
                # Treat as directory path even if it doesn't exist yet
                dest.mkdir(parents=True, exist_ok=True)
                final_dest = dest / src.name
            else: # if path given allong with filename
                dest.parent.mkdir(parents=True, exist_ok=True)
                final_dest = dest
            final_dest.parent.mkdir(parents=True, exist_ok=True)
            self.client.download_file(self.bucket_name, s3_path, final_dest)
            logger.info(f"Downloaded s3://{self.bucket_name}/{s3_path} → {final_dest}")
            return True
        
        except ClientError as e:
            logger.error(f"Failed to download {s3_path}: {e}")
            return False

    def read_s3(self, s3_path: str) -> Any:
        """
        Read a CSV or JSON file from S3.
        Automatically detects file type from extension.
        """
        try:
            if s3_path.endswith(".csv"):
                obj = self.client.get_object(Bucket=self.bucket_name, Key=s3_path)
                df = pd.read_csv(obj["Body"])
                logger.info(f"Read CSV from {s3_path}")
                return df

            elif s3_path.endswith(".json"):
                obj = self.client.get_object(Bucket=self.bucket_name, Key=s3_path)
                data = obj["Body"].read().decode("utf-8")
                logger.info(f"Read JSON from {s3_path}")
                return data

            else:
                raise ValueError("Unsupported file type — only .csv and .json are supported")

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"Object {s3_path} not found")
                return None
            logger.error(f"Failed to read {s3_path}: {e}")
            raise

    def move_object(self, source_path: str, dest_path: str) -> bool:
        """Move (copy + delete) an object within the same bucket."""

        try:
            copy_source = {"Bucket": self.bucket_name, "Key": source_path}
            self.client.copy_object(
                CopySource=copy_source, Bucket=self.bucket_name, Key=dest_path
            )
            self.client.delete_object(Bucket=self.bucket_name, Key=source_path)
            logger.info(
                f"Moved {source_path} → {dest_path}"
            )
            return True
        
        except ClientError as e:
            logger.error(f"Failed to move object: {e}")
            return False

    def delete_object(self, s3_path: str) -> bool:
        """Delete a single object from S3."""

        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=s3_path)
            logger.info(f"Deleted {s3_path}")
            return True
        
        except ClientError as e:
            logger.error(f"Failed to delete {s3_path}: {e}")
            return False

    def delete_prefix(self, prefix: str) -> bool:
        """Delete all objects under a prefix (i.e., folder)."""

        try:
            keys = self.list_objects(prefix)
            if not keys:
                logger.info(f"No objects to delete under prefix '{prefix}'")
                return True
            delete_payload = {"Objects": [{"Key": key} for key in keys]}
            self.client.delete_objects(Bucket=self.bucket_name, Delete=delete_payload)
            logger.info(f"Deleted {len(keys)} objects under prefix '{prefix}'")
            return True
        
        except ClientError as e:
            logger.error(f"Failed to delete prefix {prefix}: {e}")
            return False

if __name__ == "__main__":
    
    epc_s3_client = S3BucketClient(
        bucket_name=os.getenv("AWS_BUCKET_NAME"),
        aws_access_key_id=os.getenv("AWS_S3_KEY"),
        aws_secret_access_key=os.getenv("AWS_S3_PASSWORD"),
        region_name=os.getenv("AWS_REGION_NAME")
    )

    local_csv = "/home/projects/epc_pipeline/api_datasets/snowflake_project.csv"
    s3_csv = "api_datasets/snowflake_project.csv"
    epc_s3_client.upload_file(local_csv, s3_csv)

    s3_csv = "api_datasets/snowflake_project_v2.csv"
    epc_s3_client.upload_file(local_csv, s3_csv)
    epc_s3_client.upload_file(local_csv, s3_csv)
    epc_s3_client.upload_file(local_csv, s3_csv, overwrite=True)

    print("Bucket contents:", epc_s3_client.list_objects("api_datasets/"))

    df = epc_s3_client.read_s3(s3_csv)
    print(df.head() if isinstance(df, pd.DataFrame) else df)

    epc_s3_client.move_object("api_datasets/snowflake_project_v2.csv", "api_datasets_v2/snowflake_project_v2.csv")

    print("Bucket contents:", epc_s3_client.list_objects(""))

    epc_s3_client.download_file("api_datasets_v2/snowflake_project_v2.csv", "/home/projects/epc_pipeline/api_datasets")

    epc_s3_client.delete_object("api_datasets_v2/snowflake_project_v2.csv")

    epc_s3_client.delete_prefix("api_datasets/")
