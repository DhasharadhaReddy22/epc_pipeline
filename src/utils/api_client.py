import requests
import time
import os
import base64
from csv import DictReader
from io import StringIO
from typing import Any, Dict, List
from dotenv import load_dotenv
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s | %(name)s | %(asctime)s] - [%(filename)s | %(module)s | %(funcName)s | L%(lineno)d] : %(message)s"
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

class EPCAPIClient:
    """
        API Client for EPC Open Data Communities.

        Args:
            email (str): EPC API account email.
            api_key (str): EPC API key.
            headers (dict): Optional headers (e.g., Accept types).
            timeout (int): Timeout for requests.
            backoff (int): Backoff base for retries.
            max_retries (int): Maximum number of retries.
            allow_redirects (bool): Whether to follow redirects.
    """
    def __init__(self,
                 email: str,
                 api_key: str,
                 headers: dict = None, 
                 timeout: int = 10, 
                 backoff: int = 2, 
                 max_retries: int = 3,
                 allow_redirects: bool = False
        ):
        self.base_url = "https://epc.opendatacommunities.org/api/v1"
        self.timeout = timeout
        self.backoff = backoff
        self.max_retries = max_retries
        self.allow_redirects = allow_redirects

        auth_string = f"{email}:{api_key}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        self.headers = headers or {}
        self.headers["Authorization"] = f"Basic {encoded_auth}"

    def get(self, endpoint: str, params: dict = None) -> List[Dict[str, str]]:
        """
        Execute GET request and parse response automatically (JSON or CSV)

        Args:
            endpoint (str): API endpoint, e.g., '/domestic/search'
            params (dict, optional): Query params for request

        Returns:
            List[Dict[str, str]]: Parsed response data as list of dictionaries
        """

        url = str.rstrip(self.base_url) + "/" + str.lstrip(endpoint, "/")
        attempt = 0

        while attempt < self.max_retries:
            try:
                logger.info(f"GET {url} (Attempt {attempt + 1}/{self.max_retries})")
                response = requests.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=self.timeout,
                    allow_redirects=self.allow_redirects
                )

                response.raise_for_status()
                
                # Parse data according to content type
                content_type = response.headers.get("content-type", "").lower()
                if "json" in content_type:
                    data = response.json()['rows']
                elif "csv" in content_type:
                    data = self._parse_csv(response.text)
                else:
                    logger.warning(f"Unhandled content type: {content_type}")
                    data = response.text

                if response.status_code==400:
                    logger.error(f"API call successful, but invalid request made to {url} for params={params}, ERROR: {data}")
                    return None

                logger.info(f"Successfully fetched from {endpoint}")
                return data
            
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP {response.status_code} while calling {url}: {e}")
                break

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout calling {url}, retrying...")
                attempt += 1
                time.sleep(self.backoff ** attempt)
                continue
            
            except requests.exceptions.RequestException as e:
                attempt += 1
                if attempt >= self.max_retries:
                    logger.error(f"Failed after {self.max_retries} attempts: {str(e)}")
                    break
                sleep_time = self.backoff ** attempt
                logger.warning(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            
            except Exception as e:
                logger.exception(f"Unexpected error: {str(e)}")
                break

        return None
    
    def get_by_lmk_key(self, endpoint: str, lmk_key: str) -> List[Dict[str, str]]:
        """
            Execute GET request and parse response automatically (JSON or CSV)

            Args:
                endpoint (str): API endpoint, e.g., '/domestic/certificate/{lmk-key}'
                lmk_key (str): lmk-key to identify unique record/certificate

            Returns:
                List[Dict[str, str]]: Parsed response data as list of dictionaries
        """

        new_endpoint = str.rstrip(endpoint, "/") + "/" + str.strip(lmk_key)
        return self.get(endpoint=new_endpoint)

    @staticmethod
    def _parse_csv(csv_text: str) -> List[Dict[str, str]]:
        """
        Parses CSV text into a list of dictionaries.
        """
        try:
            csv_reader = DictReader(StringIO(csv_text))
            rows = [row for row in csv_reader]
            logger.info(f"Parsed {len(rows)} CSV rows.")
            return rows
        except Exception as e:
            logger.error(f"Failed to parse CSV: {e}")
            return []

    def get_file(self, filename: str, folder_path: str = "api_datasets/bulk_download/zips") -> bool:
        """
            Execute GET request download required bulk file

            Args:
                filename
            
            Returns:
                Any: the dataset zip file
        """

        if not filename.endswith(".zip"):
            filename += ".zip"

        endpoint = f"/files/{filename}"
        url = str.rstrip(self.base_url) + "/" + str.lstrip(endpoint, "/")

        folder = Path(BASE_DIR / folder_path)
        folder.mkdir(parents=True, exist_ok=True)
        output_path = folder / filename
        logger.info(f"output path set to {output_path}")

        headers["accept"] = "*/*"  # Override Accept header for file download
        attempt = 0
        
        with requests.Session() as session:
            while attempt < self.max_retries:
                try:
                    logger.info(f"Downloading file from {url} to {output_path} (Attempt {attempt + 1}/{self.max_retries})")
                    with session.get(
                        url,
                        headers=self.headers,  # Remove Accept header for file download
                        timeout=self.timeout,
                        stream=True,
                        allow_redirects=True
                    ) as response:

                        response.raise_for_status()

                        # Handle redirect trace logging
                        if response.history:
                            for r in response.history:
                                logger.info(f"Redirected: {r.status_code} → {r.headers.get('Location', 'Unknown')}")

                        # Stream download in chunks
                        with open(output_path, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)

                        logger.info(f"Successfully downloaded: {output_path}")
                        return True
                
                except requests.exceptions.Timeout:
                    attempt += 1
                    logger.warning(f"Timeout while downloading {filename}. Retrying ({attempt}/{self.max_retries})...")
                    time.sleep(self.backoff ** attempt)

                except requests.exceptions.RequestException as e:
                    logger.error(f"Request failed for {filename}: {str(e)}")
                    return False

                except Exception as e:
                    logger.exception(f"Unexpected error during download: {str(e)}")
                    return False

        logger.error(f"Failed to download {filename} after {self.max_retries} attempts.")
        return False

if __name__ == "__main__":

    email = os.getenv("API_EMAIL")
    api_key = os.getenv("API_KEY")
    
    headers = {
        'accept': 'application/json'
    }

    params = {
        'UPRN': '100023336956'
    }
    client = EPCAPIClient(email=email, api_key=api_key, headers=headers)
    # response = client.get("/domestic/search", params=params)
    # print(response)

    # response = client.get_by_lmk_key("/domestic/certificate/", "001d181000911f17dd2b1f7344c7730037c4251f9247e020353aa8e507e00e01")
    # print(response)


    response = client.get_file("display-E07000103-Watford")
    print(response)