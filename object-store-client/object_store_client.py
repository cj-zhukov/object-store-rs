import argparse
from dataclasses import dataclass
import os 
from tqdm import tqdm
import logging
import requests
import time
from tabulate import tabulate

logging.basicConfig(level=logging.INFO)


URL = os.getenv("OBJECT_STORE_URL")
CHUNK_SIZE = 8192
CHECK_INTERVAL = 5 # secs 
MAX_RETRIES = 1000  # ~80 min CHECK_INTERVAL * MAX_RETRIES = max time waiting for file

@dataclass
class ObjectStoreClient:
    query: str = "select * from object_store limit 10"
    path: str = "select" 
    method: str = "post"
    output: str = "download.zip" 


    def __post_init__(self):
        """Post-initialization checks."""
        if not self.path:
            raise ValueError("Path must be provided")
        if not self.query:
            raise ValueError("Query must be provided")
        if self.method.lower() not in {"post"}:
            raise ValueError(f"Unsupported HTTP method: {self.method}")
    

    def run(self) -> None:
        """ObejectStoreClient main interface"""
        match self.path.lower():
            case "select":
                url = f"{URL}/select"
                res = self._send_request(url)
                if res is None:
                    return
                columns = ["file_name", "file_type", "file_size", "file_path", "file_url", "dt"]
                filtered = [{k: row[k] for k in columns} for row in res]
                print(tabulate(filtered, headers="keys", tablefmt="grid"))
            case "download":
                url = f"{URL}/download"
                presigned_url = self._send_request(url)
                if presigned_url is None:
                    return
                self._wait_and_download(presigned_url)
            case _:
                raise ValueError(f"Unsupported path: {self.path}")
            

    def _send_request(self, url: str) -> dict | str | None:
        """Sends a request and returns response or None."""
        payload = {"query": self.query}
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status() 
            return response.json().get("result")
        except requests.HTTPError as http_err:
            logging.error(f"HTTP error: {http_err} | Status Code: {response.status_code}")
        except requests.RequestException as req_err:
            logging.error(f"Request error: {req_err}")
        return None


    def _wait_and_download(self, url: str, chunk_size=CHUNK_SIZE) -> None:
        """Waits for the file to become available, then downloads it."""
        retries = 0
        while retries < MAX_RETRIES:
            if self._try_download(url, chunk_size):
                return 
            time.sleep(CHECK_INTERVAL)
            retries += 1
        raise Exception("Timed out waiting for file.")


    def _try_download(self, url: str, chunk_size=CHUNK_SIZE) -> bool:
        """Tries to download the file with progress tracking using tqdm."""
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200 and response.headers.get("Content-Length") != "0":
                total_size = int(response.headers.get("Content-Length", 0)) 
                downloaded_size = 0

                with open(self.output, "wb") as file, tqdm(
                    total=total_size, unit="B", unit_scale=True, unit_divisor=1024, desc="Downloading"
                ) as progress_bar:
                    for chunk in response.iter_content(chunk_size):
                        if chunk:
                            file.write(chunk)
                            progress_bar.update(len(chunk))
                            downloaded_size += len(chunk)

                logging.info(f"File: {self.output} downloaded successfully.")
                return True
            
            else:
                logging.info("Backend is processing the request.")

        except requests.RequestException as e:
            logging.error(f"Download error: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description="Object Store Client")
    parser.add_argument("-p", "--path", type=str, required=True, help="Path")
    parser.add_argument("-q", "--query", type=str, required=True, help="SQL query for selection/download")
    parser.add_argument("-o", "--output", type=str, default="download.zip", help="Output file path for download")
    args = parser.parse_args()
    client = ObjectStoreClient(query=args.query, path=args.path, method="post", output=args.output)
    client.run()


if __name__ == "__main__":
    main()
