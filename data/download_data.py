import os
import logging
from pathlib import Path
import requests
from datetime import datetime
from typing import List, Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def download_file(url: str, save_path: Path) -> None:
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Successfully downloaded: {save_path}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {url}: {str(e)}")
        raise

def download_tripdata(
    data_types: List[str],
    year: int,
    month: int,
    base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data",
    temp_dir: str = str(Path(__file__).parent / "new") 
) -> Dict[str, str]:
    result = {}
    for data_type in data_types:
        file_name = f"{data_type}_tripdata_{year}-{month:02d}.parquet"
        url = f"{base_url}/{file_name}"
        save_path = Path(temp_dir) / str(year) / file_name
        
        final_path = Path(temp_dir).parent / str(year) / file_name
        if final_path.exists():
            logger.info(f"Skipping already exists file: {str(file_name)}")
            result[data_type] = str(final_path)
            continue
    
        logger.info(f"Downloading {file_name} from {url} to {save_path}...")
        try:
            download_file(url, save_path)
            result[data_type] = str(save_path)
        except Exception as e:
            logger.error(f"Failed to download {file_name}: {str(e)}")
            raise
    
    return result

def download_tripdata_for_airflow(**context) -> Dict[str, str]:
    logical_date = context['logical_date']
    year = logical_date.year
    month = 2

    data_types = ['yellow', 'green']
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    temp_dir = str(Path(__file__).parent / "new")

    return download_tripdata(data_types, year, month, base_url, temp_dir)

if __name__ == "__main__":
    test_context = {'execution_date': datetime(2025, 1, 1)}
    result = download_tripdata_for_airflow(**test_context)
    logger.info(f"Downloaded file: {result}")


