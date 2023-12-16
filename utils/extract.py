import requests
import json
from datetime import datetime
import logging

from pyspark.sql import DataFrame

from utils.processing import is_containing_nulls


logging.basicConfig(filename="./logs/logs.log", format="%(name)s - %(levelname)s - %(asctime)s - %(message)s", level=logging.INFO)


def extract_data(params: dict) -> list:
    """
    Extracts data from an API using the provided query parameters.

    Parameters:
    - params (dict): Query parameters for the API request.

    Returns:
    list (List[dict]): A list of json containing the extracted data.
    """
    try:
        logging.info("Starting requesting data to API...")
        raw_data = requests.get("http://api.aviationstack.com/v1/flights", params=params).json()
        
        # Vérifie si raw_data est vide
        if not raw_data or "data" not in raw_data:
            raise ValueError("No data found in the API response.")
        
        return raw_data["data"]
    except Exception as e:
        logging.error("Problem requesting data...", e)
        raise



def write_bronze(data:json) -> None:
    """
    Write data to a bronze-level file.

    Parameters:
    - data (json): The data to be written, typically raw JSON data.
    """
    try:
        with open(f"./data/bronze/{datetime.today().strftime('%Y-%m-%d')}_data.json", 'x') as f:
            json.dump(data, f)
        logging.info("Bronze data written successfully.")
    except Exception as e:
        logging.error("Problem writing bronze data...", e)
        

def write_silver(data:DataFrame, date:str) -> None:
    """
    Write data to a silver-level file.

    Parameters:
    - data (pyspark.sql.DataFrame): The PySpark DataFrame containing processed data to be written.
    - date (str): The date parameter for the output file (optional).
    """
    try:
        if date:
            data.write.mode("overwrite").parquet(f"./data/silver/{date}_data.parquet")
        else:
            data.write.mode("overwrite").parquet(f"./data/silver/{datetime.today().strftime('%Y-%m-%d')}_data.parquet")
        logging.info("Silver data written successfully.")
    except Exception as e:
        logging.error("Problem writing silver data...", e)


def write_data(data:DataFrame or json, level:str, date:str=None) -> None:
    """
    Writes data to disk.

    Parameters:
    - data (json or DataFrame): The raw JSON data from the API or a PySpark DataFrame.
    - level (str): The processing level for writing data, either "bronze" or "silver".
    - date (str): The date of a particular JSON file to write (optional).
    """
    logging.info("Starting writing data to local...")

    if level == "bronze":
        write_bronze(data)
    elif level == "silver" and not is_containing_nulls(data):
        write_silver(data, date)
    else:
        logging.warning("Data not written due to level or null values.")