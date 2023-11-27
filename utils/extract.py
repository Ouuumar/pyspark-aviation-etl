import requests
import json
from datetime import datetime
import logging

from pyspark.sql import DataFrame

logging.basicConfig(filename="./logs/logs.log", format="%(name)s - %(levelname)s - %(asctime)s - %(message)s", level=logging.INFO)


def extract_data(params:dict) -> list:
    """
    Extracts data from an API using the provided query parameters.

    Parameters:
    - params (dict): Query parameters for the API request.

    Returns:
    list: A list of json containing the extracted data.
    """
    try:
        logging.info("Starting requesting data to API...")
        raw_data = requests.get("http://api.aviationstack.com/v1/flights", params=params).json()
        return raw_data["data"]
    except Exception as e:
        logging.error("Problem requesting data...", e)


def write_data(data:json or DataFrame, level:str):
    """
    Writes data to disk.

    Parameters:
    - data (json or DataFrame): The raw JSON data from the API or a PySpark DataFrame.
    - level (str): The processing level for writing data, either "bronze" or "silver".
    """
    logging.info("Starting writing data to local...")
    if level == "bronze":
        try:
            with open(f"./data/{level}/{datetime.today().strftime('%Y-%m-%d')}_data.json", 'x') as f:
                json.dump(data, f)
        except Exception as e:
            logging.error("Problem writing data...", e)
    elif level == "silver":
        data.write.mode("overwrite").parquet(f"./data/{level}/{datetime.today().strftime('%Y-%m-%d')}_data.parquet") 


    