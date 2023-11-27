import logging
import os
from dotenv import load_dotenv

from create_spark import *
from extract import *
from processing import *

logging.basicConfig(filename="./logs/logs.log", format="%(name)s - %(levelname)s - %(asctime)s - %(message)s", level=logging.INFO)

load_dotenv("./properties/.env")

PARAMS = {
    "access_key" : str(os.environ["API_KEY"])
}


if __name__ == "__main__":
    
    if not os.path.isfile(f"./data/bronze/{datetime.today().strftime('%Y-%m-%d')}_data.json"):
        logging.info("File not present, extract will begin...")
        data = extract_data(PARAMS)
        write_data(data, "bronze")

    if os.path.isfile(f"./data/bronze/{datetime.today().strftime('%Y-%m-%d')}_data.json"):
        logging.info("File is available, processing will begin...")
        spark = get_spark_object()
        schema = schema()
        df_raw = read_data(spark, schema)
        df_root = arrange_schema(df_raw)
        df_cleansed = handle_nulls(df_root)
        show_nulls(df_cleansed)
        write_data(df_cleansed, "silver")
