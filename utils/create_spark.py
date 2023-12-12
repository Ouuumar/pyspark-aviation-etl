import logging
import logging.config
from datetime import datetime

from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(filename="./logs/logs.log", format="%(name)s - %(levelname)s - %(asctime)s - %(message)s", level=logging.INFO)


def get_spark_object(env:str="dev", appName:str="flight-tracker-app") -> SparkSession:
    """
    Creates a PySpark SparkSession object.

    Parameters:
    - env (str): The environment, either "dev" for local development or another value for production.
    - appName (str): The name to be assigned to the Spark application.

    Returns:
    pyspark.sql.SparkSession: The PySpark SparkSession object.
    """
    try:
        logging.info("Creating spark object")

        if env == "dev":
            master = "local[*]"
        else:
            master = "Yarn"
            
        logging.info(f"Master is {master}")
        return SparkSession.builder.master(master)\
                .appName(appName)\
                        .config("spark.sql.warehouse.dir", "./data/spark-warehouse").getOrCreate()

    except Exception as e:
        logging.error("An error occured ", e)


def read_data(spark:SparkSession, schema:StructType, date:str=None) -> DataFrame:
    """
    Reads JSON data into a PySpark DataFrame.

    Parameters:
    - spark (pyspark.sql.SparkSession): The PySpark SparkSession.
    - schema (pyspark.sql.types.StructType): The schema to be used for reading the JSON data.
    - date (str): The date of a particular json file to read (optionnal)

    Returns:
    pyspark.sql.DataFrame: The PySpark DataFrame containing the read JSON data.
    """
    try:
        logging.info("Reading data into Spark...")
        file_path = f"./data/bronze/{date}_data.json" if date else f"./data/bronze/{datetime.today().strftime('%Y-%m-%d')}_data.json"
        df = spark.read.option("multiline", "true").json(file_path, schema=schema)
    except Exception as e:
        logging.error("Problem reading data into Spark...", e)
        return None
    return df