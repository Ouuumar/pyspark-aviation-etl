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


def read_bronze(spark: SparkSession, schema: StructType, date: str = None) -> DataFrame:
    """
    Reads JSON data into a PySpark DataFrame.

    Parameters:
    - spark (pyspark.sql.SparkSession): The PySpark SparkSession.
    - schema (pyspark.sql.types.StructType): The schema to be used for reading the JSON data.
    - date (str): The date of a particular JSON file to read (optional).

    Returns:
    pyspark.sql.DataFrame: The PySpark DataFrame containing the read JSON data.
    """
    try:
        logging.info("Reading bronze data into Spark...")
        file_path = f"./data/bronze/{date}_data.json" if date else f"./data/bronze/{datetime.today().strftime('%Y-%m-%d')}_data.json"
        df = spark.read.option("multiline", "true").json(file_path, schema=schema)
        return df
    except Exception as e:
        logging.error("Problem reading data into Spark...", e)


def read_silver(spark: SparkSession, schema: StructType, date: str = None) -> DataFrame:
    """
    Reads parquet data into a PySpark DataFrame.

    Parameters:
    - spark (pyspark.sql.SparkSession): The PySpark SparkSession.
    - schema (pyspark.sql.types.StructType): The schema to be used for reading the JSON data.
    - date (str): The date of a particular JSON file to read (optional).

    Returns:
    pyspark.sql.DataFrame: The PySpark DataFrame containing the read JSON data.
    """
    try:
        logging.info("Reading silver data into Spark...")
        file_path = f"./data/silver/{date}_data.parquet" if date else f"./data/silver/{datetime.today().strftime('%Y-%m-%d')}_data.parquet"
        df = spark.read.option("multiline", "true").parquet(file_path, schema=schema)
        return df
    except Exception as e:
        logging.error("Problem reading data into Spark...", e)


def read_data(spark: SparkSession, schema: StructType, level: str, date: str = None) -> DataFrame:
    """
    Reads data into a PySpark DataFrame.

    Parameters:
    - spark (pyspark.sql.SparkSession): The PySpark SparkSession.
    - schema (pyspark.sql.types.StructType): The schema to be used for reading the data.
    - level (str): The processing level for reading data, either "bronze" or "silver".
    - date (str): The date of a particular JSON file to read (optional).

    Returns:
    pyspark.sql.DataFrame: The PySpark DataFrame containing the read data.
    """
    logging.info("Starting reading data into Spark...")

    if level == "bronze":
        return read_bronze(spark, schema, date)
    elif level == "silver":
        return read_silver(spark, schema, date)
    else:
        logging.warning("Data not read due to invalid level.")