from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

import logging

logging.basicConfig(filename="./logs/logs.log", format="%(name)s - %(levelname)s - %(asctime)s - %(message)s", level=logging.INFO)


def schema() -> StructType:
    """
    Defines the PySpark schema for the JSON data.

    This function specifies the nested schema for different sections of the JSON data:
    - 'location_schema' for airport locations.
    - 'airline_schema' for airline information.
    - 'flight_schema' for flight details, including codeshared flights.
    - The main schema for the entire JSON data, including flight date, status, departure,
      arrival, airline, and flight information.

    Returns:
    pyspark.sql.types.StructType: The PySpark schema for the JSON data.
    """
    location_schema = StructType([
        StructField("airport", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("terminal", StringType(), True),
        StructField("gate", StringType(), True),
        StructField("delay", IntegerType(), True),
        StructField("scheduled", TimestampType(), True),
        StructField("estimated", TimestampType(), True),
        StructField("actual", TimestampType(), True),
        StructField("estimated_runway", TimestampType(), True),
        StructField("actual_runway", TimestampType(), True),
    ])

    airline_schema = StructType([
        StructField("name", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
    ])

    flight_schema = StructType([
        StructField("number", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("codeshared", StructType([
            StructField("airline_name", StringType(), True),
            StructField("airline_iata", StringType(), True),
            StructField("airline_icao", StringType(), True),
            StructField("flight_number", StringType(), True),
            StructField("flight_iata", StringType(), True),
            StructField("flight_icao", StringType(), True),
        ]), True),
    ])
    
    return StructType([
                StructField("flight_date", TimestampType(), True),
                StructField("flight_status", StringType(), True),
                StructField("departure", location_schema, True),
                StructField("arrival", location_schema, True),
                StructField("airline", airline_schema, True),
                StructField("flight", flight_schema, True),
            ])



def arrange_schema(df:DataFrame):
    """
    Arranges the schema of the given PySpark DataFrame.

    This function reorganizes the DataFrame columns to flatten nested structures,
    making it more convenient for analysis or further processing.

    Parameters:
    - df (pyspark.sql.DataFrame): The PySpark DataFrame with a nested schema.

    Returns:
    pyspark.sql.DataFrame: The PySpark DataFrame with the arranged schema.
    """
    try:
          logging.info("Starting arranging dataframe schema...")
          return df.withColumn("dep_airport",col("arrival.airport"))\
                    .withColumn("dep_timezone",col("departure.timezone"))\
                    .withColumn("dep_iata",col("departure.iata"))\
                    .withColumn("dep_icao",col("departure.icao"))\
                    .withColumn("dep_terminal",col("departure.terminal"))\
                    .withColumn("dep_gate",col("departure.gate"))\
                    .withColumn("dep_delay",col("departure.delay"))\
                    .withColumn("dep_scheduled",col("departure.scheduled"))\
                    .withColumn("dep_estimated",col("departure.estimated"))\
                    .withColumn("dep_actual",col("departure.actual"))\
                    .withColumn("dep_estimated_runway",col("departure.estimated_runway"))\
                    .withColumn("dep_actual_runway",col("departure.actual_runway"))\
                    .withColumn("arr_timezone",col("arrival.timezone"))\
                    .withColumn("arr_iata",col("arrival.iata"))\
                    .withColumn("arr_icao",col("arrival.icao"))\
                    .withColumn("arr_terminal",col("arrival.terminal"))\
                    .withColumn("arr_gate",col("arrival.gate"))\
                    .withColumn("arr_delay",col("arrival.delay"))\
                    .withColumn("arr_scheduled",col("arrival.scheduled"))\
                    .withColumn("arr_estimated",col("arrival.estimated"))\
                    .withColumn("arr_actual",col("arrival.actual"))\
                    .withColumn("arr_estimated_runway",col("arrival.estimated_runway"))\
                    .withColumn("arr_actual_runway",col("arrival.actual_runway"))\
                    .withColumn("airline_name",col("airline.name"))\
                    .withColumn("airline_iata",col("airline.iata"))\
                    .withColumn("airline_icao",col("airline.icao"))\
                    .withColumn("flight_number",col("flight.number"))\
                    .withColumn("flight_iata",col("flight.iata"))\
                    .withColumn("flight_icao",col("flight.icao"))\
                    .withColumn("codeshared_airline_name",col("flight.codeshared.airline_name"))\
                    .withColumn("codeshared_airline_iata",col("flight.codeshared.airline_iata"))\
                    .withColumn("codeshared_airline_icao",col("flight.codeshared.airline_icao"))\
                    .withColumn("codeshared_flight_number",col("flight.codeshared.flight_number"))\
                    .withColumn("codeshared_flight_iata",col("flight.codeshared.flight_iata"))\
                    .withColumn("codeshared_flight_icao",col("flight.codeshared.flight_icao"))\
                    .drop("departure", "arrival", "arrival", "flight", "airline")
    except Exception as e:
          logging.error("Problem arranging data...", e)
    

def handle_nulls(df:DataFrame) -> DataFrame:
    """
    Handles null values in the specified PySpark DataFrame.

    This function fills null values in the DataFrame with predefined values for different columns:
    - String columns are filled with "Uknown".
    - Numeric columns 'dep_delay' and 'arr_delay' are filled with 0.
    - 'dep_terminal' column is filled with "AZERTY".

    Timestamp columns ('dep_estimated_runway', 'dep_actual', 'dep_actual_runway',
    'arr_actual_runway', 'arr_estimated_runway', 'arr_actual') are filled with the timestamp
    corresponding to "1945-12-27 00:50:39" for null values

    Parameters:
    - df (pyspark.sql.DataFrame): The PySpark DataFrame with null values to be handled.

    Returns:
    pyspark.sql.DataFrame: The PySpark DataFrame with null values handled.
    """
    try:
        logging.info("Starting handling nulls...")
        df = df.fillna("Uknown")\
            .fillna(0, subset=["dep_delay", "arr_delay"])\
            .fillna("AZERTY", subset="dep_terminal")
        
        col_to_fill=["dep_estimated_runway", "dep_actual", "dep_actual_runway", "arr_actual_runway", "arr_estimated_runway", "arr_actual"]

        for colonne in col_to_fill:
            df = df.withColumn(
                colonne,
                when(
                    col(colonne).isNull(),
                    datetime(1945, 1, 1, 0, 0, 0) # 1945-12-27 00:50:39 = Null value
                ).otherwise(col(colonne).cast(TimestampType()))
            )
    except Exception as e:
         logging.error("Problem handling nulls...", e)

    return df


def show_nulls(df:DataFrame) -> DataFrame:
    """
    Displays the count of null values for each column in the DataFrame.

    Parameters:
    - df (pyspark.sql.DataFrame): The PySpark DataFrame to analyze.
    """
    return df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()