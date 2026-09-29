import dlt
from pyspark.sql.functions import col

# ==============================================================================
# 1. INGEST THE SUBSET OF POLLUTANT CSV FILES
# ==============================================================================
@dlt.table(
    name="aqi_data.bronze.historical_pollutants",
    comment="Raw historical pollutant data from Kaggle CSV subset",
    table_properties={
        "quality": "bronze",
        "delta.columnMapping.mode": "name" # Enables spaces and special characters in column names
    }
)
def bronze_historical_pollutants():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://aqi-data@lshc.dfs.core.windows.net/Time Series Air Quality Data of India (2010-2023)/*.csv")
        
        .withColumn("file_name", col("_metadata.file_name"))
    )

# ==============================================================================
# 2. INGEST THE REGISTRATION INFO (STATIONS MAPPING) CSV
# ==============================================================================
@dlt.table(
    name="aqi_data.bronze.bronze_historical_stations_info",
    comment="Raw registration mapping file for historical data",
    table_properties={
        "quality": "bronze",
        "delta.columnMapping.mode": "name" # Applied here as well for safety
    }
)
def bronze_historical_stations_info():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://aqi-data@lshc.dfs.core.windows.net/Time Series Air Quality Data of India (2010-2023)/station_info/")
    )