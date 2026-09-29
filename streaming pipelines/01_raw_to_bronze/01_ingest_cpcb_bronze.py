# Import the Delta Live Tables (DLT) module which provides the declarative pipeline framework
import dlt

# Import specific PySpark functions used to manipulate columns and generate timestamps
from pyspark.sql.functions import col, current_timestamp

# Define the source path pointing to your Azure Data Lake Storage (ADLS) container
# The wildcards (*/*/*/*) allow Databricks Auto Loader to automatically scan through nested folder partitions
SOURCE_PATH = "abfss://aqi-data@lshc.dfs.core.windows.net/cpcb_raw/*/*/*/*.json"

# The @dlt.table decorator tells Databricks to manage this function as a pipeline dataset
# 'name' defines the exact table name that will be registered in your target catalog and schema
@dlt.table(
    name="aqi_data.bronze.bronze_cpcb_telemetry",
    
    # 'comment' adds documentation that will be visible inside the Unity Catalog Data Explorer
    comment="Raw ingested CPCB telemetry data, appended continuously from ADLS",
    
    # 'table_properties' applies custom tags, in this case labeling it as the 'bronze' data layer
    table_properties={"quality": "bronze"}
)

# Define the Python function that encapsulates the ingestion logic for this specific table
def bronze_cpcb_telemetry():
    
    return (
        # Initialize a PySpark streaming read operation to process data continuously or incrementally
        spark.readStream
        
        # Specify 'cloudFiles' to invoke Databricks Auto Loader, which tracks processed files efficiently
        .format("cloudFiles")
        
        # Configure Auto Loader to expect JSON files in the source directory
        .option("cloudFiles.format", "json")
        
        # Enable the multiline option because the raw JSON structures span across multiple lines
        .option("multiline", "true")
        
        # Instruct Auto Loader to sample the data and automatically determine the data types (e.g., string, array)
        .option("cloudFiles.inferColumnTypes", "true")
        
        # Tell Auto Loader to safely add new columns to the table if the source JSON structure changes in the future
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        
        # Point the read operation to the ADLS source path variable defined at the top of the script
        .load(SOURCE_PATH)
        
        # Begin the projection phase to select which columns will actually be saved into the Bronze table
        .select(
            
            # col("*") grabs every single field that was natively parsed from the raw JSON payload
            col("*"),
            
            # Extract the hidden file path from Databricks metadata and rename it to 'source_file' for data lineage tracking
            col("_metadata.file_path").alias("source_file"),
            
            # Generate a system timestamp at the exact moment of processing to record when the row landed in Bronze
            current_timestamp().alias("bronze_ingested_at")
        )
    )