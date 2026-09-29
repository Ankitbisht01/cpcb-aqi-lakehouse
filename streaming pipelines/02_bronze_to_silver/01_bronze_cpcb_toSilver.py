# Import the Delta Live Tables framework for pipeline orchestration
import dlt

# Import PySpark functions for column extraction, data type casting, and array explosion
from pyspark.sql.functions import col, explode, coalesce, to_timestamp, current_timestamp

# Hardcode the source table path to read directly from the completed Bronze layer
SOURCE_TABLE = "aqi_data.bronze.bronze_cpcb_telemetry"

# Register this function as a DLT target table in the pipeline
@dlt.table(
    name="aqi_data.silver.silver_cpcb_telemetry",
    comment="Unnested and flattened CPCB station metrics with enforced data types to silver layer",
    table_properties={"quality": "silver"}
)

# Apply a data quality rule: drop any row completely if the station ID is missing
@dlt.expect_or_drop("valid_station", "station IS NOT NULL")

# Apply a data quality rule: drop any row completely if there is no pollutant data
@dlt.expect_or_drop("valid_pollutant", "pollutant_id IS NOT NULL")

# Apply a monitoring rule: flag rows with missing coordinates, but do not drop them
@dlt.expect("valid_coordinates", "latitude IS NOT NULL AND longitude IS NOT NULL")

def silver_cpcb_telemetry():
    
    return (
        # Initialize a streaming read from the existing Bronze table. 
        # DLT automatically manages the checkpoint state under the hood.
        spark.readStream.table(SOURCE_TABLE)
        
        # The raw JSON stores multiple stations in a single array called 'records'.
        # explode() rips this array open, turning one row with 50 stations into 50 distinct rows.
        .withColumn("record", explode(col("records")))
        
        # Select specific fields out of the new 'record' structure and define their final schema
        .select(
            col("record.country").alias("country"),
            col("record.state").alias("state"),
            col("record.city").alias("city"),
            col("record.station").alias("station"),
            
            # Cast geographic coordinates from raw strings into double-precision floats
            col("record.latitude").cast("double").alias("latitude"),
            col("record.longitude").cast("double").alias("longitude"),
            
            # coalesce() tries the first date format. If it fails (returns null), it tries the second.
            # This standardizes the observation time into a native Timestamp format.
            coalesce(
                to_timestamp(col("record.last_update"), "dd-MM-yyyy HH:mm:ss"),
                to_timestamp(col("record.last_update"), "yyyy-MM-dd HH:mm:ss")
            ).alias("observation_time"),
            
            col("record.pollutant_id").alias("pollutant_id"),
            
            # Cast all pollutant metrics from strings to numerical doubles so you can run math on them later
            col("record.min_value").cast("double").alias("pollutant_min"),
            col("record.max_value").cast("double").alias("pollutant_max"),
            col("record.avg_value").cast("double").alias("pollutant_avg"),
            
            # Keep the original file path from the Bronze layer for data auditing
            col("source_file"),
            
            # Keep the timestamp showing when the raw data landed in Bronze
            col("bronze_ingested_at"),
            
            # Generate a brand new timestamp to record exactly when this row was processed into Silver
            current_timestamp().alias("silver_processed_at")
        )
    )