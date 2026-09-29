import dlt
from pyspark.sql.functions import col, to_timestamp, current_timestamp, regexp_replace

POLLUTANT_TABLE = "aqi_data.bronze.historical_pollutants"
STATION_INFO_TABLE = "aqi_data.bronze.bronze_historical_stations_info"

@dlt.table(
    name="aqi_data.silver.silver_historical_telemetry",
    comment="Joined and unpivoted historical AQI data mapped to the live schema",
    table_properties={
        "quality": "silver",
        "delta.columnMapping.mode": "name"
    }
)
def silver_historical_telemetry():
    pollutants_stream = (
        spark.readStream.table(POLLUTANT_TABLE)
        .withColumn("file_name", regexp_replace(col("file_name"), r"\.csv", ""))
    ).alias("pollutants")
    
    stations_static = spark.read.table(STATION_INFO_TABLE).alias("stations")

    joined_df = pollutants_stream.join(stations_static, on="file_name", how="inner")

    prepared_df = joined_df.select(
        col("stations.state").alias("state"),
        col("stations.city").alias("city"),
        col("stations.station_location").alias("station"),
        to_timestamp(col("pollutants.`From Date`"), "yyyy-MM-dd HH:mm:ss").alias("observation_time"),
        
        col("pollutants.`PM2.5 (ug/m3)`").cast("double").alias("PM2.5 (ug/m3)"),
        col("pollutants.`PM10 (ug/m3)`").cast("double").alias("PM10 (ug/m3)"),
        col("pollutants.`NO2 (ug/m3)`").cast("double").alias("NO2 (ug/m3)"),
        col("pollutants.`CO (mg/m3)`").cast("double").alias("CO (mg/m3)"),
        col("pollutants.`Ozone (ug/m3)`").cast("double").alias("Ozone (ug/m3)")
    )

    id_columns = ["state", "city", "station", "observation_time"]
    
    # The column strings must also be wrapped in backticks here so unpivot does not misinterpret the dot
    value_columns = [
        "`PM2.5 (ug/m3)`", "`PM10 (ug/m3)`", "`NO2 (ug/m3)`", 
        "`CO (mg/m3)`", "`Ozone (ug/m3)`"
    ]

    unpivoted_df = prepared_df.unpivot(
        ids=id_columns,
        values=value_columns,
        variableColumnName="pollutant_id",
        valueColumnName="pollutant_avg"
    )

    return unpivoted_df.select(
        col("state"),
        col("city"),
        col("station"),
        col("pollutant_id"),
        col("pollutant_avg"),
        col("observation_time"),
        current_timestamp().alias("silver_processed_at")
    ).filter(col("pollutant_avg").isNotNull())