import dlt
from pyspark.sql.functions import to_date, avg, max, round

# Hardcode the fully qualified Silver source tables
LIVE_TABLE = "aqi_data.silver.silver_cpcb_telemetry"
HISTORICAL_TABLE = "aqi_data.silver.silver_historical_telemetry"

# ==============================================================================
# 1. THE UNIFIED FACT TABLE (Live + Historical)
# ==============================================================================
@dlt.table(
    name="aqi_data.gold.gold_unified_telemetry",
    comment="Combined live and historical AQI telemetry for seamless time-series analysis",
    table_properties={"quality": "gold"}
)
def gold_unified_telemetry():
    # Read both Silver tables as static DataFrames (Materialized Views)
    live_df = spark.table(LIVE_TABLE)
    historical_df = spark.table(HISTORICAL_TABLE)
    
    # unionByName stacks the rows vertically. 
    # allowMissingColumns=True safely handles columns that exist in one table but not the other 
    # (e.g., live data has 'latitude' and 'longitude', historical might only have 'city' and 'station').
    return live_df.unionByName(historical_df, allowMissingColumns=True)


# ==============================================================================
# 2. DAILY AGGREGATIONS FOR THE HEAT MAP DASHBOARD
# ==============================================================================
@dlt.table(
    name="aqi_data.gold.daily_city_data",
    comment="Daily aggregated metrics optimized for heat maps and historical trend charts",
    table_properties={"quality": "gold"}
)
def gold_daily_city_summary():
    return (
        # Read directly from the newly created unified Gold table
        spark.read.table("aqi_data.gold.gold_unified_telemetry")
        
        # Group by location, pollutant, and the specific day
        .groupBy(
            "state", 
            "city", 
            "station",
            "latitude",   # Carried over from live data for heat map pins
            "longitude",  # Carried over from live data for heat map pins
            "pollutant_id", 
            to_date("observation_time").alias("date")
        )
        .agg(
            # Calculate metrics and enforce two decimal places
            round(avg("pollutant_avg"), 2).alias("daily_avg"),
            round(max("pollutant_avg"), 2).alias("daily_max")
        )
    )