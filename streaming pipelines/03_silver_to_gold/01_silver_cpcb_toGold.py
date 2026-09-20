import dlt
from pyspark.sql.functions import col, avg, max, to_date, count, round

# Hardcode the source table to read from your completed Silver layer
SOURCE_TABLE = "aqi_data.silver.silver_cpcb_telemetry"

# ==============================================================================
# 1. REAL-TIME ALERTING TABLE (Streaming Table)
# ==============================================================================
@dlt.table(
    name="critical_stations_alert",
    comment="Real-time stream of stations reporting hazardous pollutant levels above 80",
    table_properties={"quality": "gold"}
)
def critical_stations_alert():
    return (
        # Using readStream because alert filtering is a row-by-row operation
        # that needs to happen instantly as new data arrives from Silver.
        spark.readStream.table(SOURCE_TABLE)
        
        # Filter for hazardous AQI levels updated to trigger at > 80
        .filter(col("pollutant_avg") > 80)
        
        # Select necessary columns and round the pollutant average to 2 decimal places
        .select(
            col("station"),
            col("city"),
            col("state"),
            col("pollutant_id"),
            round(col("pollutant_avg"), 2).alias("pollutant_avg"),
            col("observation_time")
        )
    )

# ==============================================================================
# 2. DAILY CITY SUMMARY (Materialized View)
# ==============================================================================
@dlt.table(
    name="daily_city_summary",
    comment="Daily aggregated minimum, maximum, and average pollutant metrics per city (rounded to 2 decimals)",
    table_properties={"quality": "gold"}
)
def daily_city_summary():
    return (
        # Using spark.table() (Materialized View) to calculate global aggregations
        spark.table(SOURCE_TABLE)
        .groupBy("state", "city", to_date("observation_time").alias("date"))
        .agg(
            # Calculate average and max, rounding both to exactly 2 decimal places
            round(avg("pollutant_avg"), 2).alias("city_daily_avg"),
            round(max("pollutant_max"), 2).alias("city_daily_max"),
            count("station").alias("active_stations_count")
        )
    )

# ==============================================================================
# 3. CURRENT STATE RANKING (Materialized View)
# ==============================================================================
@dlt.table(
    name="state_ranking_current",
    comment="Current ranking of states by average pollutant levels (rounded to 2 decimals)",
    table_properties={"quality": "gold"}
)
def state_ranking_current():
    return (
        spark.table(SOURCE_TABLE)
        .groupBy("state")
        .agg(
            # Calculate state-wide average and round to 2 decimal places
            round(avg("pollutant_avg"), 2).alias("state_overall_avg")
        )
        # Sort states from highest pollution average to lowest
        .orderBy(col("state_overall_avg").desc())
    )