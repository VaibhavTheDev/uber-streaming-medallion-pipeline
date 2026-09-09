from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json

# JSON structure inside the `rides` column of uber.bronze.rides_raw
RIDES_JSON_SCHEMA = """
STRUCT<
    ride_id: STRING,
    confirmation_number: STRING,
    passenger_id: STRING,
    driver_id: STRING,
    vehicle_id: STRING,
    pickup_location_id: STRING,
    dropoff_location_id: STRING,
    vehicle_type_id: BIGINT,
    vehicle_make_id: BIGINT,
    payment_method_id: BIGINT,
    ride_status_id: BIGINT,
    pickup_city_id: BIGINT,
    dropoff_city_id: BIGINT,
    cancellation_reason_id: BIGINT,
    passenger_name: STRING,
    passenger_email: STRING,
    passenger_phone: STRING,
    driver_name: STRING,
    driver_rating: DOUBLE,
    driver_phone: STRING,
    driver_license: STRING,
    vehicle_model: STRING,
    vehicle_color: STRING,
    license_plate: STRING,
    pickup_address: STRING,
    pickup_latitude: DOUBLE,
    pickup_longitude: DOUBLE,
    dropoff_address: STRING,
    dropoff_latitude: DOUBLE,
    dropoff_longitude: DOUBLE,
    distance_miles: DOUBLE,
    duration_minutes: BIGINT,
    booking_timestamp: STRING,
    pickup_timestamp: STRING,
    dropoff_timestamp: STRING,
    base_fare: DOUBLE,
    distance_fare: DOUBLE,
    time_fare: DOUBLE,
    surge_multiplier: DOUBLE,
    subtotal: DOUBLE,
    tip_amount: DOUBLE,
    total_fare: DOUBLE,
    rating: DOUBLE
>
"""

# Target staging table
dp.create_streaming_table("stg_rides")

# Read new records from Bronze, parse JSON, and append to staging
@dp.append_flow(target="stg_rides")
def load_rides():
    df = spark.readStream.table("uber.bronze.rides_raw")

    df_parsed = (
        df.withColumn(
            "parsed_rides",
            from_json(col("rides"), RIDES_JSON_SCHEMA)
        )
        .select("parsed_rides.*")
    )

    return df_parsed.withColumn(
        "booking_timestamp",
        col("booking_timestamp").cast("timestamp")
    )