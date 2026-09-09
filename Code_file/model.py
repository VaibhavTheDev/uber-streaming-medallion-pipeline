from pyspark import pipelines as dp
from pyspark.sql.functions import col, struct

SOURCE_TABLE = "uber.bronze.silver_obt_streaming"
# ---------------------------
# Dim Passenger - SCD Type 1
# ---------------------------
@dp.view
def dim_passenger_view():
    return (
        spark.readStream.table(SOURCE_TABLE)
        .select(
            "passenger_id",
            "passenger_name",
            "passenger_email",
            "passenger_phone",
            "booking_timestamp",
            "ride_id"
        )
    )

dp.create_streaming_table("dim_passenger")

dp.create_auto_cdc_flow(
    target="dim_passenger",
    source="dim_passenger_view",
    keys=["passenger_id"],
    sequence_by=struct(col("booking_timestamp"), col("ride_id")),
    except_column_list=["booking_timestamp", "ride_id"],
    stored_as_scd_type=1
)

# ---------------------------
# Dim Driver - SCD Type 1
# ---------------------------
@dp.view
def dim_driver_view():
    return (
        spark.readStream.table(SOURCE_TABLE)
        .select(
            "driver_id",
            "driver_name",
            "driver_rating",
            "driver_phone",
            "driver_license",
            "booking_timestamp",
            "ride_id"
        )
    )

dp.create_streaming_table("dim_driver")

dp.create_auto_cdc_flow(
    target="dim_driver",
    source="dim_driver_view",
    keys=["driver_id"],
    sequence_by=struct(col("booking_timestamp"), col("ride_id")),
    except_column_list=["booking_timestamp", "ride_id"],
    stored_as_scd_type=1
)

# ---------------------------
# Dim Vehicle - SCD Type 1
# ---------------------------
@dp.view
def dim_vehicle_view():
    return (
        spark.readStream.table(SOURCE_TABLE)
        .select(
            "vehicle_id",
            "vehicle_make_id",
            "vehicle_type_id",
            "vehicle_model",
            "vehicle_color",
            "license_plate",
            "vehicle_make",
            "vehicle_type",
            "booking_timestamp",
            "ride_id"
        )
    )

dp.create_streaming_table("dim_vehicle")

dp.create_auto_cdc_flow(
    target="dim_vehicle",
    source="dim_vehicle_view",
    keys=["vehicle_id"],
    sequence_by=struct(col("booking_timestamp"), col("ride_id")),
    except_column_list=["booking_timestamp", "ride_id"],
    stored_as_scd_type=1
)

# ---------------------------
# Dim Payment - SCD Type 1
# ---------------------------
@dp.view
def dim_payment_view():
    return (
        spark.readStream.table(SOURCE_TABLE)
        .select(
            "payment_method_id",
            "payment_method",
            "is_card",
            "requires_auth",
            "booking_timestamp",
            "ride_id"
        )
    )

dp.create_streaming_table("dim_payment")

dp.create_auto_cdc_flow(
    target="dim_payment",
    source="dim_payment_view",
    keys=["payment_method_id"],
    sequence_by=struct(col("booking_timestamp"), col("ride_id")),
    except_column_list=["booking_timestamp", "ride_id"],
    stored_as_scd_type=1
)

# ---------------------------
# Dim Booking - SCD Type 1
# ---------------------------
@dp.view
def dim_booking_view():
    return (
        spark.readStream.table(SOURCE_TABLE)
        .select(
            "ride_id",
            "confirmation_number",
            "pickup_location_id",
            "dropoff_location_id",
            "ride_status_id",
            "dropoff_city_id",
            "cancellation_reason_id",
            "pickup_address",
            "pickup_latitude",
            "pickup_longitude",
            "dropoff_address",
            "dropoff_latitude",
            "dropoff_longitude",
            "booking_timestamp",
            "dropoff_timestamp"
        )
    )

dp.create_streaming_table("dim_booking")

dp.create_auto_cdc_flow(
    target="dim_booking",
    source="dim_booking_view",
    keys=["ride_id"],
    sequence_by="booking_timestamp",
    stored_as_scd_type=1
)

# ---------------------------
# Dim Location - SCD Type 2
# ---------------------------
@dp.view
def dim_location_view():
    return (
        spark.readStream.table(SOURCE_TABLE)
        .select(
            col("pickup_city_id").alias("city_id"),
            col("pickup_city").alias("city"),
            col("pickup_state").alias("state"),
            col("pickup_region").alias("region"),
            col("city_updated_at")
        )
    )

dp.create_streaming_table("dim_location")

dp.create_auto_cdc_flow(
    target="dim_location",
    source="dim_location_view",
    keys=["city_id"],
    sequence_by="city_updated_at",
    stored_as_scd_type=2
)

# ---------------------------
# Fact Ride - SCD Type 1
# ---------------------------
@dp.view
def fact_view():
    return (
        spark.readStream.table(SOURCE_TABLE)
        .select(
            "ride_id",
            "pickup_city_id",
            "payment_method_id",
            "driver_id",
            "passenger_id",
            "vehicle_id",
            "booking_timestamp",
            "distance_miles",
            "duration_minutes",
            "base_fare",
            "distance_fare",
            "time_fare",
            "surge_multiplier",
            "total_fare",
            "tip_amount",
            "rating",
            "base_rate",
            "per_mile",
            "per_minute"
        )
    )

dp.create_streaming_table("fact")

dp.create_auto_cdc_flow(
    target="fact",
    source="fact_view",
    keys=["ride_id"],
    sequence_by="booking_timestamp",
    stored_as_scd_type=1
)