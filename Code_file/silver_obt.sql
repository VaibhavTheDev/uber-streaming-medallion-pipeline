CREATE OR REFRESH STREAMING TABLE silver_obt_streaming
AS
SELECT
    r.*,
    vm.vehicle_make,
    vt.vehicle_type,
    vt.description AS vehicle_type_description,
    vt.base_rate,
    vt.per_mile,
    vt.per_minute,
    rs.ride_status,
    pm.payment_method,
    pm.is_card,
    pm.requires_auth,
    c.city AS pickup_city,
    c.state AS pickup_state,
    c.region AS pickup_region,
    c.updated_at AS city_updated_at,
    cr.cancellation_reason

FROM STREAM(uber.bronze.stg_rides) AS r

LEFT JOIN uber.bronze.map_vehicle_makes AS vm
    ON r.vehicle_make_id = vm.vehicle_make_id

LEFT JOIN uber.bronze.map_vehicle_types AS vt
    ON r.vehicle_type_id = vt.vehicle_type_id

LEFT JOIN uber.bronze.map_ride_statuses AS rs
    ON r.ride_status_id = rs.ride_status_id

LEFT JOIN uber.bronze.map_payment_methods AS pm
    ON r.payment_method_id = pm.payment_method_id

LEFT JOIN uber.bronze.map_cities AS c
    ON r.pickup_city_id = c.city_id

LEFT JOIN uber.bronze.map_cancellation_reasons AS cr
    ON r.cancellation_reason_id = cr.cancellation_reason_id;