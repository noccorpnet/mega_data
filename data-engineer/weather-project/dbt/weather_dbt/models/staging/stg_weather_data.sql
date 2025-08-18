SELECT
    "time"::date AS weather_date,
    name AS location_name,
    weather_code,
    temperature_2m_max AS max_temp_celsius,
    temperature_2m_min AS min_temp_celsius,
    precipitation_sum AS total_precipitation_mm
FROM
    {{ source('weather_dbt', 'weather_data') }}