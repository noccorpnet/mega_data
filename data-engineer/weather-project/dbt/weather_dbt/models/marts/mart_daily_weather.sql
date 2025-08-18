-- Using CTE for cleared data
With weather_data AS (
    SELECT * FROM {{ ref('stg_weather_data')}}
),

date_dimension AS (
    SELECT * FROM {{ ref('dim_dates') }}
),

location_dimension AS (
    SELECT * FROM {{ ref('dim_locations') }}
)




SELECT 
    wd.weather_date,
    wd.location_name,
    dl.province,
    wd.max_temp_celsius,
    wd.min_temp_celsius,
    wd.total_precipitation_mm,
    (wd.max_temp_celsius + wd.min_temp_celsius) / 2 AS avg_temp_celsius,

    dd.day_of_week_name AS nama_hari,
    dd.month_name AS nama_bulan,
    
    CASE
        WHEN wd.weather_code IN (0,1) THEN 'Cerah'
        WHEN wd.weather_code IN (2,3,45,48) THEN 'Berawan'
        WHEN wd.weather_code IN (51,53,55,56,57,61,63,65,66,67) THEN 'Hujan Ringan'
        WHEN wd.weather_code IN (71,73,75,77) THEN 'Hujan Sedang'
        WHEN wd.weather_code IN (80,81,82) THEN 'Hujan Lebat'
        WHEN wd.weather_code IN (95,96,99) THEN 'Hujan Badai / Petir'
        ELSE 'Lainnya'
    END AS weather_condition

FROM
    weather_data wd
LEFT JOIN
    date_dimension dd ON wd.weather_date = dd.date_day
LEFT JOIN
    location_dimension dl ON wd.location_name = dl.name


