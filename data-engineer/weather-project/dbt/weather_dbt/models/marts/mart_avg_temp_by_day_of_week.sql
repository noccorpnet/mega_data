Select 
    dd.day_of_week_name AS nama_hari,
    wd.location_name,

    AVG(wd.max_temp_celsius) AS rata_rata_suhu_maksimum

FROM
    {{ ref('stg_weather_data') }} as wd
LEFT JOIN
    {{ ref('dim_dates') }} as dd ON wd.weather_date = dd.date_day
Group by
    1,2,dd.day_of_week
Order by
    dd.day_of_week