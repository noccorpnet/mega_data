SELECT
    name,
    lat AS latitude,
    lng AS longitude,
    population,
    "adminName1" as province

FROM
    {{ source('geolocation_dbt', 'geolocation') }}