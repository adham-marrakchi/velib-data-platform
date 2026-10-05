-- Dimension station (SCD type 1 : on garde la dernière version connue de chaque station).
select
    station_id,
    station_code,
    nom_station,
    commune,
    arrondissement,
    zone,
    departement,
    est_paris,
    latitude,
    longitude,
    capacite,
    premiere_apparition,
    derniere_apparition
from {{ ref('stg_velib_stations') }}
