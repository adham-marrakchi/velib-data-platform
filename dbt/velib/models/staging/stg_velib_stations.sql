-- Référentiel des stations enrichi : commune, arrondissement parisien, département, zone lisible.
with source as (
    select * from {{ source('staging', 'velib_stations') }}
)

select
    station_id,
    station_code,
    station_name                                                   as nom_station,
    lat                                                            as latitude,
    lon                                                            as longitude,
    capacity                                                       as capacite,
    code_insee,
    case when code_insee like '751%' then 'Paris' else coalesce(commune, 'Inconnue') end as commune,
    case when code_insee like '751%' then substr(code_insee, 4, 2)::int end              as arrondissement,
    case
        when code_insee = '75101' then 'Paris 1er'
        when code_insee like '751%' then 'Paris ' || substr(code_insee, 4, 2)::int || 'e'
        else coalesce(commune, 'Inconnue')
    end                                                            as zone,
    left(code_insee, 2)                                            as departement,
    coalesce(code_insee like '751%', false)                        as est_paris,
    first_seen                                                     as premiere_apparition,
    last_seen                                                      as derniere_apparition
from source
