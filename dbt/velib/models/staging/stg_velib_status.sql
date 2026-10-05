-- Relevés de disponibilité, renommés en français et convertis en heure locale.
select
    station_id,
    station_code,
    last_reported                                           as releve_at,
    last_reported at time zone '{{ var("timezone") }}'      as releve_local,
    num_bikes_available                                     as velos_disponibles,
    num_mechanical                                          as velos_mecaniques,
    num_ebike                                               as velos_electriques,
    num_docks_available                                     as bornes_libres,
    is_installed                                            as est_installee,
    is_renting                                              as location_active,
    is_returning                                            as retour_actif,
    ingested_at,
    loaded_at
from {{ source('staging', 'velib_status') }}
