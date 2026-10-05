{{
    config(
        materialized='incremental',
        unique_key=['station_id', 'releve_at'],
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['station_id', 'releve_at'], 'unique': true},
            {'columns': ['date_id']},
            {'columns': ['loaded_at']},
        ],
    )
}}

-- Table de faits : une ligne par station et par relevé (grain le plus fin).
-- Incrémentale : seuls les relevés chargés depuis le dernier run sont traités.
with releves as (
    select *
    from {{ ref('stg_velib_status') }}
    {% if is_incremental() %}
    where loaded_at > (select coalesce(max(loaded_at), '1970-01-01'::timestamptz) from {{ this }})
    {% endif %}
)

select
    r.station_id,
    r.releve_at,
    to_char(r.releve_local, 'YYYYMMDD')::int                             as date_id,
    extract(hour from r.releve_local)::int                               as heure,
    r.velos_disponibles,
    r.velos_mecaniques,
    r.velos_electriques,
    r.bornes_libres,
    r.velos_disponibles + r.bornes_libres                                as capacite_effective,
    case
        when r.velos_disponibles + r.bornes_libres > 0
            then round(r.velos_disponibles::numeric / (r.velos_disponibles + r.bornes_libres), 4)
    end                                                                  as taux_remplissage,
    r.velos_disponibles = 0                                              as est_vide,
    r.bornes_libres = 0                                                  as est_pleine,
    r.est_installee,
    r.location_active,
    r.retour_actif,
    r.ingested_at,
    r.loaded_at
from releves r
-- uniquement les stations présentes dans le référentiel (intégrité de l'étoile)
inner join {{ ref('dim_station') }} s on s.station_id = r.station_id
