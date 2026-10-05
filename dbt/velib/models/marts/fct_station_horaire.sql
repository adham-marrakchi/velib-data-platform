{{
    config(
        materialized='incremental',
        unique_key=['station_id', 'heure_debut'],
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['station_id', 'heure_debut'], 'unique': true},
            {'columns': ['heure_debut']},
        ],
    )
}}

-- Photographie HORAIRE de chaque station : état connu à la fin de chaque heure (dernier relevé
-- antérieur, "forward fill"). Grille complète station x heure = base des séries temporelles
-- (agrégats horaires, features du modèle ML, courbes Superset).
with bornes as (
    select
        {% if is_incremental() %}
        (select max(heure_debut) from {{ this }}) - interval '{{ var("incremental_lookback_hours") }} hours' as h0,
        {% else %}
        date_trunc('hour', min(releve_at)) as h0,
        {% endif %}
        date_trunc('hour', max(releve_at)) as h1
    from {{ ref('fact_disponibilite') }}
),

heures as (
    select generate_series(h0, h1, interval '1 hour') as heure_debut from bornes
),

grille as (
    select s.station_id, h.heure_debut
    from {{ ref('dim_station') }} s
    cross join heures h
)

select
    g.station_id,
    g.heure_debut,
    g.heure_debut + interval '1 hour'                                                 as heure_fin,
    to_char(g.heure_debut at time zone '{{ var("timezone") }}', 'YYYYMMDD')::int      as date_id,
    extract(hour from g.heure_debut at time zone '{{ var("timezone") }}')::int        as heure_locale,
    d.velos_disponibles,
    d.velos_mecaniques,
    d.velos_electriques,
    d.bornes_libres,
    d.capacite_effective,
    d.taux_remplissage,
    d.releve_at                                                                       as dernier_releve_at,
    round(extract(epoch from (g.heure_debut + interval '1 hour' - d.releve_at)) / 60.0, 1) as minutes_depuis_releve
from grille g
cross join lateral (
    select f.*
    from {{ ref('fact_disponibilite') }} f
    where f.station_id = g.station_id
      and f.releve_at < g.heure_debut + interval '1 hour'
    order by f.releve_at desc
    limit 1
) d
