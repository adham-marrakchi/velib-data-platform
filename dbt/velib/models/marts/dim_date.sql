-- Dimension calendrier (2025, ou le plus ancien relevé s'il est antérieur -> aujourd'hui + 1 an)
-- avec jours fériés français. Certaines stations hors service publient un relevé vieux de plusieurs années.
with debut as (
    select least(date '2025-01-01', coalesce(min(releve_local)::date, date '2025-01-01')) as date_debut
    from {{ ref('stg_velib_status') }}
),

jours as (
    select generate_series(d.date_debut, (current_date + interval '1 year')::date, interval '1 day')::date as date_jour
    from debut d
)

select
    to_char(j.date_jour, 'YYYYMMDD')::int                                         as date_id,
    j.date_jour,
    extract(isodow from j.date_jour)::int                                         as jour_semaine_num,
    (array['lundi', 'mardi', 'mercredi', 'jeudi', 'vendredi', 'samedi', 'dimanche'])
        [extract(isodow from j.date_jour)::int]                                   as jour_semaine,
    extract(isodow from j.date_jour) in (6, 7)                                    as est_weekend,
    f.date_ferie is not null                                                      as est_ferie,
    f.nom_ferie,
    case
        when extract(isodow from j.date_jour) in (6, 7) or f.date_ferie is not null then 'week-end / férié'
        else 'semaine'
    end                                                                           as type_jour,
    extract(day from j.date_jour)::int                                            as jour,
    extract(month from j.date_jour)::int                                          as mois,
    (array['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août',
           'septembre', 'octobre', 'novembre', 'décembre'])[extract(month from j.date_jour)::int] as nom_mois,
    extract(year from j.date_jour)::int                                           as annee,
    extract(week from j.date_jour)::int                                           as semaine_iso
from jours j
left join {{ ref('jours_feries') }} f on f.date_ferie = j.date_jour
