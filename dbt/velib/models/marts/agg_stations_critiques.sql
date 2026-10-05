-- Temps passé VIDE (aucun vélo) ou PLEINE (aucune borne libre) par station, sur toute la période
-- et sur les 7 derniers jours. Les rangs permettent un "top 10" direct dans la BI.
with durees as (
    select
        station_id,
        sum(duree_minutes)                                                    as minutes_observees,
        sum(duree_minutes) filter (where est_vide)                            as minutes_vide,
        sum(duree_minutes) filter (where est_pleine)                          as minutes_pleine,
        sum(duree_minutes) filter (where releve_at >= now() - interval '7 days') as minutes_observees_7j,
        sum(duree_minutes) filter (where est_vide and releve_at >= now() - interval '7 days')   as minutes_vide_7j,
        sum(duree_minutes) filter (where est_pleine and releve_at >= now() - interval '7 days') as minutes_pleine_7j
    from {{ ref('int_releves_intervalles') }}
    group by station_id
),

pourcentages as (
    select
        d.station_id,
        round((d.minutes_observees / 60.0)::numeric, 1)                                       as heures_observees,
        round((coalesce(d.minutes_vide, 0) / 60.0)::numeric, 1)                               as heures_vide,
        round((coalesce(d.minutes_pleine, 0) / 60.0)::numeric, 1)                             as heures_pleine,
        round((coalesce(d.minutes_vide, 0) / nullif(d.minutes_observees, 0))::numeric, 4)     as part_temps_vide,
        round((coalesce(d.minutes_pleine, 0) / nullif(d.minutes_observees, 0))::numeric, 4)   as part_temps_pleine,
        round((coalesce(d.minutes_vide_7j, 0) / nullif(d.minutes_observees_7j, 0))::numeric, 4)   as part_temps_vide_7j,
        round((coalesce(d.minutes_pleine_7j, 0) / nullif(d.minutes_observees_7j, 0))::numeric, 4) as part_temps_pleine_7j
    from durees d
)

select
    p.station_id,
    s.nom_station,
    s.zone,
    s.commune,
    s.capacite,
    p.heures_observees,
    p.heures_vide,
    p.heures_pleine,
    p.part_temps_vide,
    p.part_temps_pleine,
    p.part_temps_vide_7j,
    p.part_temps_pleine_7j,
    rank() over (order by p.part_temps_vide desc nulls last)    as rang_vide,
    rank() over (order by p.part_temps_pleine desc nulls last)  as rang_pleine
from pourcentages p
join {{ ref('dim_station') }} s on s.station_id = p.station_id
