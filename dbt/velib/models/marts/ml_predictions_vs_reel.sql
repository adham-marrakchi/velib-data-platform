-- Prévisions du modèle confrontées à la réalité observée 1 h plus tard (suivi de performance en production).
with predictions as (
    select distinct on (station_id, target_time)
        station_id, feature_time, target_time, predicted_bikes, model_name, model_version, predicted_at
    from {{ source('ml', 'predictions_disponibilite') }}
    order by station_id, target_time, predicted_at desc
)

select
    p.station_id,
    s.nom_station,
    s.zone,
    p.target_time                                                        as heure_cible,
    p.feature_time                                                       as heure_donnees,
    p.model_version                                                      as version_modele,
    round(p.predicted_bikes::numeric, 2)                                 as velos_predits,
    h.velos_disponibles                                                  as velos_reels,
    round(abs(p.predicted_bikes - h.velos_disponibles)::numeric, 2)      as erreur_absolue,
    h.velos_disponibles is not null                                      as est_evaluee
from predictions p
join {{ ref('dim_station') }} s on s.station_id = p.station_id
left join {{ ref('fct_station_horaire') }} h
    on h.station_id = p.station_id
   and h.heure_fin = p.target_time
