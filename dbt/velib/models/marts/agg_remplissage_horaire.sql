-- Taux de remplissage moyen par station, type de jour (semaine / week-end-férié) et heure locale.
select
    h.station_id,
    s.nom_station,
    s.zone,
    d.type_jour,
    h.heure_locale                                          as heure,
    round(avg(h.taux_remplissage), 4)                       as taux_remplissage_moyen,
    round(avg(h.velos_disponibles), 2)                      as velos_disponibles_moyen,
    round(avg(h.velos_mecaniques), 2)                       as velos_mecaniques_moyen,
    round(avg(h.velos_electriques), 2)                      as velos_electriques_moyen,
    count(*)                                                as nb_heures_observees
from {{ ref('fct_station_horaire') }} h
join {{ ref('dim_date') }} d on d.date_id = h.date_id
join {{ ref('dim_station') }} s on s.station_id = h.station_id
group by 1, 2, 3, 4, 5
