-- Disponibilité agrégée par zone (arrondissement / commune) et par heure : comparaison territoriale
-- et évolution du parc mécanique vs électrique.
select
    h.heure_debut,
    h.date_id,
    h.heure_locale                                          as heure,
    s.zone,
    s.commune,
    s.est_paris,
    count(*)                                                as nb_stations,
    sum(h.velos_disponibles)                                as velos_disponibles,
    sum(h.velos_mecaniques)                                 as velos_mecaniques,
    sum(h.velos_electriques)                                as velos_electriques,
    sum(h.bornes_libres)                                    as bornes_libres,
    round(avg(h.taux_remplissage), 4)                       as taux_remplissage_moyen,
    count(*) filter (where h.velos_disponibles = 0)         as nb_stations_vides,
    count(*) filter (where h.bornes_libres = 0)             as nb_stations_pleines
from {{ ref('fct_station_horaire') }} h
join {{ ref('dim_station') }} s on s.station_id = h.station_id
group by 1, 2, 3, 4, 5, 6
