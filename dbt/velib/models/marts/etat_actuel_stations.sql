-- Dernier état connu de chaque station (carte "temps réel" de Superset).
with dernier as (
    select distinct on (station_id) *
    from {{ ref('fact_disponibilite') }}
    order by station_id, releve_at desc
)

select
    s.station_id,
    s.nom_station,
    s.zone,
    s.commune,
    s.arrondissement,
    s.est_paris,
    s.latitude,
    s.longitude,
    s.capacite,
    d.releve_at                                                          as dernier_releve_at,
    round(extract(epoch from (now() - d.releve_at)) / 60.0, 1)           as minutes_depuis_releve,
    d.velos_disponibles,
    d.velos_mecaniques,
    d.velos_electriques,
    d.bornes_libres,
    d.taux_remplissage,
    case
        when not d.est_installee or not d.location_active then 'hors service'
        when d.est_vide then 'vide'
        when d.est_pleine then 'pleine'
        when d.taux_remplissage < 0.2 then 'presque vide'
        when d.taux_remplissage > 0.8 then 'presque pleine'
        else 'disponible'
    end                                                                  as statut
from {{ ref('dim_station') }} s
join dernier d on d.station_id = s.station_id
