-- Durée pendant laquelle chaque relevé reste "vrai" : jusqu'au relevé suivant de la station,
-- plafonnée à max_gap_minutes (une station muette n'est pas comptée vide/pleine indéfiniment).
select
    f.station_id,
    f.releve_at,
    f.date_id,
    f.heure,
    f.est_vide,
    f.est_pleine,
    least(
        extract(epoch from (
            coalesce(lead(f.releve_at) over (partition by f.station_id order by f.releve_at), now()) - f.releve_at
        )) / 60.0,
        {{ var('max_gap_minutes') }}
    )                                                                    as duree_minutes
from {{ ref('fact_disponibilite') }} f
