-- Dimension heure de la journée (heure locale de Paris).
select
    h                                                   as heure,
    lpad(h::text, 2, '0') || 'h'                        as libelle_heure,
    case
        when h between 0 and 5 then 'nuit'
        when h between 6 and 9 then 'matin'
        when h between 10 and 13 then 'midi'
        when h between 14 and 16 then 'après-midi'
        when h between 17 and 20 then 'soir'
        else 'nuit'
    end                                                 as tranche_horaire,
    h in (7, 8, 9, 17, 18, 19)                          as est_heure_pointe
from generate_series(0, 23) as h
