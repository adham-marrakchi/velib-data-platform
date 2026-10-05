{#
  Tests génériques maison (équivalents de dbt_utils, sans dépendance externe).
#}

{% test accepted_range(model, column_name, min_value=none, max_value=none, inclusive=true) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and (
    false
    {% if min_value is not none %}
      or {{ column_name }} {{ '<' if inclusive else '<=' }} {{ min_value }}
    {% endif %}
    {% if max_value is not none %}
      or {{ column_name }} {{ '>' if inclusive else '>=' }} {{ max_value }}
    {% endif %}
  )
{% endtest %}


{% test unique_combination_of_columns(model, combination_of_columns) %}
select {{ combination_of_columns | join(', ') }}, count(*) as nb
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1
{% endtest %}
