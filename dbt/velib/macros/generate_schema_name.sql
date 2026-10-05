{#
  Par défaut dbt préfixe le schéma personnalisé par le schéma cible (gold_dbt_staging...).
  Ici on veut des schémas lisibles : "gold" pour les marts, "dbt_staging" pour les vues intermédiaires.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
