{# Use the literal custom schema name instead of dbt's default
   <target_schema>_<custom_schema> concatenation.

   With this macro:
     +schema: raw         -> writes to <db>.raw.*
     +schema: staging     -> writes to <db>.staging.*
     +schema: marts       -> writes to <db>.marts.*

   Without this macro, target.schema = 'dbo' would produce dbo_raw, dbo_staging,
   etc., which collides with the literal raw schema created by ingest DDL. #}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
