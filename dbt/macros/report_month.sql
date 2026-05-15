{# Resolves the calendar month (1-12) the marts should output. The MDM
   importers take one month of data at a time, so each mart is filtered
   to a single month.

   Default : the month before run_started_at. The container runs on the
             1st of each month, so an unattended run produces the month
             that just ended (run on May 1 -> April).
   Override: dbt run --vars 'report_month: <1-12>'

   Hours-of-darkness data is year-agnostic, so this is a bare month
   number with no year component. #}

{% macro report_month() %}
  {%- set override = var('report_month', none) -%}
  {%- if override is not none -%}
    {%- set m = override | int -%}
  {%- else -%}
    {%- set current = run_started_at.month -%}
    {%- set m = current - 1 if current > 1 else 12 -%}
  {%- endif -%}
  {%- if m < 1 or m > 12 -%}
    {{- exceptions.raise_compiler_error("report_month must be 1-12, got: " ~ m) -}}
  {%- endif -%}
  {{- return(m) -}}
{% endmacro %}
