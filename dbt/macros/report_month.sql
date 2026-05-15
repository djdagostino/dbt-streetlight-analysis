{# Resolves the calendar month (1-12) the marts should output. The MDM
   importers take one month of data at a time, so each mart is filtered
   to a single month.

   Default : the month of run_started_at. The pipeline runs on the 1st of
             each month and produces that month's estimated usage (a run on
             June 1 produces June). kWh is an estimate from a year-agnostic
             darkness profile, so the current month is fully computable on
             day one.
   Override: dbt run --vars 'report_month: <1-12>'

   Hours-of-darkness data is year-agnostic, so this is a bare month
   number with no year component. #}

{% macro report_month() %}
  {%- set override = var('report_month', none) -%}
  {%- if override is not none -%}
    {%- set m = override | int -%}
  {%- else -%}
    {%- set m = run_started_at.month -%}
  {%- endif -%}
  {%- if m < 1 or m > 12 -%}
    {{- exceptions.raise_compiler_error("report_month must be 1-12, got: " ~ m) -}}
  {%- endif -%}
  {{- return(m) -}}
{% endmacro %}
