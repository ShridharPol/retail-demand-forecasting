-- Override dbt's default schema naming behavior
-- By default dbt appends the custom schema to the target schema
-- e.g. retail_staging_retail_staging — we don't want that
-- This macro makes dbt use the custom schema name directly

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}