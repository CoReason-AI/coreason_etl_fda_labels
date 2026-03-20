{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {#
           By default dbt prefixes custom schemas with the target schema (e.g. public_silver).
           For Medallion architecture, we want the schema to be exactly 'silver' or 'gold'
           regardless of the user's default schema target.
        #}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
