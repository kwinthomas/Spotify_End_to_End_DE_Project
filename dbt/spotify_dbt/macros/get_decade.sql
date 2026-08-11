{#- Derives a decade label from a year integer. -#}
{% macro get_decade(year_col) -%}
    case
        when {{ year_col }} is null then 'Unknown'
        else concat(cast(floor({{ year_col }} / 10) * 10 as string), 's')
    end
{%- endmacro %}