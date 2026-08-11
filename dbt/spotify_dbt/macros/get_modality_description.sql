{#- To map Spotify mode flag to musical modality. -#}
{% macro get_modality_description(mode_col) -%}
    case {{ mode_col }}
        when 0 then 'Minor'
        when 1 then 'Major'
        else 'Unknown'
    end
{%- endmacro %}