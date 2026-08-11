{#- To map Spotify's integer pitch class to standard note notation, here -1 means no key detected. -#}
{% macro get_key_description(key_col) -%}
    case {{ key_col }}
        when 0  then 'C'
        when 1  then 'C#'
        when 2  then 'D'
        when 3  then 'D#'
        when 4  then 'E'
        when 5  then 'F'
        when 6  then 'F#'
        when 7  then 'G'
        when 8  then 'G#'
        when 9  then 'A'
        when 10 then 'A#'
        when 11 then 'B'
        else 'Unknown'
    end
{%- endmacro %}