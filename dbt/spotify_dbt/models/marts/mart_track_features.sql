{{ config(materialized='table') }}

with tracks as (
    select * from {{ ref('stg_tracks') }}
),

enriched as (
    select
        track_id,
        track_name,
        album_name,
        artist_names,
        artist_ids,
        artist_count,
        is_explicit,

        {{ get_key_description('key_id') }}       as key_description,
        {{ get_modality_description('mode_id') }} as modality_description,

        danceability,
        energy,
        loudness,
        speechiness,
        acousticness,
        instrumentalness,
        liveness,
        valence,
        tempo,
        duration_seconds,
        round(duration_seconds / 60.0, 2) as duration_minutes,

        release_year,
        release_year_date,
        floor(release_year / 10) * 10 as decade_start,
        {{ get_decade('release_year') }} as decade,

        case
            when valence >= 0.6 then 'Happy'
            when valence <= 0.4 then 'Sad'
            else 'Ambivalent'
        end as mood,

        case
            when energy >= 0.66 then 'High'
            when energy >= 0.33 then 'Medium'
            else 'Low'
        end as energy_band,

        case
            when tempo < 90  then 'Slow'
            when tempo < 120 then 'Moderate'
            when tempo < 150 then 'Fast'
            else 'Very Fast'
        end as tempo_band

    from tracks
)

select * from enriched