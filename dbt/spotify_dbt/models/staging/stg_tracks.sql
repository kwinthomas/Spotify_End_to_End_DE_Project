with source as (
    select * from {{ source('silver', 'tracks_features') }}
),

renamed as (
    select
        id                as track_id,
        name              as track_name,
        album             as album_name,
        artists           as artist_names,
        artist_ids,
        artist_count,
        explicit          as is_explicit,
        danceability,
        energy,
        key               as key_id,
        loudness,
        mode              as mode_id,
        speechiness,
        acousticness,
        instrumentalness,
        liveness,
        valence,
        tempo,
        duration_s        as duration_seconds,
        year              as release_year,
        year_date         as release_year_date,
        release_date_parsed as release_date,
        year_was_repaired
    from source
)

select * from renamed