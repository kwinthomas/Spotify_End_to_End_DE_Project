-- Fails if the mart loses or gains rows relative to staging.
with counts as (
    select
        (select count(*) from {{ ref('stg_tracks') }})          as staging_rows,
        (select count(*) from {{ ref('mart_track_features') }}) as mart_rows
)
select * from counts where staging_rows != mart_rows