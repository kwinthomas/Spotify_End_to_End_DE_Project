# Spotify 1.2M Tracks — Azure Lakehouse Pipeline

An end-to-end data engineering project that ingests 1.2 million Spotify tracks into an Azure
lakehouse via Azure Data Factory, transforms them through a medallion architecture in Databricks,
models them with dbt, and serves the result to a Power BI dashboard.

Built as a portfolio project to demonstrate Azure Data Factory, Azure Data Lake Storage Gen2,
Databricks, PySpark, Delta Lake, Unity Catalog, dbt Core and Power BI working together as one
governed, version-controlled system.

---

## Contents

- [Architecture](#architecture)
- [Dataset](#dataset)
- [Repository layout](#repository-layout)
- [The pipeline](#the-pipeline)
  - [0. Storage and governance](#0-storage-and-governance)
  - [1. Ingestion — Azure Data Factory](#1-ingestion--azure-data-factory)
  - [2. Bronze — raw-faithful landing](#2-bronze--raw-faithful-landing)
  - [3. Silver — cleaning and typing](#3-silver--cleaning-and-typing)
  - [4. Gold — dbt models](#4-gold--dbt-models)
  - [5. Serving — Power BI](#5-serving--power-bi)
- [Source control strategy](#source-control-strategy)
- [Engineering decisions](#engineering-decisions)
- [Deviations from the original](#deviations-from-the-original)
- [Findings](#findings)
- [Running it yourself](#running-it-yourself)
- [Not implemented](#not-implemented)
- [Cost](#cost)

---

## Architecture

```
                Kaggle CSV (1.2M rows)
                        │
                        │  committed to GitHub via Git LFS
                        ▼
  ┌─────────────────────────────────────────────┐
  │  Azure Data Factory · spotifydeprojectadf   │
  │                                             │
  │   git_to_adls pipeline                      │
  │     Lookup  → file_list_lookup              │  metadata-driven file list
  │     ForEach → for_each_file                 │  iterate, scale without edits
  │       Copy  → copy_git_to_adls              │  HTTP source → ADLS sink
  └─────────────────────┬───────────────────────┘
                        ▼
            ┌───────────────────────┐
            │  ADLS Gen2 (staging)  │   hierarchical namespace enabled
            └───────────┬───────────┘
                        │  
                        ▼
┌───────────────────────────────────────────────┐
│  Azure Databricks  ·  Unity Catalog           │
│                                               │
│   BRONZE   spotify.bronze.tracks_features     │  Delta · all columns StringType
│      │     raw-faithful, lineage columns      │
│      ▼                                        │
│   SILVER   spotify.silver.tracks              │  Delta · typed, cleaned, constrained
│      │     year repaired, artists parsed      │
│      ▼                                        │
│   GOLD     spotify.gold.stg_tracks    (view)  │  dbt Core via SQL Warehouse
│            spotify.gold.mart_track_features   │  Delta · enriched, tested
└───────────────────────┬───────────────────────┘
                        │  Databricks connector · Import mode
                        ▼
                ┌───────────────┐
                │   Power BI    │  Including DAX measures
                └───────────────┘
```

**Stack**

| Layer | Technology |
|---|---|
| Ingestion | Azure Data Factory — Lookup / ForEach / Copy, HTTP linked service |
| Storage | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks (PySpark, Delta Lake) |
| Governance | Unity Catalog — catalog, schemas, external locations, storage credential |
| Transformation | PySpark (bronze, silver) · dbt Core with `dbt-databricks` (gold) |
| Query engine | Databricks Serverless SQL Warehouse |
| Visualisation | Power BI Desktop (Import mode) |
| Infrastructure | Azure Portal, provisioned manually |
| Version control | GitHub — ADF Git integration, Databricks Git folders, Git LFS for the dataset |

---

## Dataset

[Spotify 1.2M+ Songs](https://www.kaggle.com/datasets/rodolfofigueroa/spotify-12m-songs) by
Rodolfo Figueroa, sourced from the Spotify Web API.

- **1,204,025 rows**, one per track
- 24 source columns — identifiers, metadata, and Spotify's derived audio features
  (danceability, energy, valence, tempo, key, mode, loudness, acousticness,
  instrumentalness, liveness, speechiness)
- Release years span the early 1900s to 2020, heavily weighted toward recent decades

---

## High level Repository layout (Not exhaustive)

```
Spotify_End_to_End_DE_Project/
├── Raw_Dataset/                     source CSV, tracked via Git LFS
├── spotifydeprojectadf/             ADF Git integration root
│   ├── pipeline/                    git_to_adls
│   ├── dataset/                     HTTP source, ADLS sink
│   ├── linkedService/               git_big_file_ls, ADLS linked service
│   ├── factory/                     factory-level config
│   └── publish_config.json
├── Databricks_Analysis/             Databricks Git folder
│   ├── 00_setup_catalog.py          Unity Catalog, schemas
│   ├── 01_bronze_ingest.py          CSV → Delta, explicit schema
│   └── 02_silver_transform.py       Cleaning, typing, constraints
├── dbt/
│   └── spotify_dbt/
│       ├── dbt_project.yml
│       ├── packages.yml             dbt_utils
│       ├── models/
│       │   ├── staging/
│       │   │   ├── _sources.yml     silver.tracks source + tests
│       │   │   └── stg_tracks.sql   renaming only
│       │   └── marts/
│       │       ├── _marts.yml       schema tests, docs
│       │       └── mart_track_features.sql
│       ├── macros/
│       │   ├── get_key_description.sql
│       │   ├── get_modality_description.sql
│       │   └── get_decade.sql
│       └── tests/
│           └── assert_mart_rowcount_matches_silver.sql
├── powerbi/
│   └── spotify_dashboard.pbix
├── docs/
│   └── images/                      dashboard screenshots, ADF DAG, dbt lineage
└── README.md
```

---

## The pipeline

### 0. Storage and governance

Databricks reaches storage through an **Access Connector** with a system-assigned managed
identity, granted **Storage Blob Data Contributor** on the account. 

```sql
CREATE CATALOG spotify MANAGED LOCATION 'abfss://...',
CREATE SCHEMA spotify.bronze,   -- raw-faithful
CREATE SCHEMA spotify.silver,   -- cleaned, typed
CREATE SCHEMA spotify.gold,     -- dbt-managed marts
```

### 1. Ingestion — Azure Data Factory

`spotifydeprojectadf` · pipeline `git_to_adls`

Rather than uploading the source file by hand, ingestion is an orchestrated ADF pipeline. The
CSV is committed to this repository giving the pipeline a stable HTTP endpoint
to pull from, and the pipeline lands it in the ADLS staging container.

**Pipeline design**

| Activity | Name | Role |
|---|---|---|
| Lookup | `file_list_lookup` | Reads a config file listing the files to ingest |
| ForEach | `for_each_file` | Iterates the Lookup output |
| Copy | `copy_git_to_adls` | HTTP source (`git_big_file_ls`) → ADLS Gen2 sink, per file |

**Git integration.** The factory is connected to this repository rather than using ADF's live
mode. Work happens on feature branches, is merged to `main` via pull request, and publishing
generates ARM templates into the `adf_publish` branch. See
[Source control strategy](#source-control-strategy).

### 2. Bronze — raw-faithful landing

`Databricks_Analysis/01_bronze_ingest.py`

Bronze does three things: land the data as Delta, make it queryable, record
where it came from.

**Every column is read as `StringType`.** Reading as string guarantees bronze loses nothing
and makes the cast an explicit, testable gate in silver.

CSV track names contain commas and embedded quotes, and `artists`
is a stringified Python list:

```python
.option("quote", '"').option("escape", '"').option("multiLine", True)
```

Lineage columns are appended on every load:

| Column | Source |
|---|---|
| `_source_file` | Spark's `_metadata.file_path` |
| `_ingested_at` | `current_timestamp()` |
| `_batch_id` | UUID generated per run |

The write is `overwrite`, making the notebook idempotent.

### 3. Silver — cleaning and typing

`Databricks_Analysis/02_silver_transform.py`

**Year repair.** A small number of rows have missing or implausible `year` values. 
The repair is **data-driven**: rows where `year` is null or outside 1900–2026 are
reconstructed from `release_date`, which parses inconsistently across three formats and is
handled with a `coalesce` cascade:

```python
F.coalesce(
    F.to_date(F.col("release_date"), "yyyy-MM-dd"),                              # yyyy-MM-dd
    F.to_date(F.concat_ws("-", "release_date", F.lit("01")), "yyyy-MM-dd"),      # yyyy-MM
    F.to_date(F.concat_ws("-", "release_date", F.lit("01"), F.lit("01")), ...),  # yyyy
)
```

**Other transformations**

- `artists` is parsed from `['Artist A', 'Artist B']` into a proper array, then exposed as a
  readable string plus `primary_artist` and `artist_count`
- Audio features cast to double and rounded to 2 decimal places
- `tempo` uses a double-then-int cast — casting `"120.5"` directly to int returns null in Spark,
  unlike pandas. A common porting bug.
- `duration_s` derived from `duration_ms`
- Deduplication on `id` via window function, retaining the earliest release

### 4. Gold — dbt models

`dbt/spotify_dbt/`

dbt Core runs locally against a Databricks Serverless SQL Warehouse via the `dbt-databricks`
adapter. Credentials come from an environment variable, `profiles.yml` is not committed (for obv reasones).

**`stg_tracks`** (view) — renaming and light recasting only, no business logic.

**`mart_track_features`** (table) — the analytics layer. Adds:

- `key_description` and `modality_description` via Jinja macros mapping Spotify's integer
  pitch class and mode flag to musical notation
- `decade` and `decade_start`, derived arithmetically
- `mood` from valence — Happy ≥ 0.6, Sad ≤ 0.4, Ambivalent between
- `energy_band` and `tempo_band`
- `duration_minutes`

**Testing** — `unique` and `not_null` on the primary key, `accepted_values` on every derived
category, `dbt_utils.accepted_range` on the audio features and release year, plus a custom
singular test asserting the mart's row count matches staging.

```bash
dbt deps && dbt run && dbt test
```

### 5. Serving — Power BI

Measures are defined in DAX rather than dragging raw columns onto visuals, so that they are reusable:

Example:
```dax
Track Count = COUNTROWS('mart_track_features')

Artist Count = DISTINCTCOUNT('mart_track_features'[primary_artist])

Avg Danceability = AVERAGE('mart_track_features'[danceability])

Avg Energy = AVERAGE('mart_track_features'[energy])

Avg Valence = AVERAGE('mart_track_features'[valence])

Avg Duration (min) = AVERAGE('mart_track_features'[duration_minutes])

Explicit % = 
DIVIDE(
    CALCULATE([Track Count], 'mart_track_features'[is_explicit] = TRUE()),
    [Track Count]
)

Happy % = 
DIVIDE(
    CALCULATE([Track Count], 'mart_track_features'[mood] = "Happy"),
    [Track Count]
)

Repaired Years = 
CALCULATE([Track Count], 'mart_track_features'[year_was_repaired] = TRUE())
```

<img width="3188" height="1802" alt="Dashboard" src="https://github.com/user-attachments/assets/ed16c4a1-5ace-4b9a-abb0-429176e28eae" />

---

## Findings

- **Collection bias.** Track volume rises steeply toward 2020. This is a property of the dataset,
  not of music production, and it undermines naive year-over-year comparison — worth stating
  before any trend is read.
- **Louder and sadder.** Average loudness and energy climb steadily from the 1960s while average
  valence declines. The loudness trend is a well-documented artefact of mastering practice, the
  valence trend is harder to attribute.
- **Collaboration is rising.** Mean `artist_count` per track increases sharply in recent decades.
- **Major keys dominate**, and within them C, G and D — consistent with guitar and piano
  writing conventions.

---

## Running it yourself

**Prerequisites** — Azure subscription, Python 3.9+, Git LFS installed locally if you want the source file.

```bash
# 1. Azure resources (portal)
#    Resource group → ADLS Gen2 with hierarchical namespace → containers
#    Databricks workspace (Premium) → Access Connector → Storage Blob Data Contributor
#    Data Factory → connect to this repo in Git mode

# 2. Run the ADF pipeline
#    git_to_adls → Debug, or trigger manually
#    Lands the CSV in the ADLS staging container

# 3. In Databricks: clone this repo into a Git folder, then run
#    Databricks_Analysis/00_setup_catalog.py
#    Databricks_Analysis/01_bronze_ingest.py
#    Databricks_Analysis/02_silver_transform.py

# 4. dbt, locally
cd dbt/spotify_dbt
python -m venv .venv && source .venv/bin/activate
pip install dbt-core dbt-databricks
export DBT_DATABRICKS_TOKEN='dapi...'
dbt deps && dbt debug && dbt run && dbt test

# 5. Power BI Desktop → Get data → Azure Databricks → Import
#    Authenticate with a Databricks personal access token
```

Update storage account names and warehouse HTTP paths to match your own. A
`profiles.yml.example` is provided, the real file belongs in `~/.dbt/`.

**On macOS**, Power BI Desktop is Windows-only. This project used a Windows Server VM in the
same Azure subscription, accessed over RDP with folder redirection to retrieve the `.pbix`.
Deallocate the VM from the portal — shutting down inside Windows keeps it billing.

---

## Licence

MIT. Dataset licensed separately by its Kaggle publisher.
