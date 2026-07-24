-- Copyright (c) 2026 CoReason Inc.
--
-- This software is proprietary and dual-licensed.
-- Licensed under the Prosperity Public License 3.0 (the "License").
-- A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
-- For details, see the LICENSE file.
-- Commercial use beyond a 30-day trial requires a separate license.
--
-- Source Code: https://github.com/CoReason-AI/coreason_etl_fda_labels

WITH raw_labels AS (
    SELECT
        coreason_id,
        raw_data,
        ingestion_ts
    FROM {{ source('bronze', 'coreason_etl_fda_labels_bronze_fda_labels_raw') }}
),

unnested_applications AS (
    SELECT
        coreason_id,
        raw_data->>'set_id' AS source_id,
        -- Unnest the application_number array into individual rows
        jsonb_array_elements_text(
            CASE
                WHEN jsonb_typeof(raw_data->'openfda'->'application_number') = 'array'
                THEN raw_data->'openfda'->'application_number'
                ELSE '[]'::jsonb
            END
        ) AS raw_application_number,
        ingestion_ts
    FROM raw_labels
)

SELECT
    coreason_id,
    source_id,
    -- Strip alphabetic prefixes (e.g., NDA, ANDA, BLA) and whitespace
    REGEXP_REPLACE(raw_application_number, '^[A-Za-z\s]+', '') AS application_number,
    ingestion_ts
FROM unnested_applications
