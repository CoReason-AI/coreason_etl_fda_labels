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
)

SELECT
    coreason_id,
    raw_data->>'set_id' AS source_id,
    raw_data->'openfda'->>'brand_name' AS brand_name,
    raw_data->'openfda'->>'generic_name' AS generic_name,
    -- openFDA clinical text blocks are often JSON arrays of strings. We MUST coalesce these.
    ARRAY_TO_STRING(ARRAY(SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(raw_data->'indications_and_usage') = 'array' THEN raw_data->'indications_and_usage' ELSE '[]'::jsonb END)), E'\n\n') AS indications_and_usage,
    ARRAY_TO_STRING(ARRAY(SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(raw_data->'boxed_warning') = 'array' THEN raw_data->'boxed_warning' ELSE '[]'::jsonb END)), E'\n\n') AS boxed_warning,
    ARRAY_TO_STRING(ARRAY(SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(raw_data->'adverse_reactions') = 'array' THEN raw_data->'adverse_reactions' ELSE '[]'::jsonb END)), E'\n\n') AS adverse_reactions,
    ARRAY_TO_STRING(ARRAY(SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(raw_data->'contraindications') = 'array' THEN raw_data->'contraindications' ELSE '[]'::jsonb END)), E'\n\n') AS contraindications,
    ARRAY_TO_STRING(ARRAY(SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(raw_data->'active_ingredient') = 'array' THEN raw_data->'active_ingredient' ELSE '[]'::jsonb END)), E'\n\n') AS active_ingredient,
    ARRAY_TO_STRING(ARRAY(SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(raw_data->'dosage_and_administration') = 'array' THEN raw_data->'dosage_and_administration' ELSE '[]'::jsonb END)), E'\n\n') AS dosage_and_administration,
    ARRAY_TO_STRING(ARRAY(SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(raw_data->'inactive_ingredient') = 'array' THEN raw_data->'inactive_ingredient' ELSE '[]'::jsonb END)), E'\n\n') AS inactive_ingredient,
    ARRAY_TO_STRING(ARRAY(SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(raw_data->'package_label_principal_display_panel') = 'array' THEN raw_data->'package_label_principal_display_panel' ELSE '[]'::jsonb END)), E'\n\n') AS package_label_principal_display_panel,
    ingestion_ts
FROM raw_labels
