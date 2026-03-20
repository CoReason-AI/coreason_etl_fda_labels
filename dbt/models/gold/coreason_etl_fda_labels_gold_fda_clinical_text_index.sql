-- Copyright (c) 2026 CoReason Inc.
--
-- This software is proprietary and dual-licensed.
-- Licensed under the Prosperity Public License 3.0 (the "License").
-- A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
-- For details, see the LICENSE file.
-- Commercial use beyond a 30-day trial requires a separate license.
--
-- Source Code: https://github.com/CoReason-AI/coreason_etl_fda_labels

SELECT
    coreason_id,
    source_id,
    brand_name,
    generic_name,
    CONCAT_WS(
        E'\n\n',
        'BRAND NAME: ' || COALESCE(brand_name, 'UNKNOWN'),
        'GENERIC NAME: ' || COALESCE(generic_name, 'UNKNOWN'),
        'INDICATIONS AND USAGE:',
        COALESCE(NULLIF(indications_and_usage, ''), 'None'),
        'BOXED WARNING:',
        COALESCE(NULLIF(boxed_warning, ''), 'None'),
        'ADVERSE REACTIONS:',
        COALESCE(NULLIF(adverse_reactions, ''), 'None'),
        'CONTRAINDICATIONS:',
        COALESCE(NULLIF(contraindications, ''), 'None')
    ) AS clinical_context_block,
    ingestion_ts
FROM {{ ref('coreason_etl_fda_labels_silver_fda_labels') }}
