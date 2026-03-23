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
    l.coreason_id,
    l.source_id,
    a.application_number,
    l.brand_name,
    l.generic_name,
    l.indications_and_usage,
    l.boxed_warning,
    l.adverse_reactions,
    l.contraindications,
    l.active_ingredient,
    l.dosage_and_administration,
    l.inactive_ingredient,
    l.package_label_principal_display_panel,
    l.ingestion_ts
FROM {{ ref('coreason_etl_fda_labels_silver_fda_labels') }} AS l
INNER JOIN {{ ref('coreason_etl_fda_labels_silver_fda_application_numbers') }} AS a
    ON l.coreason_id = a.coreason_id
