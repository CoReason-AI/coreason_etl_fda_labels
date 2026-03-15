# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_labels

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import dlt

from coreason_etl_fda_labels.config import IngestionConfigManifest
from coreason_etl_fda_labels.discovery import EpistemicDiscoveryTask
from coreason_etl_fda_labels.extraction import EpistemicExtractionTask
from coreason_etl_fda_labels.utils.logger import logger


@dlt.source(max_table_nesting=0)  # type: ignore[misc]
def fda_spl_source(config: IngestionConfigManifest | None = None) -> dlt.sources.DltResource:
    """
    Epistemic boundary defining the ingestion manifold for the openFDA SPL dataset.

    AGENT INSTRUCTION: This source enforces the clean-room architecture by setting
    max_table_nesting=0 globally. This prevents fragile relational unnesting of
    complex JSON structures, deferring transformation to the dbt Silver layer.

    Args:
        config: The ingestion configuration manifest.

    Returns:
        A strictly typed dlt resource configured for incremental appending.
    """
    if config is None:
        config = IngestionConfigManifest()

    @dlt.resource(name="bronze_fda_labels_raw", write_disposition="append")  # type: ignore[misc]
    def bronze_fda_labels_raw() -> Iterator[list[dict[str, Any]]]:
        logger.info("Initializing Bronze Ingestion Manifold")

        # 1. Execute Discovery Phase
        discovery_task = EpistemicDiscoveryTask(config=config)
        locator_manifest = discovery_task.execute()

        # 2. Execute Extraction Phase
        extraction_task = EpistemicExtractionTask()
        ingestion_ts = datetime.now(UTC).isoformat()

        for partition_url in locator_manifest.partition_urls:
            for batch in extraction_task.execute(url=partition_url):
                # Ensure ingestion_ts is injected into the record schema
                for record in batch:
                    record["ingestion_ts"] = ingestion_ts
                yield batch

    return bronze_fda_labels_raw()
