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

import dlt
import pyarrow as pa

from coreason_etl_fda_labels.config import IngestionConfigManifest
from coreason_etl_fda_labels.discovery import EpistemicDiscoveryTask
from coreason_etl_fda_labels.extraction import EpistemicExtractionTask
from coreason_etl_fda_labels.utils.logger import logger


@dlt.source(max_table_nesting=0)  # type: ignore[misc, unused-ignore]
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

    @dlt.resource(name="coreason_etl_fda_labels_bronze_fda_labels_raw", write_disposition="append")  # type: ignore[misc, unused-ignore]
    def coreason_etl_fda_labels_bronze_fda_labels_raw() -> Iterator[pa.Table]:
        logger.info("Initializing Bronze Ingestion Manifold")

        # 1. Execute Discovery Phase
        discovery_task = EpistemicDiscoveryTask(config=config)
        locator_manifest = discovery_task.execute()

        # 2. Execute Extraction Phase
        extraction_task = EpistemicExtractionTask()
        ingestion_ts = datetime.now(UTC).isoformat()

        for partition_url in locator_manifest.partition_urls:
            for batch_table in extraction_task.execute(url=partition_url):
                # Inject ingestion_ts directly into the Arrow Table as a fast contiguous array
                num_rows = batch_table.num_rows
                ts_array = pa.array([ingestion_ts] * num_rows, type=pa.string())
                augmented_table = batch_table.append_column("ingestion_ts", ts_array)
                yield augmented_table

    return coreason_etl_fda_labels_bronze_fda_labels_raw()
