# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_labels

from typing import Any

import dlt
from dlt.pipeline.pipeline import Pipeline

from coreason_etl_fda_labels.config import IngestionConfigManifest
from coreason_etl_fda_labels.pipeline import fda_spl_source
from coreason_etl_fda_labels.utils.logger import logger


class EpistemicPipelineExecutionIntent:
    """
    Cognitive trigger responsible for materializing the Bronze ingestion pipeline.

    AGENT INSTRUCTION: This intent MUST strictly enforce the clean-room pattern
    by isolating the DLT execution sequence. It establishes the topological boundary
    for the pipeline, aiming to sink the JSON partitions into the Postgres storage manifold.
    """

    def __init__(self, config: IngestionConfigManifest | None = None) -> None:
        """Initialize the execution intent with an optional configuration manifest."""
        self.config = config or IngestionConfigManifest()

    def execute(self) -> dict[str, Any]:
        """
        Executes the data transmutation sequence.

        Initializes the DLT pipeline connected to a Postgres destination,
        mounts the `fda_spl_source`, and executes the extraction and load processes.

        Returns:
            A dictionary containing the pipeline load statistics and metadata.
        """
        logger.info("Triggering EpistemicPipelineExecutionIntent for openFDA dataset.")

        # Initialize the stateful DLT pipeline sequence
        pipeline: Pipeline = dlt.pipeline(
            pipeline_name="coreason_etl_fda_labels",
            destination="postgres",
            dataset_name="bronze",
        )

        # Mount the primary epistemic source using the provided manifest
        source = fda_spl_source(config=self.config)

        # Execute the transformation and sink the data
        load_info = pipeline.run(source)
        logger.info("EpistemicPipelineExecutionIntent materialized successfully.")

        # Cast to dict for standardized cognitive snapshot
        result: dict[str, Any] = load_info.asdict()
        return result

if __name__ == "__main__":
    intent = EpistemicPipelineExecutionIntent()
    intent.execute()
