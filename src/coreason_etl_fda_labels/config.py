# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_labels

from pydantic import BaseModel, Field, HttpUrl


class IngestionConfigManifest(BaseModel):
    """
    Epistemic boundary defining the configuration for FDA SPL data ingestion.

    AGENT INSTRUCTION: This manifest strictly handles the discovery configuration
    and sets the base HTTPS coordinates to resolve openFDA drug label partitions.
    """

    discovery_endpoint: HttpUrl = Field(
        default=HttpUrl("https://api.fda.gov/download.json"),
        description="The root URL endpoint for discovering the openFDA bulk manifest.",
    )
