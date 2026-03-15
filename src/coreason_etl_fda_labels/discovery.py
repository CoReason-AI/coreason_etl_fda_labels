# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_labels

import requests
from pydantic import BaseModel, Field, HttpUrl

from coreason_etl_fda_labels.config import IngestionConfigManifest
from coreason_etl_fda_labels.utils.logger import logger


class PartitionLocatorManifest(BaseModel):
    """
    Epistemic boundary defining the resolved locations of openFDA data partitions.

    AGENT INSTRUCTION: This manifest strictly houses the final HTTPS coordinates
    required to commence the streaming data ingestion. The partition URLs
    are deterministically ordered to guarantee consistent hashing.
    """

    partition_urls: list[HttpUrl] = Field(
        default_factory=list,
        description=(
            "A deterministically sorted list of HTTPS URLs pointing to ZIP files containing openFDA SPL datasets."
        ),
    )

    def __init__(self, **data: object) -> None:
        super().__init__(**data)
        self.partition_urls = sorted(self.partition_urls, key=str)


class EpistemicDiscoveryTask:
    """
    Cognitive task responsible for traversing the openFDA index manifest to locate
    and acquire the necessary partition ZIP locations for downstream mutation.

    AGENT INSTRUCTION: This task MUST safely fetch and navigate the JSON structure,
    handling structural mutations gracefully without failing the entire system,
    and returning a validated PartitionLocatorManifest.
    """

    def __init__(self, config: IngestionConfigManifest) -> None:
        """Initialize the discovery task with its configuration manifest."""
        self.config = config

    def execute(self) -> PartitionLocatorManifest:
        """
        Executes the discovery traversal.

        Fetches the `download.json` manifest from the openFDA base endpoint.
        Extracts the list of ZIP partitions specifically from the `results.drug.label.partitions` hierarchy.

        Returns:
            PartitionLocatorManifest: The resolved list of partition URLs.

        Raises:
            requests.exceptions.RequestException: If the HTTP transit fails.
            KeyError: If the expected JSON topological structure is malformed.
        """
        logger.info(
            "Initiating EpistemicDiscoveryTask",
            endpoint=str(self.config.discovery_endpoint),
        )

        response = requests.get(str(self.config.discovery_endpoint), timeout=30)
        response.raise_for_status()

        data = response.json()

        try:
            partitions = data["results"]["drug"]["label"]["partitions"]
            urls = [HttpUrl(p["file"]) for p in partitions if "file" in p]
            return PartitionLocatorManifest(partition_urls=urls)
        except KeyError as e:
            logger.exception("EpistemicDiscoveryTask encountered a structural defect in the FDA manifest topology.")
            raise KeyError(f"Missing expected key in FDA payload topology: {e}") from e
