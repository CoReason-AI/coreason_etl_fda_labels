# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_labels

import os
import tempfile
import uuid
import zipfile
from collections.abc import Iterator
from typing import Any

import ijson
import polars as pl
import requests
from pydantic import HttpUrl

from coreason_etl_fda_labels.utils.logger import logger

# Epistemic Constant: The strict UUID namespace for generating deterministic FDA Label identifiers.
NAMESPACE_FDALABEL = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


class EpistemicExtractionTask:
    """
    Cognitive task responsible for securely streaming compressed openFDA datasets
    into temporary storage, bypassing catastrophic memory spikes, and iteratively
    extracting semantic JSON objects into highly optimized Polars manifolds.

    AGENT INSTRUCTION: This class MUST enforce the clean-room pattern by reading
    compressed JSON directly from the ZIP stream and strictly using ijson to parse
    the hierarchy without loading the entire document into RAM.
    """

    def process_polars_batch(self, batch: list[dict[str, Any]], partition_metadata: str) -> list[dict[str, Any]]:
        """
        Transmutes raw JSON structures into a vectorized Polars manifold, generating
        deterministic UUIDv5 identifiers from the source FDA `set_id` before yielding.

        Args:
            batch: A raw cognitive slice of FDA items from the uncompressed JSON stream.
            partition_metadata: The origin coordinate string for tracing data lineage.

        Returns:
            A list of dictionary schemas representing the processed Bronze-layer data.
        """
        df = pl.DataFrame({"raw_data": batch})

        # Extract the set_id from the nested JSON object
        df = df.with_columns(pl.col("raw_data").struct.field("set_id").alias("set_id"))

        # Shift-Left UUID5 Generation via map_batches
        df = df.with_columns(
            pl.col("set_id")
            .map_batches(
                lambda s: pl.Series([str(uuid.uuid5(NAMESPACE_FDALABEL, str(x))) if x else None for x in s]),
                return_dtype=pl.String,
            )
            .alias("coreason_id"),
            pl.lit(partition_metadata).alias("partition_url"),
        )

        # Cast explicitly to satisfy typing since Polars dynamically creates dictionary types
        result: list[dict[str, Any]] = df.to_dicts()
        return result

    def execute(self, url: HttpUrl) -> Iterator[list[dict[str, Any]]]:
        """
        Initiates the secure streaming, uncompression, and yield sequence.

        Args:
            url: The HTTPS target pointing to a ZIP-compressed JSON dataset.

        Yields:
            Batches of standardized dictionaries representing the raw data and generated
            `coreason_id` for downstream ingestion pipelines.

        Raises:
            requests.exceptions.RequestException: If the external data pipeline fails.
            zipfile.BadZipFile: If the provided binary block is structurally corrupt.
            ValueError: If the compressed block contains no accessible JSON payloads.
        """
        partition_metadata = str(url)
        logger.info("Initiating EpistemicExtractionTask", target=partition_metadata)

        # OS Safety: Delete=False prevents Windows from locking the uncompressed file prematurely.
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
            tmp_name = tmp_zip.name

            try:
                with requests.get(partition_metadata, stream=True, timeout=120) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=8192):
                        tmp_zip.write(chunk)
            finally:
                tmp_zip.close()  # Release OS lock

        try:
            with zipfile.ZipFile(tmp_name, "r") as z:
                # Resolve the single internal JSON file (usually openFDA zips only have one)
                json_filenames = [name for name in z.namelist() if name.endswith(".json")]
                if not json_filenames:
                    raise ValueError(f"No JSON files found in the dataset archive from {partition_metadata}")

                json_filename = json_filenames[0]

                with z.open(json_filename) as f:
                    # Memory Safety: Iterative parsing of massive JSON blocks.
                    items = ijson.items(f, "results.item")
                    batch: list[dict[str, Any]] = []

                    for item in items:
                        batch.append(item)
                        # Process chunks dynamically to keep memory usage flat
                        if len(batch) >= 1000:
                            yield self.process_polars_batch(batch, partition_metadata)
                            batch = []

                    if batch:
                        yield self.process_polars_batch(batch, partition_metadata)

        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)  # Ensure cross-platform cleanup of ephemeral states
