# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_labels

import io
import json
import uuid
import zipfile
from typing import Any

import pytest
import requests
import responses
from pydantic import HttpUrl

from coreason_etl_fda_labels.extraction import NAMESPACE_FDALABEL, EpistemicExtractionTask


def create_mock_zip(json_content: list[dict[str, Any]], filename: str = "data.json") -> bytes:
    """Helper to generate an in-memory ZIP archive containing the specified JSON items."""
    in_memory_zip = io.BytesIO()
    with zipfile.ZipFile(in_memory_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Wrap the list in a struct matching openFDA's results.item layout
        fda_mock = {"results": json_content}
        zf.writestr(filename, json.dumps(fda_mock))
    in_memory_zip.seek(0)
    return in_memory_zip.read()


@responses.activate
def test_epistemic_extraction_task_success() -> None:
    """Test successful streaming, JSON extraction, and deterministic ID generation."""
    test_url_str = "https://api.fda.gov/mock_partition_1.zip"
    test_url = HttpUrl(test_url_str)

    # 1. Prepare Mock Data (Multiple set_ids to test batching)
    mock_data: list[dict[str, Any]] = [
        {"set_id": "abc-123", "brand_name": ["Drug A"]},
        {"set_id": "def-456", "brand_name": ["Drug B"]},
        # Edge case: Missing set_id
        {"brand_name": ["Drug C"]},
    ]

    mock_zip_bytes = create_mock_zip(mock_data)

    # 2. Mock HTTP endpoint
    responses.add(
        responses.GET,
        test_url_str,
        body=mock_zip_bytes,
        status=200,
        content_type="application/zip",
        stream=True,
    )

    # 3. Execution
    task = EpistemicExtractionTask()
    batches = list(task.execute(test_url))

    # 4. Assertions
    assert len(batches) == 1
    batch = batches[0]
    assert len(batch) == 3

    # Assert deterministic ID generation
    expected_id_1 = str(uuid.uuid5(NAMESPACE_FDALABEL, "abc-123"))
    expected_id_2 = str(uuid.uuid5(NAMESPACE_FDALABEL, "def-456"))

    assert batch[0]["coreason_id"] == expected_id_1
    assert batch[0]["partition_url"] == test_url_str
    assert batch[0]["raw_data"] == mock_data[0]

    assert batch[1]["coreason_id"] == expected_id_2
    assert batch[1]["partition_url"] == test_url_str
    assert batch[1]["raw_data"] == mock_data[1]

    # Edge case: Missing set_id -> None
    assert batch[2]["coreason_id"] is None
    assert batch[2]["partition_url"] == test_url_str

    # Cast raw_data for typed dictionary access in tests
    raw_data_2 = batch[2]["raw_data"]
    assert isinstance(raw_data_2, dict)
    assert raw_data_2["brand_name"] == mock_data[2]["brand_name"]


@responses.activate
def test_epistemic_extraction_task_http_error() -> None:
    """Test resilience against upstream HTTP failures."""
    test_url_str = "https://api.fda.gov/mock_partition_fail.zip"
    test_url = HttpUrl(test_url_str)

    responses.add(
        responses.GET,
        test_url_str,
        status=500,
    )

    task = EpistemicExtractionTask()
    with pytest.raises(requests.exceptions.RequestException):
        list(task.execute(test_url))


@responses.activate
def test_epistemic_extraction_task_no_json_files() -> None:
    """Test resilience against corrupt or missing JSON file structure inside the archive."""
    test_url_str = "https://api.fda.gov/mock_partition_empty.zip"
    test_url = HttpUrl(test_url_str)

    in_memory_zip = io.BytesIO()
    with zipfile.ZipFile(in_memory_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("readme.txt", "No JSON here.")
    in_memory_zip.seek(0)

    responses.add(
        responses.GET,
        test_url_str,
        body=in_memory_zip.read(),
        status=200,
        content_type="application/zip",
        stream=True,
    )

    task = EpistemicExtractionTask()
    with pytest.raises(ValueError, match="No JSON files found in the dataset archive"):
        list(task.execute(test_url))


@responses.activate
def test_epistemic_extraction_task_corrupt_zip() -> None:
    """Test handling of structurally invalid binary zip archives."""
    test_url_str = "https://api.fda.gov/mock_partition_corrupt.zip"
    test_url = HttpUrl(test_url_str)

    responses.add(
        responses.GET,
        test_url_str,
        body=b"NOT A ZIP FILE BLAH BLAH BLAH",
        status=200,
        content_type="application/zip",
        stream=True,
    )

    task = EpistemicExtractionTask()
    with pytest.raises(zipfile.BadZipFile):
        list(task.execute(test_url))


@responses.activate
def test_epistemic_extraction_task_batching() -> None:
    """Test memory-safe batching when number of items exceeds chunk size."""
    test_url_str = "https://api.fda.gov/mock_partition_large.zip"
    test_url = HttpUrl(test_url_str)

    # Create 2500 items, expect 3 batches (1000, 1000, 500)
    mock_data = [{"set_id": f"item-{i}"} for i in range(2500)]
    mock_zip_bytes = create_mock_zip(mock_data)

    responses.add(
        responses.GET,
        test_url_str,
        body=mock_zip_bytes,
        status=200,
        content_type="application/zip",
        stream=True,
    )

    task = EpistemicExtractionTask()
    batches = list(task.execute(test_url))

    assert len(batches) == 3
    assert len(batches[0]) == 1000
    assert len(batches[1]) == 1000
    assert len(batches[2]) == 500

    # Verify ID on the last item
    expected_id = str(uuid.uuid5(NAMESPACE_FDALABEL, "item-2499"))
    assert batches[2][499]["coreason_id"] == expected_id
