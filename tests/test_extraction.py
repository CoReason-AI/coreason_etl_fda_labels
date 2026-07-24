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
from unittest.mock import patch

import pyarrow as pa
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


class MockNamedTemporaryFile:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        del args
        del kwargs
        self.name = "/fake/temp/file.zip"
        self._io = io.BytesIO()

    def __enter__(self) -> "MockNamedTemporaryFile":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    def write(self, data: bytes) -> None:
        self._io.write(data)

    def close(self) -> None:
        pass


original_zipfile = zipfile.ZipFile


@pytest.fixture
def mock_tempfile() -> Any:
    """Fixture to mock temporary file and disk I/O for extraction tasks."""
    mock_file = MockNamedTemporaryFile()

    def mock_zipfile_init(
        file: Any,
        mode: str = "r",
        compression: int = zipfile.ZIP_STORED,
        allowZip64: bool = True,  # noqa: N803
        compresslevel: int | None = None,
        *,
        strict_timestamps: bool = True,
        metadata_encoding: str | None = None,
    ) -> Any:
        if file == mock_file.name:
            file = mock_file._io
        # use cast or ignore to satisfy mypy overload resolution
        return original_zipfile(  # type: ignore[call-overload]
            file,
            mode=mode,
            compression=compression,
            allowZip64=allowZip64,
            compresslevel=compresslevel,
            strict_timestamps=strict_timestamps,
            metadata_encoding=metadata_encoding,
        )

    with (
        patch("tempfile.NamedTemporaryFile", return_value=mock_file),
        patch("zipfile.ZipFile", side_effect=mock_zipfile_init),
        patch("os.unlink"),
        patch("os.path.exists", return_value=True),
    ):
        # We need to rewind the mock file before ZipFile reads it
        original_enter = mock_file.__enter__

        def custom_enter(*args: Any, **kwargs: Any) -> MockNamedTemporaryFile:
            return original_enter(*args, **kwargs)

        def reset_io() -> None:
            mock_file._io.seek(0)

        # Hook into close to simulate file readiness for zipfile reading
        original_close = mock_file.close

        def custom_close() -> None:
            original_close()
            reset_io()

        mock_file.close = custom_close  # type: ignore

        yield mock_file


@responses.activate
def test_epistemic_extraction_task_success(mock_tempfile: Any) -> None:
    del mock_tempfile
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
    batch_table = batches[0]
    assert isinstance(batch_table, pa.Table)
    assert batch_table.num_rows == 3

    # Assert deterministic ID generation
    expected_id_1 = str(uuid.uuid5(NAMESPACE_FDALABEL, "abc-123"))
    expected_id_2 = str(uuid.uuid5(NAMESPACE_FDALABEL, "def-456"))

    # Convert table to dicts for easy assertions
    batch_dicts = batch_table.to_pylist()

    assert batch_dicts[0]["coreason_id"] == expected_id_1
    assert batch_dicts[0]["partition_url"] == test_url_str
    assert batch_dicts[0]["raw_data"] == mock_data[0]

    assert batch_dicts[1]["coreason_id"] == expected_id_2
    assert batch_dicts[1]["partition_url"] == test_url_str
    assert batch_dicts[1]["raw_data"] == mock_data[1]

    # Edge case: Missing set_id -> None
    assert batch_dicts[2]["coreason_id"] is None
    assert batch_dicts[2]["partition_url"] == test_url_str

    raw_data_2 = batch_dicts[2]["raw_data"]
    assert isinstance(raw_data_2, dict)
    assert raw_data_2["brand_name"] == mock_data[2]["brand_name"]


@responses.activate
def test_epistemic_extraction_task_http_error(mock_tempfile: Any) -> None:
    del mock_tempfile
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
def test_epistemic_extraction_task_no_json_files(mock_tempfile: Any) -> None:
    del mock_tempfile
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
def test_epistemic_extraction_task_corrupt_zip(mock_tempfile: Any) -> None:
    del mock_tempfile
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
def test_epistemic_extraction_task_batching(mock_tempfile: Any) -> None:
    del mock_tempfile
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
    assert batches[0].num_rows == 1000
    assert batches[1].num_rows == 1000
    assert batches[2].num_rows == 500

    # Verify ID on the last item
    expected_id = str(uuid.uuid5(NAMESPACE_FDALABEL, "item-2499"))
    batch_3_dicts = batches[2].to_pylist()
    assert batch_3_dicts[499]["coreason_id"] == expected_id
