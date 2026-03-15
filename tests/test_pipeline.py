from datetime import UTC, datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import HttpUrl

from coreason_etl_fda_labels.config import IngestionConfigManifest
from coreason_etl_fda_labels.discovery import PartitionLocatorManifest
from coreason_etl_fda_labels.pipeline import fda_spl_source

# Dummy config instance to inject
DUMMY_CONFIG = IngestionConfigManifest(discovery_endpoint=HttpUrl("https://dummy.api/download.json"))
DUMMY_PARTITION_URLS = [
    HttpUrl("https://dummy.api/partitions/part1.zip"),
    HttpUrl("https://dummy.api/partitions/part2.zip"),
]


@pytest.fixture
def mock_discovery_task() -> Any:
    with patch("coreason_etl_fda_labels.pipeline.EpistemicDiscoveryTask") as mock:
        instance = mock.return_value
        instance.execute.return_value = PartitionLocatorManifest(partition_urls=DUMMY_PARTITION_URLS)
        yield mock


@pytest.fixture
def mock_extraction_task() -> Any:
    with patch("coreason_etl_fda_labels.pipeline.EpistemicExtractionTask") as mock:
        instance = mock.return_value

        # Simulate extraction yielding batches
        def mock_execute(url: HttpUrl) -> Any:
            yield [{"raw_data": {"id": 1, "set_id": "test_id"}, "coreason_id": "uuid-1", "partition_url": str(url)}]

        instance.execute.side_effect = mock_execute
        yield mock


def test_fda_spl_source_configuration() -> None:
    """Verify that the source enforces max_table_nesting=0 correctly."""
    source = fda_spl_source()
    assert source.max_table_nesting == 0
    # source.name is fda_spl_source, resource name is bronze_fda_labels_raw
    assert source.name == "fda_spl_source"
    assert "bronze_fda_labels_raw" in source.resources


@patch("coreason_etl_fda_labels.pipeline.datetime")
def test_fda_spl_source_yields_correct_records(
    mock_datetime: Any, mock_discovery_task: Any, mock_extraction_task: Any
) -> None:
    """Verify that records are yielded correctly with ingestion_ts appended."""
    fixed_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    mock_datetime.now.return_value = fixed_time
    mock_datetime.timezone = timezone

    source = fda_spl_source(config=DUMMY_CONFIG)
    resource = source.resources["bronze_fda_labels_raw"]

    # Iterate through the resource generator (since dlt unrolls yield to the resource itself)
    # resource yields elements of the iterator directly, which are dictionaries, not lists of dicts
    all_records = list(resource)

    # We expect 2 records, one for each partition
    assert len(all_records) == 2

    # Check the contents and appended `ingestion_ts`
    for i, record in enumerate(all_records):
        assert record["ingestion_ts"] == "2025-01-01T12:00:00+00:00"
        assert record["partition_url"] == str(DUMMY_PARTITION_URLS[i])
        assert "raw_data" in record
        assert "coreason_id" in record

    # Verify that tasks were instantiated with correct arguments
    mock_discovery_task.assert_called_once_with(config=DUMMY_CONFIG)

    # Check that execution iterations were called properly for both partitions
    extraction_instance = mock_extraction_task.return_value
    assert extraction_instance.execute.call_count == 2
