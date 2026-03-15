# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_labels


import pytest
import requests
import responses
from pydantic import HttpUrl

from coreason_etl_fda_labels.config import IngestionConfigManifest
from coreason_etl_fda_labels.discovery import EpistemicDiscoveryTask, PartitionLocatorManifest


@pytest.fixture
def config_manifest() -> IngestionConfigManifest:
    """Provides a default IngestionConfigManifest."""
    return IngestionConfigManifest()


def test_partition_locator_manifest_sorts_urls() -> None:
    """Test that PartitionLocatorManifest deterministically sorts URLs."""
    urls = [
        HttpUrl("https://example.com/b.zip"),
        HttpUrl("https://example.com/a.zip"),
        HttpUrl("https://example.com/c.zip"),
    ]
    manifest = PartitionLocatorManifest(partition_urls=urls)

    assert [str(url) for url in manifest.partition_urls] == [
        "https://example.com/a.zip",
        "https://example.com/b.zip",
        "https://example.com/c.zip",
    ]


@responses.activate
def test_epistemic_discovery_task_success(config_manifest: IngestionConfigManifest) -> None:
    """Test successful discovery of FDA partition URLs."""
    mock_payload = {
        "meta": {"last_updated": "2023-10-01"},
        "results": {
            "drug": {
                "label": {
                    "partitions": [
                        {"size_mb": "100", "records": 5000, "file": "https://api.fda.gov/download/part1.zip"},
                        {"size_mb": "120", "records": 5500, "file": "https://api.fda.gov/download/part2.zip"},
                    ]
                }
            }
        },
    }

    responses.add(
        responses.GET,
        str(config_manifest.discovery_endpoint),
        json=mock_payload,
        status=200,
    )

    task = EpistemicDiscoveryTask(config=config_manifest)
    manifest = task.execute()

    assert len(manifest.partition_urls) == 2
    assert str(manifest.partition_urls[0]) == "https://api.fda.gov/download/part1.zip"
    assert str(manifest.partition_urls[1]) == "https://api.fda.gov/download/part2.zip"


@responses.activate
def test_epistemic_discovery_task_http_error(config_manifest: IngestionConfigManifest) -> None:
    """Test discovery task handling HTTP 404."""
    responses.add(
        responses.GET,
        str(config_manifest.discovery_endpoint),
        status=404,
    )

    task = EpistemicDiscoveryTask(config=config_manifest)

    with pytest.raises(requests.exceptions.HTTPError):
        task.execute()


@responses.activate
def test_epistemic_discovery_task_malformed_json(config_manifest: IngestionConfigManifest) -> None:
    """Test discovery task handling missing keys in payload."""
    mock_payload = {
        "meta": {"last_updated": "2023-10-01"},
        "results": {"drug": {}},
    }

    responses.add(
        responses.GET,
        str(config_manifest.discovery_endpoint),
        json=mock_payload,
        status=200,
    )

    task = EpistemicDiscoveryTask(config=config_manifest)

    with pytest.raises(KeyError, match="Missing expected key in FDA payload topology: 'label'"):
        task.execute()


@responses.activate
def test_epistemic_discovery_task_missing_file_key(config_manifest: IngestionConfigManifest) -> None:
    """Test discovery task gracefully skips partitions missing the 'file' key."""
    mock_payload = {
        "meta": {"last_updated": "2023-10-01"},
        "results": {
            "drug": {
                "label": {
                    "partitions": [
                        {
                            "size_mb": "100",
                            "records": 5000,
                        },
                        {"size_mb": "120", "records": 5500, "file": "https://api.fda.gov/download/part2.zip"},
                    ]
                }
            }
        },
    }

    responses.add(
        responses.GET,
        str(config_manifest.discovery_endpoint),
        json=mock_payload,
        status=200,
    )

    task = EpistemicDiscoveryTask(config=config_manifest)
    manifest = task.execute()

    assert len(manifest.partition_urls) == 1
    assert str(manifest.partition_urls[0]) == "https://api.fda.gov/download/part2.zip"
