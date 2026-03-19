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
from hypothesis import given
from hypothesis import strategies as st
from pydantic import HttpUrl, ValidationError

from coreason_etl_fda_labels.config import IngestionConfigManifest


def test_ingestion_config_manifest_default() -> None:
    """Validate that the IngestionConfigManifest has the correct default discovery endpoint."""
    config = IngestionConfigManifest()
    assert str(config.discovery_endpoint) == "https://api.fda.gov/download.json"


def test_ingestion_config_manifest_custom() -> None:
    """Validate that the IngestionConfigManifest accepts valid custom HttpUrls."""
    custom_url = "https://example.com/api/v1/download.json"
    config = IngestionConfigManifest(discovery_endpoint=HttpUrl(custom_url))
    assert str(config.discovery_endpoint) == custom_url


def test_ingestion_config_manifest_invalid_url() -> None:
    """Validate that the IngestionConfigManifest rejects invalid URLs."""
    with pytest.raises(ValidationError):
        IngestionConfigManifest(discovery_endpoint="not-a-url")


@given(url_str=st.from_regex(r"^https://[a-z0-9-]+\.[a-z]{2,}/[a-zA-Z0-9/_-]*$", fullmatch=True))
def test_ingestion_config_manifest_hypothesis_valid_url(url_str: str) -> None:
    """Validate robust URL parsing using hypothesis generated strings.
    Restricts domain to lowercase to avoid Pydantic's automatic lowercasing from failing the exact match.
    """
    config = IngestionConfigManifest(discovery_endpoint=HttpUrl(url_str))
    assert str(config.discovery_endpoint) == url_str


@given(invalid_url_str=st.text().filter(lambda x: not x.strip().lower().startswith("http")))
def test_ingestion_config_manifest_hypothesis_invalid_url(invalid_url_str: str) -> None:
    """Validate robust rejection of invalid URLs using hypothesis generated strings."""
    with pytest.raises(ValidationError):
        IngestionConfigManifest(discovery_endpoint=invalid_url_str)  # type: ignore[arg-type, unused-ignore]


@given(
    invalid_scheme_url=st.builds(
        lambda scheme, domain, path: f"{scheme}://{domain}/{path}",
        scheme=st.sampled_from(["ftp", "file", "ws", "wss", "tcp", "udp", "gopher", "mailto", "data"]),
        domain=st.from_regex(r"^[a-z0-9-]+\.[a-z]{2,}$", fullmatch=True),
        path=st.from_regex(r"^[a-zA-Z0-9/_-]*$", fullmatch=True),
    )
)
def test_ingestion_config_manifest_hypothesis_invalid_scheme(invalid_scheme_url: str) -> None:
    """Validate that IngestionConfigManifest rejects URLs with non-HTTP/HTTPS schemes."""
    with pytest.raises(ValidationError):
        IngestionConfigManifest(discovery_endpoint=invalid_scheme_url)  # type: ignore[arg-type, unused-ignore]


@given(
    invalid_host_url=st.builds(
        lambda scheme, invalid_chars, path: f"{scheme}://domain{invalid_chars}name/{path}",
        scheme=st.sampled_from(["http", "https"]),
        invalid_chars=st.sampled_from([" ", "<", ">", "^", "|"]),
        path=st.from_regex(r"^[a-zA-Z0-9/_-]+$", fullmatch=True),
    )
)
def test_ingestion_config_manifest_hypothesis_invalid_host(invalid_host_url: str) -> None:
    """Validate that IngestionConfigManifest rejects HTTP URLs with invalid host characters."""
    with pytest.raises(ValidationError):
        IngestionConfigManifest(discovery_endpoint=invalid_host_url)  # type: ignore[arg-type, unused-ignore]


@given(
    partition_urls=st.lists(
        st.from_regex(r"^https://[a-z0-9-]+\.[a-z]{2,}/[a-zA-Z0-9/_-]*\.zip$", fullmatch=True),
        min_size=1,
        max_size=20,
        unique=True,
    )
)
def test_partition_locator_manifest_hypothesis_sorting(partition_urls: list[str]) -> None:
    """Validate that PartitionLocatorManifest deterministically sorts valid partition URLs.
    This fulfills the requirement of data determinism for consistent hashing.
    """
    from pydantic import HttpUrl

    from coreason_etl_fda_labels.discovery import PartitionLocatorManifest

    urls = [HttpUrl(url) for url in partition_urls]
    manifest = PartitionLocatorManifest(partition_urls=urls)

    expected_sorted = sorted(partition_urls)
    actual_sorted = [str(url) for url in manifest.partition_urls]
    assert actual_sorted == expected_sorted
