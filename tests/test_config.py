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
