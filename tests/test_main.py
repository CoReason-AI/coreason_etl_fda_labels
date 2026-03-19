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
from unittest.mock import MagicMock, patch

from coreason_etl_fda_labels.config import IngestionConfigManifest
from coreason_etl_fda_labels.main import EpistemicPipelineExecutionIntent


@patch("coreason_etl_fda_labels.main.dlt.pipeline")
@patch("coreason_etl_fda_labels.main.fda_spl_source")
def test_epistemic_pipeline_execution_intent_success(
    mock_source: MagicMock,
    mock_pipeline: MagicMock,
) -> None:
    """Test the successful execution of the DLT pipeline intent without live DB connections."""

    # 1. Setup mocks
    mock_pipeline_instance = MagicMock()
    mock_load_info = MagicMock()
    mock_load_info.asdict.return_value = {"status": "success", "rows_inserted": 100}
    mock_pipeline_instance.run.return_value = mock_load_info
    mock_pipeline.return_value = mock_pipeline_instance

    mock_source_instance = MagicMock()
    mock_source.return_value = mock_source_instance

    config = IngestionConfigManifest()
    intent = EpistemicPipelineExecutionIntent(config=config)

    # 2. Execute
    result: dict[str, Any] = intent.execute()

    # 3. Assertions
    mock_pipeline.assert_called_once_with(
        pipeline_name="coreason_etl_fda_labels",
        destination="postgres",
        dataset_name="bronze",
    )
    mock_source.assert_called_once_with(config=config)
    mock_pipeline_instance.run.assert_called_once_with(mock_source_instance)

    assert result == {"status": "success", "rows_inserted": 100}


@patch("coreason_etl_fda_labels.main.dlt.pipeline")
@patch("coreason_etl_fda_labels.main.fda_spl_source")
def test_epistemic_pipeline_execution_intent_default_config(
    mock_source: MagicMock,
    mock_pipeline: MagicMock,
) -> None:
    """Test the intent initialization with default configuration."""
    # 1. Setup mocks
    mock_pipeline_instance = MagicMock()
    mock_load_info = MagicMock()
    mock_load_info.asdict.return_value = {"status": "success"}
    mock_pipeline_instance.run.return_value = mock_load_info
    mock_pipeline.return_value = mock_pipeline_instance

    intent = EpistemicPipelineExecutionIntent()

    # 2. Execute
    intent.execute()

    # 3. Assertions
    assert isinstance(intent.config, IngestionConfigManifest)
    mock_source.assert_called_once_with(config=intent.config)
