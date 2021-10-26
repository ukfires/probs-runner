"""Common configuration shared by all tests in this directory."""

import os
import pytest
from pathlib import Path


@pytest.fixture
def script_source_dir():
    if "PROBS_SCRIPT_SOURCE_DIR" in os.environ:
        script_source_dir = Path(os.environ["PROBS_SCRIPT_SOURCE_DIR"])
        if not script_source_dir.exists():
            raise FileNotFoundError(f"SCRIPT_SOURCE_DIR not found: '{script_source_dir}'")
    else:
        script_source_dir = None
    return script_source_dir
