import json
import logging
import os
from logging.config import dictConfig
from pathlib import Path
from typing import Any, cast

DEFAULT_LOGGING_CONFIG_PATH = Path("config/logging.json")


def configure_logging(config_path: Path | None = None) -> None:
    path = config_path or Path(os.getenv("LOGGING_CONFIG_PATH", DEFAULT_LOGGING_CONFIG_PATH))
    with path.open(encoding="utf-8") as config_file:
        config = cast(dict[str, Any], json.load(config_file))

    dictConfig(config)
    logging.captureWarnings(capture=True)
