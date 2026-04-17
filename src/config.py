"""Configuration management for Bundeshaushalt Q&A."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


class Config:
    """Application configuration loaded from environment variables."""

    # Azure OpenAI
    AZURE_OPENAI_ENDPOINT: str = os.getenv("AZURE_OPENAI_ENDPOINT", "")
    AZURE_OPENAI_API_KEY: str = os.getenv("AZURE_OPENAI_API_KEY", "")
    AZURE_OPENAI_API_VERSION: str = os.getenv(
        "AZURE_OPENAI_API_VERSION", "2024-12-01-preview"
    )
    AZURE_OPENAI_DEPLOYMENT: str = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")

    # Paths (relative to project root)
    PROJECT_ROOT: Path = _PROJECT_ROOT
    DOCS_DIR: Path = _PROJECT_ROOT / os.getenv("DOCS_DIR", "docs")
    DATA_DIR: Path = _PROJECT_ROOT / os.getenv("DATA_DIR", "data")
    DB_PATH: Path = _PROJECT_ROOT / os.getenv("DATA_DIR", "data") / "bundeshaushalt.db"

    @classmethod
    def validate(cls) -> list[str]:
        """Return list of missing required config values."""
        errors = []
        if not cls.AZURE_OPENAI_ENDPOINT:
            errors.append("AZURE_OPENAI_ENDPOINT is not set")
        if not cls.AZURE_OPENAI_API_KEY:
            errors.append("AZURE_OPENAI_API_KEY is not set")
        if not cls.DOCS_DIR.exists():
            errors.append(f"DOCS_DIR does not exist: {cls.DOCS_DIR}")
        return errors


config = Config()
